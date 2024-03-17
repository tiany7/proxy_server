use std::convert::{TryFrom, TryInto};
use std::vec::Vec;
use std::any::Any;

use tokio::sync::mpsc;
use anyhow::{Result,Error};
use trade::AggTradeData;

// 定义一个动态数据类型
type DynData = Box<dyn Any + Send>;

pub mod trade {
    tonic::include_proto!("trade");
}

// 定义一个数据结构，包装了动态数据
pub struct ChannelData(DynData);


struct SampleData {
    name: String,
    age: i32,
}

// 实现 ChannelData 的方法
impl ChannelData {
    
    pub fn new<T: 'static + std::marker::Send>(data: T) -> Self {
        ChannelData(Box::new(data))
    }
}

impl Into<SampleData> for ChannelData {
    fn into(self) -> SampleData {
        *self.0.downcast::<SampleData>().unwrap()
    }
}


impl Into<AggTradeData> for ChannelData {
    fn into(self) -> AggTradeData {
        *self.0.downcast::<AggTradeData>().unwrap()
    }
}




pub fn create_channel(capacity: usize) -> (mpsc::Sender<ChannelData>, mpsc::Receiver<ChannelData>) {
    let (tx, rx) = mpsc::channel(capacity);
    (tx, rx)
}


pub trait BaseTransformer {
    // this is blocking
    async fn transform(&self, input: Vec<mpsc::Receiver<ChannelData>>, output: Vec<mpsc::Sender<ChannelData>>) -> Result<()>;
}


pub struct SampleTransformer {
    
}

impl BaseTransformer for SampleTransformer {
    async fn transform(&self, mut input: Vec<mpsc::Receiver<ChannelData>>, mut output: Vec<mpsc::Sender<ChannelData>>) -> Result<()> {
        while let Some(data) = input[0].recv().await {
            let data : SampleData = data.into();
            output[0].send(ChannelData::new(data)).await.unwrap();
        }
        Ok(())
    }
}

// we will synethize the bar after this

pub struct AggTradeDataTransformer {

}

impl BaseTransformer for AggTradeDataTransformer {
    async fn transform(&self, mut input: Vec<mpsc::Receiver<ChannelData>>, mut output: Vec<mpsc::Sender<ChannelData>>) -> Result<()> {
        // might do something here
        let data = input[0].recv().await.unwrap();
        let data: AggTradeData = data.try_into()?;
        output[0].send(ChannelData::new(data)).await.unwrap();
        Ok(())
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use tokio::{test, spawn};
    #[test]
    async fn test_channel() {
        let (tx, mut rx) = create_channel(10);
        for i in 1..=10 {
            tx.send(ChannelData::new(AggTradeData {
                symbol: "BTCUSDT".to_string(),
                price: 100.0,
                quantity: 10.0,
                trade_time: 1000,
                event_type: "trade".to_string(),
                is_buyer_maker: true,
                first_break_trade_id: 100,
                last_break_trade_id: 200,
                aggregated_trade_id: 300,
            })).await.unwrap();
        }

        for i in 1..=10 {
            let data = rx.recv().await;
            if data.is_none() {
                panic!("no data received");
            }
            let data = data.unwrap();
            let data: AggTradeData = data.try_into().unwrap();
            assert_eq!(data.symbol, "BTCUSDT");
            assert_eq!(data.price, 100.0);
            assert_eq!(data.quantity, 10.0);
            assert_eq!(data.trade_time, 1000);
            assert_eq!(data.event_type, "trade");
            assert_eq!(data.is_buyer_maker, true);
            assert_eq!(data.first_break_trade_id, 100);
            assert_eq!(data.last_break_trade_id, 200);
            assert_eq!(data.aggregated_trade_id, 300);
        }
    }

    #[test]
    async fn test_sample_pipeline() {
        let (tx, mut rx) = create_channel(10);
        let (tx2, mut rx2) = create_channel(10);

        let sample_transformer = SampleTransformer {};
        for _ in 1..=10 {
            tx.send(ChannelData::new(SampleData {
                name: "test".to_string(),
                age: 10,
            })).await.unwrap();
        }
        drop(tx);
        sample_transformer.transform(vec![rx], vec![tx2]).await.unwrap();
        for _ in 1..=10 {
            let data = rx2.recv().await;
            if data.is_none() {
                panic!("no data received");
            }
            let data = data.unwrap();
            let data: SampleData = data.try_into().unwrap();
            assert_eq!(data.name, "test");
            assert_eq!(data.age, 10);
        }


        
    }   
}
