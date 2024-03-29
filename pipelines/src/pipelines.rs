use core::num;
use std::convert::{TryFrom, TryInto};
use std::vec::Vec;
use std::any::Any;
use std::sync::Arc;
use std::io::Write;
use std::future::Future;

use futures::FutureExt;
use arrow::datatypes::Schema;
use tokio::sync::{mpsc, Mutex};
use anyhow::{Error, Ok, Result};
use async_trait::async_trait;
use arrow::record_batch::RecordBatch;
use arrow::datatypes::{DataType, Field};
use arrow::array::{ArrayRef, Float64Array, StringArray};

use crate::trade::column::Data;
use crate::trade::AggTradeData;
use crate::trade::Column;


// 定义一个动态数据类型
type DynData = Box<dyn Any + Send + Sync>;

// 定义一个数据结构，包装了动态数据
pub struct ChannelData(DynData);


pub mod trade {
    tonic::include_proto!("trade");
}

struct SampleData {
    name: String,
    age: i32,
}

// 实现 ChannelData 的方法
impl ChannelData {
    
    pub fn new<T: 'static + std::marker::Send + std::marker::Sync>(data: T) -> Self {
        ChannelData(Box::new(data))
    }
}

impl Into<SampleData> for ChannelData {
    fn into(self) -> SampleData {
        *self.0.downcast::<SampleData>().unwrap()
    }
}

impl Into<RecordBatch> for ChannelData {
    fn into(self) -> RecordBatch {
        *self.0.downcast::<RecordBatch>().unwrap()
    }
}

impl Into<AggTradeData> for ChannelData {
    fn into(self) -> AggTradeData {
        *self.0.downcast::<AggTradeData>().unwrap()
    }
}

impl Into<Column> for ChannelData {
    fn into(self) -> Column {
        *self.0.downcast::<Column>().unwrap()
    }
}

impl Into<Vec<u8>> for ChannelData {
    fn into(self) -> Vec<u8> {
        *self.0.downcast::<Vec<u8>>().unwrap()
    }
}




pub fn create_channel(capacity: usize) -> (mpsc::Sender<ChannelData>, mpsc::Receiver<ChannelData>) {
    let (tx, rx) = mpsc::channel(capacity);
    (tx, rx)
}

#[async_trait]
pub trait Transformer{
    // this is blocking
    async fn transform<'a>(&'a self) -> Result< ()>;
}


    


// this is used to synethize the bar data from the raw data
pub struct ResamplingTransformerInner {
    // in milliseconds, this is used to control the granularity of the data
    input: Vec<mpsc::Receiver<ChannelData>>,
    output: Vec<mpsc::Sender<ChannelData>>,
    granularity: chrono::Duration,
}

pub struct CompressionTransformerInner {
    // in milliseconds, this is used to control the granularity of the data
    input: Vec<mpsc::Receiver<ChannelData>>,
    output: Vec<mpsc::Sender<ChannelData>>,
}

pub struct ResamplingTransformer {
    inner : Arc<Mutex<ResamplingTransformerInner>>,
}

pub struct CompressionTransformer {
    inner : Arc<Mutex<CompressionTransformerInner>>,
}



impl ResamplingTransformer {
    #[allow(dead_code)]
    pub fn new(input: Vec<mpsc::Receiver<ChannelData>>, output: Vec<mpsc::Sender<ChannelData>>, granularity: chrono::Duration) -> Self {
        ResamplingTransformer {
            inner: Arc::new(Mutex::new(ResamplingTransformerInner {
                input,
                output,
                granularity,
            })),
        }
    }
}

impl CompressionTransformer {
    #[allow(dead_code)]
    pub fn new(input: Vec<mpsc::Receiver<ChannelData>>, output: Vec<mpsc::Sender<ChannelData>>) -> Self {
        CompressionTransformer {
            inner: Arc::new(Mutex::new(CompressionTransformerInner {
                input,
                output,
            })),
        }
    }
}




#[async_trait]
impl Transformer for Arc<ResamplingTransformer>{
    
    async fn transform(&self) -> Result<()> {
        // this is the schema of the record batch
        let schema = Schema::new(vec![
            Field::new("Open", DataType::Float64, false),
            Field::new("High", DataType::Boolean, false),
            Field::new("Low", DataType::Float64, false),
            Field::new("Close", DataType::Float64, false),
            Field::new("Volume", DataType::Float64, false),
            Field::new("Quote_asset_volume", DataType::Float64, false),
            Field::new("Number_of_trades", DataType::UInt32, false),
            Field::new("Taker_buy_base_asset_volume", DataType::Float64, false),
            Field::new("Taker_buy_quote_asset_volume", DataType::Float64, false),
            Field::new("Min_id", DataType::UInt64, false),
            Field::new("Max_id", DataType::UInt64, false),
            Field::new("Missing_count", DataType::UInt32, false), 
            Field::new("Open_time", DataType::UInt64, false), 
            Field::new("Close_time", DataType::UInt64, false), 
        ]);

        let mut low_price = f64::MAX;
        let mut high_price = f64::MIN;
        let mut open_price = 0.0;
        let mut close_price = 0.0;
        let mut volume = 0.0;
        let mut quote_asset_volume = 0.0;
        let mut number_of_trades = 0;
        let mut taker_buy_base_asset_volume = 0.0;
        let mut taker_buy_quote_asset_volume = 0.0;
        let mut min_id = 0;
        let mut max_id = 0;
        let mut missing_count = 0;
        let mut open_time = 0;
        let mut close_time = 0;

        let mut last: Option<u64> = None;
        let this = self.inner.lock().await;
        let this_time_gap = this.granularity.num_milliseconds() as u64;
        drop(this);

        loop {
            let mut ticket = self.inner.lock().await;
            let agg_trade = ticket.input[0].recv().await;
            drop(ticket);
            if agg_trade.is_none() {
                break;
            }
            let agg_trade: AggTradeData = agg_trade.unwrap().into();
            
            // count number of trades
            number_of_trades += 1;

            // update the min_id and max_id
            if last.is_some() {
                missing_count += agg_trade.aggregated_trade_id - last.unwrap() - 1; 
            }

            last = Some(agg_trade.aggregated_trade_id);

            assert!(missing_count >= 0);

            if agg_trade.price < low_price {
                low_price = agg_trade.price;
                min_id = agg_trade.aggregated_trade_id;
            }

            if agg_trade.price > high_price {
                high_price = agg_trade.price;
                max_id = agg_trade.aggregated_trade_id;
            }

            if number_of_trades == 1 {
                open_price = agg_trade.price;
                open_time = agg_trade.trade_time;
            }

            volume += agg_trade.quantity;
            quote_asset_volume += agg_trade.price * agg_trade.quantity;

            taker_buy_base_asset_volume += {
                if !agg_trade.is_buyer_maker {
                    agg_trade.quantity
                } else {
                    0.0
                }
            };

            taker_buy_quote_asset_volume += {
                if !agg_trade.is_buyer_maker {
                    agg_trade.price * agg_trade.quantity
                } else {
                    0.0
                }
            };

            // check granularity
            if agg_trade.trade_time - open_time >= this_time_gap {
                close_price = agg_trade.price;
                close_time = agg_trade.trade_time;
                let arrays: Vec<Arc<dyn arrow::array::Array>> = vec![
                    Arc::new(Float64Array::from(vec![open_price])),
                    Arc::new(Float64Array::from(vec![high_price])),
                    Arc::new(Float64Array::from(vec![low_price])),
                    Arc::new(Float64Array::from(vec![close_price])),
                    Arc::new(Float64Array::from(vec![volume])),
                    Arc::new(Float64Array::from(vec![quote_asset_volume])),
                    Arc::new(arrow::array::UInt32Array::from(vec![number_of_trades as u32])),
                    Arc::new(Float64Array::from(vec![taker_buy_base_asset_volume])),
                    Arc::new(Float64Array::from(vec![taker_buy_quote_asset_volume])),
                    Arc::new(arrow::array::UInt64Array::from(vec![min_id])),
                    Arc::new(arrow::array::UInt64Array::from(vec![max_id])),
                    Arc::new(arrow::array::UInt32Array::from(vec![missing_count as u32])),
                    Arc::new(arrow::array::UInt64Array::from(vec![open_time])),
                    Arc::new(arrow::array::UInt64Array::from(vec![close_time])),
                ];
                let record_batch = RecordBatch::try_new(Arc::new(schema.clone()), arrays).unwrap();
                let ticket = self.inner.lock().await;
                ticket.output[0].send(ChannelData::new(record_batch)).await?;
                drop(ticket);
                // reset the variables
                low_price = f64::MAX;
                high_price = f64::MIN;
                open_price = 0.0;
                close_price = 0.0;
                volume = 0.0;
                quote_asset_volume = 0.0;
                number_of_trades = 0;
                taker_buy_base_asset_volume = 0.0;
                taker_buy_quote_asset_volume = 0.0;
                min_id = 0;
                max_id = 0;
                missing_count = 0;
                open_time = 0;
                close_time = 0;
            }
        }


        Ok(())
    }
        
}

// TODO: implement this
#[async_trait]
impl Transformer for Arc<CompressionTransformer> {
    async fn transform(&self) -> Result<()> {
        // let record_batch: RecordBatch = self.input[0].recv().await.unwrap().into();
        // let mut buffer = Vec::new();
        // let mut writer = arrow::ipc::writer::StreamWriter::try_new(&mut buffer, &record_batch.schema()).ok().expect("writer error");
        // writer.write(&record_batch).ok().expect( "write error");
        // writer.finish().ok().expect("finish error");
        // // Compress the serialized data using lz4
        // let mut encoder = lz4::EncoderBuilder::new()
        //     .block_mode(lz4::BlockMode::Linked)
        //     .build(Vec::new())
        //     .unwrap();
        // let (first_part, second_part) = buffer.split_at(buffer.len());
        // encoder.write_all(first_part).unwrap();
        // encoder.write_all(second_part).unwrap();
        // let (result, _) = encoder.finish();
        // self.output[0].send(ChannelData::new(result)).await.unwrap();
        
        Ok(())
    }
}




// pipelines start here


// TODO: make dynamic dispatch work, this is not very safe
pub struct ResamplingPipeline {
    // transformers: Vec<Box<dyn Transformer>>,
}

impl ResamplingPipeline {
    pub fn new() -> Self {
        ResamplingPipeline {
        }
    }

    pub fn launch_transformer(&mut self, mut transformer: Arc<impl Transformer + 'static> ) -> Box<dyn Future<Output = Result<()>>> {
        let this = transformer.clone();
        let join_handle = this.transform();
    }
}
#[cfg(test)]
mod tests {
    use std::io::Split;

    use super::*;
    use tokio::{test, spawn};
    #[test]
    async fn test_channel() {
        let (tx, mut rx) = create_channel(10);
        for _ in 1..=10 {
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

        for _ in 1..=10 {
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

        let mut sample_transformer = SampleTransformer {
            input: vec![rx],
            output: vec![tx2],
        };
        for _ in 1..=10 {
            tx.send(ChannelData::new(SampleData {
                name: "test".to_string(),
                age: 10,
            })).await.unwrap();
        }
        drop(tx);
        sample_transformer.transform().await.unwrap();
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

    #[test]
    async fn test_split_column() {
        let (tx, mut rx) = create_channel(10);
        let (tx2, mut rx2) = create_channel(10);
        let (tx3, mut rx3) = create_channel(10);
        let (tx4, mut rx4) = create_channel(10);
        let (tx5, mut rx5) = create_channel(10);
        let (tx6, mut rx6) = create_channel(10);
        let (tx7, mut rx7) = create_channel(10);
        let (tx8, mut rx8) = create_channel(10);
        let (tx9, mut rx9) = create_channel(10);
        let (tx10, mut rx10) = create_channel(10);

        

        for _ in 1..=10 {
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
        drop(tx);
        let mut split_column_transformer = SplitColumnTransformer {
            input: vec![rx],
            output: vec![tx2, tx3, tx4, tx5, tx6, tx7, tx8, tx9, tx10],
        };
        split_column_transformer.transform().await.unwrap();
        for _ in 1..=10 {
            let symbol = rx2.recv().await.unwrap();
            let symbol: Column = symbol.into();
            assert_eq!(symbol.column_name, "symbol");
            if matches!(symbol.data, Some(Data::StringValue(_))) {
                assert_eq!(symbol.data.unwrap(), Data::StringValue("BTCUSDT".to_string()));
            } else {
                panic!("symbol data is not string");
            }

            let is_buyer_maker = rx3.recv().await.unwrap();
            let is_buyer_maker: Column = is_buyer_maker.into();
            assert_eq!(is_buyer_maker.column_name, "is_buyer_maker");
            if matches!(is_buyer_maker.data, Some(Data::BoolValue(_))) {
                assert_eq!(is_buyer_maker.data.unwrap(), Data::BoolValue(true));
            } else {
                panic!("is_buyer_maker data is not bool");
            }
        }
    }
}