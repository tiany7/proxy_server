use std::convert::{TryFrom, TryInto};
use std::vec::Vec;

use tokio::sync::mpsc;
use anyhow::{Result,Error};


// 定义一个动态数据类型
type DynData = Box<dyn Any + Send>;

// 定义一个数据结构，包装了动态数据
pub struct ChannelData(DynData);

struct SampleData {
    name: String,
    age: i32,
}

// 实现 ChannelData 的方法
impl ChannelData {
    
    pub fn new<T: 'static>(data: T) -> Self {
        ChannelData(Box::new(data))
    }
}

impl Into<SampleData> for ChannelData {
    fn into(self) -> SampleData {
        *self.0.downcast::<SampleData>().unwrap()
    }
}

impl TryFrom<ChannelData> for SampleData {
    type Error = Error;

    fn try_from(value: ChannelData) -> Result<Self> {
        value.0.downcast::<SampleData>().map_err(|_| Error::msg("downcast error"))
    }
}



pub fn create_channel(capacity: i32) -> (mpsc::Sender<ChannelData>, mpsc::Receiver<ChannelData>) {
    let (sender, receiver) = mpsc::channel(capacity);
    (sender_channel, receiver_channel)
}


pub trait BaseTransformer {
    // this is blocking
    async fn transform(&self, input: Vec<mpsc::Receiver<ChannelData>>, output: Vec<mpsc::Sender<ChannelData>>) -> Result<ChannelData>;
}


pub struct SampleTransformer {
    
}

impl BaseTransformer for SampleTransformer {
    async fn transform(&self, input: Vec<mpsc::Receiver<ChannelData>>, output: Vec<mpsc::Sender<ChannelData>>) -> Result<ChannelData> {
        let data = input[0].recv().await.unwrap();
        let data: SampleData = data.try_into()?;
        
        Ok(ChannelData::new(data))
    }
}

// we will synethize the bar after this

pub struct AggTradeDataTransformer {

}

impl BaseTransformer for AggTradeDataTransformer {
    async fn transform(&self, input: Vec<mpsc::Receiver<ChannelData>>, output: Vec<mpsc::Sender<ChannelData>>) -> Result<ChannelData> {
        // might do something here
        let data = input[0].recv().await.unwrap();
        let data: AggTradeData = data.try_into()?;
        
        Ok(ChannelData::new(data))
    }
}