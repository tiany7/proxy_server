use core::num;
use std::convert::{TryFrom, TryInto};
use std::vec::Vec;
use std::any::Any;
use std::sync::Arc;
use std::io::Write;
use std::future::Future;

use futures::FutureExt;
use arrow::datatypes::Schema;
use tokio::sync::mpsc;
use anyhow::{Result,Error};
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
    include!(concat!(env!("OUT_DIR"), "/trade.rs"));
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
pub trait Transformer {
    // this is blocking
    async fn transform(&mut self) -> Result<()>;
}


// transformers start here
pub struct SampleTransformer {
    input: Vec<mpsc::Receiver<ChannelData>>,
    output: Vec<mpsc::Sender<ChannelData>>,
}
pub struct AggTradeDataTransformer {
    input: Vec<mpsc::Receiver<ChannelData>>,
    output: Vec<mpsc::Sender<ChannelData>>,
}
pub struct SplitColumnTransformer {
    input: Vec<mpsc::Receiver<ChannelData>>,
    output: Vec<mpsc::Sender<ChannelData>>,
}
pub struct CollectColumnTransformer {
    // in milliseconds, this is used to control the granularity of the data
    input: Vec<mpsc::Receiver<ChannelData>>,
    output: Vec<mpsc::Sender<ChannelData>>,
    granularity: chrono::Duration,
}

// this is used to synethize the bar data from the raw data
pub struct ResamplingTransformer {
    // in milliseconds, this is used to control the granularity of the data
    input: Vec<mpsc::Receiver<ChannelData>>,
    output: Vec<mpsc::Sender<ChannelData>>,
    granularity: chrono::Duration,
}

pub struct CompressionTransformer {
    // in milliseconds, this is used to control the granularity of the data
    input: Vec<mpsc::Receiver<ChannelData>>,
    output: Vec<mpsc::Sender<ChannelData>>,
}
impl SampleTransformer {
    #[allow(dead_code)]
    pub fn new(input: Vec<mpsc::Receiver<ChannelData>>, output: Vec<mpsc::Sender<ChannelData>>) -> Self {
        SampleTransformer {
            input,
            output,
        }
    }
}   

impl AggTradeDataTransformer {
    #[allow(dead_code)]
    pub fn new(input: Vec<mpsc::Receiver<ChannelData>>, output: Vec<mpsc::Sender<ChannelData>>) -> Self {
        AggTradeDataTransformer {
            input,
            output,
        }
    }
}

impl SplitColumnTransformer {
    #[allow(dead_code)]
    pub fn new(input: Vec<mpsc::Receiver<ChannelData>>, output: Vec<mpsc::Sender<ChannelData>>) -> Self {
        SplitColumnTransformer {
            input,
            output,
        }
    }
}

impl CollectColumnTransformer {
    #[allow(dead_code)]
    pub fn new(input: Vec<mpsc::Receiver<ChannelData>>, output: Vec<mpsc::Sender<ChannelData>>, granularity: chrono::Duration) -> Self {
        CollectColumnTransformer {
            input,
            output,
            granularity,
        }
    }
}

impl ResamplingTransformer {
    #[allow(dead_code)]
    pub fn new(input: Vec<mpsc::Receiver<ChannelData>>, output: Vec<mpsc::Sender<ChannelData>>, granularity: chrono::Duration) -> Self {
        ResamplingTransformer {
            input,
            output,
            granularity,
        }
    }
}

impl CompressionTransformer {
    #[allow(dead_code)]
    pub fn new(input: Vec<mpsc::Receiver<ChannelData>>, output: Vec<mpsc::Sender<ChannelData>>) -> Self {
        CompressionTransformer {
            input,
            output,
        }
    }
}



impl Transformer for SampleTransformer {
    async fn transform(&mut self) -> Result<()> {
        while let Some(data) = self.input[0].recv().await {
            let data : SampleData = data.into();
            self.output[0].send(ChannelData::new(data)).await.unwrap();
        }
        Ok(())
    }
}

// we will synethize the bar after this


impl Transformer for AggTradeDataTransformer {
    async fn transform(&mut self) -> Result<()> {
        // might do something here
        while let Some(data) = self.input[0].recv().await {
            let data : AggTradeData = data.into();
            self.output[0].send(ChannelData::new(data)).await.unwrap();
        }
        Ok(())
    }
}


// split the column into 9 columns


impl Transformer for SplitColumnTransformer {
    async fn transform(&mut self) -> Result<()> {
        assert_eq!(self.input.len(), 1);
        assert_eq!(self.output.len(), 9);

        
        while let Some(data) = self.input[0].recv().await {
            let data : AggTradeData = data.into();
            let mut symbol_col = Column::default();
            symbol_col.column_name = "symbol".to_string();
            symbol_col.data = Some(Data::StringValue(data.symbol));

            let mut is_buyer_maker_col = Column::default();
            is_buyer_maker_col.column_name = "is_buyer_maker".to_string();
            is_buyer_maker_col.data = Some(Data::BoolValue(data.is_buyer_maker));

            let mut price_col = Column::default();
            price_col.column_name = "price".to_string();
            price_col.data = Some(Data::DoubleValue(data.price));

            let mut quantity_col = Column::default();
            quantity_col.column_name = "quantity".to_string();
            quantity_col.data = Some(Data::DoubleValue(data.quantity));

            let mut trade_time_col = Column::default();
            trade_time_col.column_name = "trade_time".to_string();
            trade_time_col.data = Some(Data::UintValue(data.trade_time));


            // let mut first_break_trade_id_col = Column::default();
            // first_break_trade_id_col.column_name = "first_break_trade_id".to_string();
            // first_break_trade_id_col.data = Some(Data::UintValue(data.first_break_trade_id));

            // let mut last_break_trade_id_col = Column::default();
            // last_break_trade_id_col.column_name = "last_break_trade_id".to_string();
            // last_break_trade_id_col.data = Some(Data::UintValue(data.last_break_trade_id));

            let mut aggregated_trade_id_col = Column::default();
            aggregated_trade_id_col.column_name = "aggregated_trade_id".to_string();
            aggregated_trade_id_col.data = Some(Data::UintValue(data.aggregated_trade_id));

            // let mut event_type_col = Column::default();
            // event_type_col.column_name = "event_type".to_string();
            // event_type_col.data = Some(Data::StringValue(data.event_type));

            self.output[0].send(ChannelData::new(symbol_col)).await?;
            self.output[1].send(ChannelData::new(is_buyer_maker_col)).await?;
            self.output[2].send(ChannelData::new(price_col)).await?;
            self.output[3].send(ChannelData::new(quantity_col)).await?;
            self.output[4].send(ChannelData::new(trade_time_col)).await?;
            // self.output[5].send(ChannelData::new(first_break_trade_id_col)).await?;
            // self.output[6].send(ChannelData::new(last_break_trade_id_col)).await?;
            self.output[5].send(ChannelData::new(aggregated_trade_id_col)).await?;
            // self.output[8].send(ChannelData::new(event_type_col)).await?;

        }
        Ok(())
    }
}


// this is to collect the columns
// this will be used to collect the raw data
impl Transformer for CollectColumnTransformer {
    async fn transform(&mut self) -> Result<()> {
        let mut records: Vec<Vec<Column>> = Vec::new();
        records.resize(7, Vec::new());
        // create a timestamp option
        let mut start: Option<u64> = None;
    
        let mut missing_records = 0;
        let mut last_id = 0;
        let mut is_terminated = false;
        loop {
            // if one of the input is closed, we should close exit the propagate the error
            for idx in 0..6 {
                match self.input[idx].recv().await {
                    Some(data) => {
                        let data: Column = data.into();
                        records[idx].push(data);
                    },
                    None => {
                        is_terminated = true;
                        break;
                    }
                }
            }
            if is_terminated {
                break;
            }

            // count the missing value
            let current_trade_id = match records[5].last().unwrap().data {
                Some(Data::UintValue(trade_id)) => trade_id,
                _ => panic!("trade_id is not uint"), // this is unlikely
            };
            
            if current_trade_id != last_id + 1 {
                if last_id != 0 {
                    missing_records += current_trade_id - last_id - 1;
                }
            } 
            last_id = current_trade_id; // update the last trade_id

            // check the timestamp column and see whether we should flush the data
            let latest_timestamp_col = records[5].last().unwrap();
            let current_timestamp = match latest_timestamp_col.data {
                Some(Data::UintValue(timestamp)) => timestamp,
                _ => panic!("timestamp column is not uint"), // this is unlikely
            };
            
            if start.is_none() {
                start = Some(current_timestamp);
                continue;
            }
            
            // if the time difference is greater than the granularity, we should flush the data
            if current_timestamp - start.unwrap() >= self.granularity.num_milliseconds() as u64 {
                let schema = Schema::new(vec![
                    Field::new("symbol", DataType::Utf8, false),
                    Field::new("is_buyer_maker", DataType::Boolean, false),
                    Field::new("price", DataType::Float64, false),
                    Field::new("quantity", DataType::Float64, false),
                    Field::new("trade_time", DataType::UInt64, false),
                    Field::new("aggregated_trade_id", DataType::UInt64, false),
                    Field::new("missing_count", DataType::UInt32, false),
                ]);
                let mut arrays: Vec<Arc<dyn arrow::array::Array>> = Vec::new();
                for idx in 0..6 {
                    // decide the data type of each column
                    // and convert to arrow array
                    let array = match &records[idx].last().as_mut().unwrap().data {
                        Some(column) => {
                            match column {
                                Data::StringValue(_) => {
                                    // obtain a rust string array
                                    let string_array: Vec<String> = records[idx].iter().map(|col| {
                                        match col.data.clone() {
                                            Some(Data::StringValue(value)) => value,
                                            _ => "null".to_string(),
                                        }
                                    }).collect();
                                    Arc::new(StringArray::from(string_array)) as Arc<dyn arrow::array::Array>
                                },
                                Data::DoubleValue(_) => {
                                    // we should create a double array
                                    let double_array : Vec<f64> = records[idx].iter().map(|col| {
                                        match col.data {
                                            Some(Data::DoubleValue(value)) => value,
                                            _ => 0.0,
                                        }
                                    }).collect();
                                    Arc::new(Float64Array::from(double_array)) as Arc<dyn arrow::array::Array>
                                },
                                Data::UintValue(_) => {
                                    let uint_array: Vec<u64> = records[idx].iter().map(|col| {
                                        match col.data {
                                            Some(Data::UintValue(value)) => value,
                                            _ => 0,
                                        }
                                    }).collect();
                                    Arc::new(arrow::array::UInt64Array::from(uint_array)) as Arc<dyn arrow::array::Array>
                                },
                                Data::BoolValue(_) => {
                                    let bool_array: Vec<bool> = records[idx].iter().map(|col| {
                                        match col.data {
                                            Some(Data::BoolValue(value)) => value,
                                            _ => false,
                                        }
                                    }).collect();
                                    Arc::new(arrow::array::BooleanArray::from(bool_array)) as Arc<dyn arrow::array::Array>
                            },
                        }
                    }
                    , None => {
                        // this is unlikely
                        panic!("no data in the column");
                    }

                
                };
                arrays.push(array);
                
            }

            // for 7th column, we should create a array with single value, which is the number of missing records
            let missing_records_array = arrow::array::UInt64Array::from(vec![Some(missing_records)]);
            arrays.push(Arc::new(missing_records_array) as Arc<dyn arrow::array::Array>);
            let record_batch = RecordBatch::try_new(Arc::new(schema), arrays).unwrap();
            self.output[0].send(ChannelData::new(record_batch)).await?;
                
        };
        
        
        
    }
        Ok(())
   }
}


impl Transformer for ResamplingTransformer {
    
    async fn transform(&mut self) -> Result<()> {
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
        let this_time_gap = self.granularity.num_milliseconds() as u64;
        

        loop {
            let agg_trade = self.input[0].recv().await;
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
            quote_asset_volume += agg_trade.price;

            taker_buy_base_asset_volume += {
                if !agg_trade.is_buyer_maker {
                    agg_trade.quantity
                } else {
                    0.0
                }
            };

            taker_buy_quote_asset_volume += {
                if !agg_trade.is_buyer_maker {
                    agg_trade.price
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
                self.output[0].send(ChannelData::new(record_batch)).await?;

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

impl Transformer for CompressionTransformer {
    async fn transform(&mut self) -> Result<()> {
        let record_batch: RecordBatch = self.input[0].recv().await.unwrap().into();
        let mut buffer = Vec::new();
        let mut writer = arrow::ipc::writer::StreamWriter::try_new(&mut buffer, &record_batch.schema()).ok().expect("writer error");
        writer.write(&record_batch).ok().expect( "write error");
        writer.finish().ok().expect("finish error");
        // Compress the serialized data using lz4
        let mut encoder = lz4::EncoderBuilder::new()
            .block_mode(lz4::BlockMode::Linked)
            .build(Vec::new())
            .unwrap();
        let (first_part, second_part) = buffer.split_at(buffer.len());
        encoder.write_all(first_part).unwrap();
        encoder.write_all(second_part).unwrap();
        let (result, _) = encoder.finish();
        self.output[0].send(ChannelData::new(result)).await.unwrap();
        Ok(())
    }
}




// pipelines start here


// TODO: make dynamic dispatch work, this is not very safe
pub struct ResamplingPipeline {

}

impl ResamplingPipeline {
    pub fn new() -> Self {
        ResamplingPipeline {
        }
    }

    pub fn launch_transformer(&mut self, mut transformer: &dyn Transformer) -> Box<dyn Future<Output = Result<()>>> {
        Box::new(transformer.transform())
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