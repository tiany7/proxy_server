pub mod trade {
    include!("../../../proto/generated_code/trade.rs");
}

use std::convert::{TryFrom, TryInto};
use std::vec::Vec;
use std::any::Any;
use std::sync::Arc;
use std::io::Write;
use std::future::Future;

use actix_web::middleware;
use futures::{FutureExt, SinkExt};
use arrow::datatypes::{Schema, ToByteSlice};
use tokio::sync::{mpsc, Mutex};
use anyhow::{Error, Ok, Result};
use async_trait::async_trait;
use arrow::record_batch::RecordBatch;
use arrow::datatypes::{DataType, Field};
use arrow::array::{ArrayRef, Float64Array, StringArray};
use core::num;
use tracing::info;
use chrono::{Utc, DateTime, TimeZone, Duration as ChronoDuration};

use metrics_server::MISSING_VALUES_COUNT;


// 定义一个动态数据类型
type DynData = Box<dyn Any + Send + Sync>;

// 定义一个数据结构，包装了动态数据
pub struct ChannelData(DynData);




use trade::AggTradeData;    
use trade::Column;
use trade::BarData;

#[derive(Debug, Clone)]
pub enum TimeUnit {
    Millisecond(i64),
    Second(i64),
    Minute(i64),
    Hour(i64),
    Day(i64),
    Week(i64),
}

struct SampleData {
    name: String,
    age: i32,
}

impl TimeUnit {
    pub fn as_milliseconds(&self) -> i64 {
        match *self {
            TimeUnit::Millisecond(ms) => ms,
            TimeUnit::Second(s) => s * 1000,
            TimeUnit::Minute(m) => m * 60 * 1000,
            TimeUnit::Hour(h) => h * 60 * 60 * 1000,
            TimeUnit::Day(d) => d * 24 * 60 * 60 * 1000,
            TimeUnit::Week(w) => w * 7 * 24 * 60 * 60 * 1000,
        }
    }

    pub fn to_string(&self) -> String {
        match *self {
            TimeUnit::Millisecond(ms) => format!("{}ms", ms),
            TimeUnit::Second(s) => format!("{}s", s),
            TimeUnit::Minute(m) => format!("{}m", m),
            TimeUnit::Hour(h) => format!("{}h", h),
            TimeUnit::Day(d) => format!("{}d", d),
            TimeUnit::Week(w) => format!("{}w", w),
        }
    }

    pub fn duration_until_next(&self, now: DateTime<Utc>) -> tokio::time::Duration {
        let next_time = match *self {
            TimeUnit::Millisecond(ms) => {
                let next_ms = ((now.timestamp_millis() / ms) + 1) * ms;
                Utc.timestamp_millis(next_ms)
            },
            TimeUnit::Second(s) => {
                let next_s = ((now.timestamp() / s) + 1) * s;
                Utc.timestamp(next_s, 0)
            },
            TimeUnit::Minute(m) => {
                let next_m = ((now.timestamp() / 60 / m) + 1) * 60 * m;
                Utc.timestamp(next_m, 0)
            },
            TimeUnit::Hour(h) => {
                let next_h = ((now.timestamp() / 3600 / h) + 1) * 3600 * h;
                Utc.timestamp(next_h, 0)
            },
            TimeUnit::Day(d) => {
                (now.date() + ChronoDuration::days(d)).and_hms(0, 0, 0)
            },
            TimeUnit::Week(w) => {
                (now.date() + ChronoDuration::weeks(w)).and_hms(0, 0, 0)
            },
        };

        let duration = next_time - now;
        tokio::time::Duration::from_millis(duration.num_milliseconds() as u64)
    }
}


#[derive(Debug, Clone)]
struct DataSlice {
    low_price: f64,
    high_price: f64,
    open_price: f64,
    close_price: f64,
    volume: f64,
    quote_asset_volume: f64,
    number_of_trades: u64,
    taker_buy_base_asset_volume: f64,
    taker_buy_quote_asset_volume: f64,
    min_id: u64,
    max_id: u64,
    missing_count: u64,
    open_time: u64,
    close_time: u64,
    last: Option<u64>,
}

impl Default for DataSlice {
    fn default() -> Self {
        DataSlice {
            low_price: f64::MAX,
            high_price: f64::MIN,
            open_price: 0.0,
            close_price: 0.0,
            volume: 0.0,
            quote_asset_volume: 0.0,
            number_of_trades: 0,
            taker_buy_base_asset_volume: 0.0,
            taker_buy_quote_asset_volume: 0.0,
            min_id: 0,
            max_id: 0,
            missing_count: 0,
            open_time: 0,
            close_time: 0,
            last: None,
        }
    }
}

impl DataSlice {
    // 添加一个方法来重置所有数据
    fn reset(&mut self) {
        self.low_price = f64::MAX;
        self.high_price = f64::MIN;
        self.open_price = 0.0;
        self.close_price = 0.0;
        self.volume = 0.0;
        self.quote_asset_volume = 0.0;
        self.number_of_trades = 0;
        self.taker_buy_base_asset_volume = 0.0;
        self.taker_buy_quote_asset_volume = 0.0;
        self.min_id = 0;
        self.max_id = 0;
        self.missing_count = 0;
        self.open_time = 0;
        self.close_time = 0;
        self.last = None;
    }
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

impl Into<BarData> for ChannelData {
    fn into(self) -> BarData {
        *self.0.downcast::<BarData>().unwrap()
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
    granularity: TimeUnit,
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
    pub fn new(input: Vec<mpsc::Receiver<ChannelData>>, output: Vec<mpsc::Sender<ChannelData>>, granularity: TimeUnit) -> Self {
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

    fn record_batch_to_bytes(record_batch: &RecordBatch) -> Result<Vec<u8>, anyhow::Error> {
        let mut buffer = std::io::Cursor::new(Vec::new());
        // 创建IPC文件写入器
        let mut writer = arrow::ipc::writer::FileWriter::try_new(&mut buffer, &record_batch.schema())?;
        writer.write(&record_batch)?;
        writer.finish().map_err(|e|  anyhow::anyhow!("finish error: {}", e))?;
        drop(writer);
        let buffer = buffer.into_inner();
        Ok(buffer)
    }
}




#[async_trait]
impl Transformer for ResamplingTransformer {
    
    async fn transform(&self) -> Result<()> {
        // what we are gonna to do here is to dispatch the tasks into two coroutines
        // first one is to compute the bar data
        // second one is to take account of time and freeze the first coroutine when it is the time to send the data
        let data = Arc::new(Mutex::new(DataSlice::default()));
        let data_clone = data.clone();
        let this = self.inner.lock().await;
        let this_time_gap = this.granularity.clone();

        drop(this);

        let inner_clone = self.inner.clone();
        let inner_another_clone = self.inner.clone();
        let resampler = tokio::spawn(async move {
            loop {
                let mut ticket = inner_clone.lock().await;
                let agg_trade = ticket.input[0].recv().await;
                drop(ticket);

                if agg_trade.is_none() {
                    break;
                }
                let agg_trade: AggTradeData = agg_trade.unwrap().into();
                
                let mut data_ticket = data.lock().await;
                // count number of trades
                data_ticket.number_of_trades += 1;
    
                // update the min_id and max_id
                if data_ticket.last.is_some() {
                    data_ticket.missing_count += agg_trade.aggregated_trade_id - data_ticket.last.unwrap() - 1; 
                }
    
                data_ticket.last = Some(agg_trade.aggregated_trade_id);
    
                assert!(data_ticket.missing_count >= 0);
    
                if agg_trade.price < data_ticket.low_price {
                    data_ticket.low_price = agg_trade.price;
                    data_ticket.min_id = agg_trade.aggregated_trade_id;
                }
    
                if agg_trade.price > data_ticket.high_price {
                    data_ticket.high_price = agg_trade.price;
                    data_ticket.max_id = agg_trade.aggregated_trade_id;
                }
    
                if data_ticket.number_of_trades == 1 {
                    data_ticket.open_price = agg_trade.price;
                    data_ticket.open_time = agg_trade.trade_time;
                }
    
                data_ticket.volume += agg_trade.quantity;
                data_ticket.quote_asset_volume += agg_trade.price * agg_trade.quantity;
    
                data_ticket.taker_buy_base_asset_volume += {
                    if !agg_trade.is_buyer_maker {
                        agg_trade.quantity
                    } else {
                        0.0
                    }
                };
    
                data_ticket.taker_buy_quote_asset_volume += {
                    if !agg_trade.is_buyer_maker {
                        agg_trade.price * agg_trade.quantity
                    } else {
                        0.0
                    }
                };
            }
            Ok(())
        });

        // this task will account for the time
        let time_counter = tokio::spawn(async move{
            // get now from tokio 
            let now = Utc::now();
            let duration = this_time_gap.duration_until_next(now);
            loop {
                let _ = tokio::time::sleep(duration).await;
                let mut ticket = data_clone.lock().await;
                let data_ticket = (*ticket).clone();
                ticket.reset();
                drop(ticket);
                tracing::info!("Received data: {}", 111);
                let bar_data = BarData {
                    low: data_ticket.low_price,
                    high: data_ticket.high_price,
                    open: data_ticket.open_price,
                    close: data_ticket.close_price,
                    volume: data_ticket.volume,
                    quote_asset_volume: data_ticket.quote_asset_volume,
                    number_of_trades: data_ticket.number_of_trades,
                    taker_buy_base_asset_volume: data_ticket.taker_buy_base_asset_volume,
                    taker_buy_quote_asset_volume: data_ticket.taker_buy_quote_asset_volume,
                    min_id: data_ticket.min_id,
                    max_id: data_ticket.max_id,
                    missing_count: data_ticket.missing_count,
                    open_time: data_ticket.open_time,
                    close_time: data_ticket.close_time,
                };
                let ticket = inner_another_clone.lock().await;
                let res = ticket.output[0].send(ChannelData::new(bar_data)).await;
                if res.is_err() {
                    break;
                }
            }
            Ok(())
        });

        let _ = futures::future::join(resampler, time_counter).await;

        Ok(())
    }
        
}


// TODO: implement this
#[async_trait]
impl Transformer for CompressionTransformer {
    async fn transform(&self) -> Result<()> {
        loop {
            let mut ticket = self.inner.lock().await;
            match ticket.input[0].recv().await {
                Some(data) => {
                    let data: BarData = data.into();
                    // let serialized_bytes = CompressionTransformer::record_batch_to_bytes(&data)?;
                    // let compressed = lz4_flex::compress_prepend_size(serialized_bytes.to_byte_slice());
                    let res = ticket.output[0].send(ChannelData::new(data)).await;
                    if res.is_err() {
                        info!("pipe closed {:?}", res);
                        break;
                    }
                },
                None => {
                    break;
                }
            };
        }
        
        Ok(())
    }
}




// pipelines start here


// TODO: make dynamic dispatch work, this is not very safe
pub struct ResamplingPipeline {
    transformers: Vec<Box<dyn Transformer>>,
}

impl ResamplingPipeline {
    pub fn new() -> Self {
        ResamplingPipeline {
            transformers: Vec::new(),
        }
    }

    pub async fn run_and_wait(&self) -> Result<()> {
        let mut tasks = Vec::new();
        for transformer in self.transformers.iter() {
            tasks.push(transformer.transform());
        }
        futures::future::join_all(tasks).await;
        Ok(())
    }
}
#[cfg(test)]
mod tests {
    use std::io::Split;

    use super::*;
    use tokio::{test, spawn};
    #[test]
    async fn test_pipeline_uncompress() {
        let mut agg_trade = AggTradeData {
            symbol: "BTCUSDT".to_string(),
            aggregated_trade_id: 1,
            price: 114514.0,
            quantity: 1.0,
            trade_time: 1000,
            is_buyer_maker: false,
            event_type : "trade".to_string(),
            first_break_trade_id: 0,
            last_break_trade_id: 0,
        };
        let mut agg_trade2 = AggTradeData{
            symbol: "BTCUSDT".to_string(),
            aggregated_trade_id: 2,
            price: 1919810.0,
            quantity: 1.0,
            trade_time: 114514,
            is_buyer_maker: false,
            event_type : "trade".to_string(),
            first_break_trade_id: 0,
            last_break_trade_id: 0,
        };

        let (tx, rx) = create_channel(10);
        let (tx2, mut rx2) = create_channel(10);
        let resample_trans = ResamplingTransformer::new(vec![rx], vec![tx2], TimeUnit::Second(1));
        
        let handle = tokio::spawn(async move {
            let _ = resample_trans.transform().await;
        });
        tx.send(ChannelData::new(agg_trade)).await.unwrap();
        tx.send(ChannelData::new(agg_trade2)).await.unwrap();
        let data = rx2.recv().await.unwrap();
        let data: RecordBatch = data.into();
        assert_eq!(data.num_columns(), 14);
        assert_eq!(data.num_rows(), 1);
        println!("{:?}", data.schema());

        let open = data.column(0).as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(open.value(0), 114514.0);
        info!("{:?}", open.value(0));
        
    }   

}