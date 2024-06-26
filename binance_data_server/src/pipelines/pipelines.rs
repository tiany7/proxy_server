pub mod trade {
    include!("../../../proto/generated_code/trade.rs");
}


use std::any::Any;
use std::io::Write;
use std::ops::Add;
use std::sync::Arc;
use std::vec::Vec;
use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::{Ok, Result};
use async_trait::async_trait;
use crossbeam::atomic::AtomicCell;
use crossbeam::sync::ShardedLockReadGuard;
use futures::SinkExt;
use tokio::sync::{mpsc, Mutex};
use tracing::{error, info};
use metrics_server::MISSING_VALUES_COUNT;
use trade::AggTradeData;
use trade::BarData;
use trade::Column;
use tokio_schedule::Job;

use crate::pipelines::utils::ReadPriorityRwLock;


type DynData = Box<dyn Any + Send + Sync>;

// 定义一个数据结构，包装了动态数据
pub struct ChannelData(DynData);



struct SampleData {
    name: String,
    age: i32,
}


impl ChannelData {
    pub fn new<T: 'static + std::marker::Send + std::marker::Sync>(data: T) -> Self {
        ChannelData(Box::new(data))
    }
}

impl From<ChannelData> for SampleData {
    fn from(val: ChannelData) -> Self {
        *val.0.downcast::<SampleData>().unwrap()
    }
}


impl From<ChannelData> for AggTradeData {
    fn from(val: ChannelData) -> Self {
        *val.0.downcast::<AggTradeData>().unwrap()
    }
}

impl From<ChannelData> for Column {
    fn from(val: ChannelData) -> Self {
        *val.0.downcast::<Column>().unwrap()
    }
}

impl From<ChannelData> for Vec<u8> {
    fn from(val: ChannelData) -> Self {
        *val.0.downcast::<Vec<u8>>().unwrap()
    }
}

impl From<ChannelData> for BarData {
    fn from(val: ChannelData) -> Self {
        *val.0.downcast::<BarData>().unwrap()
    }
}



#[allow(dead_code)]
#[derive(Debug, Clone, Copy)]
pub struct DataSlice {
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

impl From<ChannelData> for DataSlice {
    fn from(val: ChannelData) -> Self {
        *val.0.downcast::<DataSlice>().unwrap()
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

    pub fn to_bar_data(&self) -> BarData {
        BarData {
            low: self.low_price,
            high: self.high_price,
            open: self.open_price,
            close: self.close_price,
            volume: self.volume,
            quote_asset_volume: self.quote_asset_volume,
            number_of_trades: self.number_of_trades,
            taker_buy_base_asset_volume: self.taker_buy_base_asset_volume,
            taker_buy_quote_asset_volume: self.taker_buy_quote_asset_volume,
            min_id: self.min_id,
            max_id: self.max_id,
            missing_count: self.missing_count,
            open_time: self.open_time,
            close_time: self.close_time,
        }
    }
}

pub fn create_channel(capacity: usize) -> (mpsc::Sender<ChannelData>, mpsc::Receiver<ChannelData>) {
    let (tx, rx) = mpsc::channel(capacity);
    (tx, rx)
}

#[async_trait]
pub trait Transformer {
    // this is blocking
    async fn transform<'a>(&'a self) -> Result<()>;
}

// this is used to synethize the bar data from the raw data
pub struct ResamplingTransformerInner {
    // in milliseconds, this is used to control the granularity of the data
    input: Vec<mpsc::Receiver<ChannelData>>,
    granularity: chrono::Duration,
}

pub struct CompressionTransformerInner {
    input: Vec<mpsc::Receiver<ChannelData>>,
    output: Vec<mpsc::Sender<ChannelData>>,
}

pub struct ResamplingTransformer {
    inner: Arc<Mutex<ResamplingTransformerInner>>,
    output: Vec<mpsc::Sender<ChannelData>>,  // this need not to be placed in inner
}

pub struct CompressionTransformer {
    inner: Arc<Mutex<CompressionTransformerInner>>,
}

impl ResamplingTransformer {
    #[allow(dead_code)]
    pub fn new(
        input: Vec<mpsc::Receiver<ChannelData>>,
        output: Vec<mpsc::Sender<ChannelData>>,
        granularity: chrono::Duration,
    ) -> Self {
        ResamplingTransformer {
            inner: Arc::new(Mutex::new(ResamplingTransformerInner {
                input,
                granularity,
            })),
            output,
        }
    }
}

impl CompressionTransformer {
    #[allow(dead_code)]
    pub fn new(
        input: Vec<mpsc::Receiver<ChannelData>>,
        output: Vec<mpsc::Sender<ChannelData>>,
    ) -> Self {
        CompressionTransformer {
            inner: Arc::new(Mutex::new(CompressionTransformerInner { input, output })),
        }
    }

    fn record_batch_to_bytes() -> Result<Vec<u8>, anyhow::Error> {
        unimplemented!();
    }
}

#[async_trait]
impl Transformer for ResamplingTransformer {
    #[tracing::instrument(skip(self))]
    async fn transform(&self) -> Result<()> {
        let this = self.inner.lock().await;
        let this_time_gap = this.granularity;
        drop(this);

        // create a shared data structure to allow routinely update
        let data = Arc::new(AtomicCell::new(DataSlice::default()));
        let data_clone = data.clone();
        let atomic_lock = Arc::new(AtomicBool::new(false));
        let atomic_lock_timer = atomic_lock.clone();
        let should_quit = Arc::new(AtomicBool::new(false));
        let should_quit_clone = should_quit.clone();
        
        let inner_clone = self.inner.clone();
        // timer task
        let notify = Arc::new(tokio::sync::Notify::new());
        let notify_clone = notify.clone();

        tokio::spawn(async move {
            loop {
                let mut ticket = inner_clone.lock().await;
                let agg_trade = ticket.input[0].recv().await;
                drop(ticket);
                if agg_trade.is_none() {
                    break;
                }
            
                let agg_trade: AggTradeData = agg_trade.unwrap().into();
                // while atomic_lock.compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed).is_err() {
                //     continue;
                // }
                let this_data = data.load();
                let mut copied_data = this_data;
                // count number of trades
                copied_data.number_of_trades += 1;
                copied_data.close_price = agg_trade.price;
                copied_data.close_time = agg_trade.trade_time;
                // update the min_id and max_id
                if copied_data.last.is_some() {
                    copied_data.missing_count +=
                        agg_trade.aggregated_trade_id - copied_data.last.unwrap() - 1;
                }

                copied_data.last = Some(agg_trade.aggregated_trade_id);

                if agg_trade.price < copied_data.low_price {
                    copied_data.low_price = agg_trade.price;
                    copied_data.min_id = agg_trade.aggregated_trade_id;
                }

                if agg_trade.price > copied_data.high_price {
                    copied_data.high_price = agg_trade.price;
                    copied_data.max_id = agg_trade.aggregated_trade_id;
                }

                if copied_data.number_of_trades == 1 {
                    copied_data.open_price = agg_trade.price;
                    copied_data.open_time = agg_trade.trade_time;
                }

                copied_data.volume += agg_trade.quantity;
                copied_data.quote_asset_volume += agg_trade.price * agg_trade.quantity;

                copied_data.taker_buy_base_asset_volume += {
                    if !agg_trade.is_buyer_maker {
                        agg_trade.quantity
                    } else {
                        0.0
                    }
                };

                copied_data.taker_buy_quote_asset_volume += {
                    if !agg_trade.is_buyer_maker {
                        agg_trade.price * agg_trade.quantity
                    } else {
                        0.0
                    }
                };
                // write back to the shared data structure
                data.store(copied_data);
                // if the lock is acquired, notify the other task
                if atomic_lock.compare_exchange_weak(true, false, Ordering::Relaxed, Ordering::Relaxed).is_ok() {
                    notify.notify_one();
                    tokio::task::yield_now().await;
                }
                // let system_time = std::time::SystemTime::now();
                // let duration_since_epoch = system_time.duration_since(std::time::UNIX_EPOCH).expect("Time went backwards").as_millis() as u64;
                // delay_duration += duration_since_epoch - agg_trade.trade_time;
                // tracing::error!("time diff: {:?}ms", duration_since_epoch - agg_trade.trade_time);
            }
            // write back to the shared data structure
        });
        let tx = self.output[0].clone();
        tokio::spawn(async move{
            let interval_duration = this_time_gap.to_std().unwrap().add(std::time::Duration::from_millis(5));
                let mut interval = tokio::time::interval(interval_duration);

                info!(
                "Resampling transformer started with time gap: {:?}",
                interval_duration
            );
            loop {
                interval.tick().await;
                if should_quit_clone.load(Ordering::Relaxed) {
                    break;
                }
                while atomic_lock_timer.compare_exchange_weak(false, true, Ordering::Acquire, Ordering::Relaxed).is_err() {
                    
                }
            }
        }
        );
        // launch another task to routinely update the data
        tokio::spawn(async move {
            loop {
                notify_clone.notified().await;
                let this_data = data_clone.load();
                data_clone.store(DataSlice::default());
                if tx.send(ChannelData::new(this_data)).await.is_err() {
                    should_quit.store(true, Ordering::Relaxed);
                    break;
                }
                
            }
        });


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
                }
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
    use tokio::{spawn, test};
}
