pub mod trade {
    include!("../../../proto/generated_code/trade.rs");
}

use std::any::Any;
use std::ops::Add;
use std::result::Result::Ok;
use std::sync::atomic::{AtomicBool, AtomicI8, Ordering};
use std::sync::Arc;
use std::vec::Vec;

use anyhow::Result;
use async_trait::async_trait;
use crossbeam::atomic::AtomicCell;
use crossbeam::sync::ShardedLockReadGuard;
use futures::SinkExt;
use metrics_server::MISSING_VALUES_COUNT;
use tokio::sync::{mpsc, Mutex};
use tokio_schedule::Job;
use tracing::{error, info};
use trade::AggTradeData;
use trade::BarData;
use trade::Column;

use super::utils as pipeline_utils;
use pipeline_utils::{next_aligned_instant, next_interval_time_point};

#[derive(Debug, Clone)]
pub struct Segment {
    pub start: u64,
    pub end: u64,
}

impl Segment {
    pub fn try_new(start: u64, end: u64) -> Result<Self, anyhow::Error> {
        if end < start {
            Err(anyhow::anyhow!("End must be greater than start"))
        } else {
            Ok(Segment { start, end })
        }
    }

    pub fn size(&self) -> u64 {
        self.end - self.start
    }
}

type DynData = Box<dyn Any + Send + Sync>;

// wrap the dynamic data
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

impl From<ChannelData> for pipeline_utils::Segment {
    fn from(val: ChannelData) -> Self {
        *val.0.downcast::<pipeline_utils::Segment>().unwrap()
    }
}

impl From<ChannelData> for Vec<AggTradeData> {
    fn from(val: ChannelData) -> Self {
        *val.0.downcast::<Vec<AggTradeData>>().unwrap()
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
    symbol: &'static str,
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
            symbol: "",
        }
    }
}

impl From<ChannelData> for DataSlice {
    fn from(val: ChannelData) -> Self {
        *val.0.downcast::<DataSlice>().unwrap()
    }
}

impl DataSlice {
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

    pub fn update_from_agg_trade(&mut self, agg_trade: AggTradeData) {
        self.number_of_trades += 1;
        self.close_price = agg_trade.price;
        self.close_time = agg_trade.trade_time;
        if self.open_time == 0 {
            self.open_time = agg_trade.trade_time;
            self.open_price = agg_trade.price;
        }
        if self.last.is_none() {
            self.last = Some(agg_trade.aggregated_trade_id);
        }
        if self.last.unwrap() < agg_trade.aggregated_trade_id {
            self.last = Some(agg_trade.aggregated_trade_id);
        }
        if agg_trade.price < self.low_price {
            self.low_price = agg_trade.price;
            self.min_id = agg_trade.aggregated_trade_id;
        }
        if agg_trade.price > self.high_price {
            self.high_price = agg_trade.price;
            self.max_id = agg_trade.aggregated_trade_id;
        }
        if self.number_of_trades == 1 {
            self.open_price = agg_trade.price;
            self.open_time = agg_trade.trade_time;
        }
        self.volume += agg_trade.quantity;
        self.quote_asset_volume += agg_trade.price * agg_trade.quantity;
        self.taker_buy_base_asset_volume += {
            if !agg_trade.is_buyer_maker {
                agg_trade.quantity
            } else {
                0.0
            }
        };
        self.taker_buy_quote_asset_volume += {
            if !agg_trade.is_buyer_maker {
                agg_trade.price * agg_trade.quantity
            } else {
                0.0
            }
        };
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
            symbol: self.symbol.to_string(),
        }
    }

    pub fn to_bar_data_with_symbol(&self, symbol: String) -> BarData {
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
            symbol,
        }
    }
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

pub struct ResamplingTransformerInnerV2 {
    // in milliseconds, this is used to control the granularity of the data
    input: Vec<mpsc::Receiver<AggTradeData>>,
    granularity: chrono::Duration,
}

pub struct CompressionTransformerInner {
    input: Vec<mpsc::Receiver<ChannelData>>,
    output: Vec<mpsc::Sender<ChannelData>>,
}

pub struct ResamplingTransformer {
    inner: Arc<Mutex<ResamplingTransformerInner>>,
    output: Vec<mpsc::Sender<ChannelData>>, // this need not to be placed in inner
}

pub struct CompressionTransformer {
    inner: Arc<Mutex<CompressionTransformerInner>>,
}

pub struct ResamplingTransformerWithTiming {
    inner: Arc<Mutex<ResamplingTransformerInnerV2>>,
    output: Vec<mpsc::Sender<BarData>>,
    name: String,
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
}

#[async_trait]
impl Transformer for ResamplingTransformer {
    #[tracing::instrument(skip(self))]
    async fn transform(&self) -> Result<()> {
        let this = self.inner.lock().await;
        let this_time_gap = this.granularity;
        drop(this);
        let recovery_enabled = self.output.len() >= 2;
        // create a shared data structure to allow routinely update
        let data = Arc::new(AtomicCell::new(DataSlice::default()));
        let data_clone = data.clone();
        let atomic_lock = Arc::new(AtomicBool::new(false));
        let atomic_lock_timer = atomic_lock.clone();
        let should_quit = Arc::new(AtomicBool::new(false));
        let should_quit_clone = should_quit.clone();
        let sent_to_downstream = Arc::new(tokio::sync::Notify::new());
        let sent_to_downstream_clone = sent_to_downstream.clone();
        let inner_clone = self.inner.clone();
        // timer task
        let notify = Arc::new(tokio::sync::Notify::new());
        let notify_clone = notify.clone();
        let mut cursor = 0; // this is the global cursor that records the current id position
        tokio::spawn(async move {
            let mut missing_segments = vec![];
            let mut nr_missing = 0;
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
                // update the cursor
                if cursor == 0 {
                    cursor = agg_trade.aggregated_trade_id;
                } else {
                    if agg_trade.aggregated_trade_id - cursor > 1 {
                        if let Ok(missed) =
                            pipeline_utils::Segment::try_new(cursor, agg_trade.aggregated_trade_id)
                        {
                            missing_segments.push(missed.clone());
                            nr_missing += missed.size();
                        }
                    }
                    cursor = agg_trade.aggregated_trade_id;
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
                if atomic_lock
                    .compare_exchange_weak(true, false, Ordering::Relaxed, Ordering::Relaxed)
                    .is_ok()
                {
                    // perform the recovery process if needed
                    // >= 2 : the disaster recovery is enabled
                    // unlikely

                    // if recovery_enabled && missing_segments.len() > 0 {
                    //     let mut start_id = 0;
                    //     let mut end_id = 0;
                    //     if let Some(ref fst) = missing_segments.first() {
                    //         start_id = fst.start;
                    //     }
                    //     if let Some(ref fst) = missing_segments.last() {
                    //         end_id = fst.end;
                    //     }
                    //     if let Err(e) = self.output[1]
                    //         .send(ChannelData::new(utils::Segment::new(start_id, end_id)))
                    //         .await
                    //     {
                    //         tracing::error!("Error recovering segment: {:?}", e);
                    //     }
                    //     let mut ticket = inner_clone.lock().await;
                    //     if let Some(c_data) = ticket.input[1].recv().await {
                    //         let data_vec: Vec<AggTradeData> = c_data.into();
                    //         let data_map = data_vec
                    //             .into_iter()
                    //             .map(|x| (x.aggregated_trade_id, x))
                    //             .collect::<std::collections::HashMap<u64, AggTradeData>>();
                    //         let mut copied_data = data.load();
                    //         missing_segments.iter().for_each(|x| {
                    //             for i in x.start..x.end {
                    //                 if let Some(agg_trade) = data_map.get(&i) {
                    //                     copied_data.number_of_trades += 1;
                    //                     nr_missing -= 1;
                    //                     // copied_data.last = Some(agg_trade.aggregated_trade_id);
                    //                     if agg_trade.price < copied_data.low_price {
                    //                         copied_data.low_price = agg_trade.price;
                    //                         copied_data.min_id = agg_trade.aggregated_trade_id;
                    //                     }

                    //                     if agg_trade.price > copied_data.high_price {
                    //                         copied_data.high_price = agg_trade.price;
                    //                         copied_data.max_id = agg_trade.aggregated_trade_id;
                    //                     }

                    //                     if copied_data.open_time > agg_trade.trade_time {
                    //                         copied_data.open_price = agg_trade.price;
                    //                         copied_data.open_time = agg_trade.trade_time;
                    //                     }
                    //                     if copied_data.close_time < agg_trade.trade_time {
                    //                         copied_data.close_price = agg_trade.price;
                    //                         copied_data.close_time = agg_trade.trade_time;
                    //                     }

                    //                     copied_data.volume += agg_trade.quantity;
                    //                     copied_data.quote_asset_volume +=
                    //                         agg_trade.price * agg_trade.quantity;

                    //                     copied_data.taker_buy_base_asset_volume += {
                    //                         if !agg_trade.is_buyer_maker {
                    //                             agg_trade.quantity
                    //                         } else {
                    //                             0.0
                    //                         }
                    //                     };

                    //                     copied_data.taker_buy_quote_asset_volume += {
                    //                         if !agg_trade.is_buyer_maker {
                    //                             agg_trade.price * agg_trade.quantity
                    //                         } else {
                    //                             0.0
                    //                         }
                    //                     };
                    //                     // write back to the shared data structure
                    //                 }
                    //             }
                    //         });
                    //     }
                    //     data.store(copied_data);

                    //     MISSING_VALUES_COUNT.inc_by(nr_missing as u64);
                    //     missing_segments.clear();
                    // }

                    notify.notify_one();
                    sent_to_downstream.notified().await;
                    // tokio::task::yield_now().await;
                }
                // let system_time = std::time::SystemTime::now();
                // let duration_since_epoch = system_time.duration_since(std::time::UNIX_EPOCH).expect("Time went backwards").as_millis() as u64;
                // delay_duration += duration_since_epoch - agg_trade.trade_time;
                // tracing::error!("time diff: {:?}ms", duration_since_epoch - agg_trade.trade_time);
            }
            // write back to the shared data structure
        });
        let tx = self.output[0].clone();
        tokio::spawn(async move {
            let interval_duration = this_time_gap
                .to_std()
                .unwrap()
                .add(std::time::Duration::from_millis(15));
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
                while atomic_lock_timer
                    .compare_exchange_weak(false, true, Ordering::Acquire, Ordering::Relaxed)
                    .is_err()
                {}
                sent_to_downstream_clone.notify_one();
            }
        });
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

impl ResamplingTransformerWithTiming {
    #[allow(dead_code)]
    pub fn new(
        input: Vec<mpsc::Receiver<AggTradeData>>,
        output: Vec<mpsc::Sender<BarData>>,
        granularity: chrono::Duration,
        name: String,
    ) -> Self {
        ResamplingTransformerWithTiming {
            inner: Arc::new(Mutex::new(ResamplingTransformerInnerV2 {
                input,
                granularity,
            })),
            output,
            name,
        }
    }
}

#[async_trait]
impl Transformer for ResamplingTransformerWithTiming {
    #[tracing::instrument(skip(self))]
    async fn transform(&self) -> Result<()> {
        // there is a timer that controls the instant to send the message to down stream
        let this = self.inner.lock().await;
        let this_time_gap = this.granularity;
        drop(this);
        // create a shared data structure to allow routinely update

        let data = Arc::new(AtomicCell::new(DataSlice::default()));
        let inner_clone = self.inner.clone();
        let inner_clone_v2 = inner_clone.clone();
        let data_clone = data.clone();
        let data_clone_v2 = data_clone.clone();
        let atomic_lock = Arc::new(AtomicBool::new(false));
        let atomic_lock_timer = atomic_lock.clone();
        let should_quit = Arc::new(AtomicBool::new(false));
        let should_quit_clone = should_quit.clone();
        let sent_to_downstream = Arc::new(tokio::sync::Notify::new());
        let sent_to_downstream_clone = sent_to_downstream.clone();
        let inner_clone = self.inner.clone();

        let mut unused_agg_trade = Arc::new(tokio::sync::Mutex::new(vec![]));
        let mut unused_buffer = unused_agg_trade.clone();
        // timer task
        let notify = Arc::new(tokio::sync::Notify::new());
        let notify_clone = notify.clone();
        let mut cursor = 0; // this is the global cursor that records the current id position
        let mut cursor = 0; // this is the global cursor that records the current id position
        tokio::spawn(async move {
            let mut missing_segments = vec![];
            let mut nr_missing = 0;

            loop {
                let agg_trade = if let Some(agg_trade) = unused_agg_trade.lock().await.pop() {
                    agg_trade
                } else {
                    let mut ticket = inner_clone.lock().await;
                    let agg_trade = ticket.input[0].recv().await;
                    drop(ticket);
                    if agg_trade.is_none() {
                        break;
                    }

                    let agg_trade: AggTradeData = agg_trade.unwrap();
                    agg_trade
                };
                let this_data = data.load();
                let mut copied_data = this_data;
                // count number of trades
                copied_data.number_of_trades += 1;
                copied_data.close_price = agg_trade.price;
                copied_data.symbol = std::mem::take(&mut copied_data.symbol);
                copied_data.close_time = agg_trade.trade_time;
                // update the cursor
                if cursor == 0 {
                    cursor = agg_trade.aggregated_trade_id;
                } else {
                    if agg_trade.aggregated_trade_id - cursor > 1 {
                        if let Ok(missed) =
                            pipeline_utils::Segment::try_new(cursor, agg_trade.aggregated_trade_id)
                        {
                            missing_segments.push(missed.clone());
                            nr_missing += missed.size();
                        }
                    }
                    cursor = cursor.max(agg_trade.aggregated_trade_id);
                }

                copied_data.update_from_agg_trade(agg_trade);
                // write back to the shared data structure
                data.store(copied_data);
                // if the lock is acquired, notify the other task
                if atomic_lock
                    .compare_exchange_weak(true, false, Ordering::Relaxed, Ordering::Relaxed)
                    .is_ok()
                {
                    notify.notify_one();
                    sent_to_downstream.notified().await;
                }
            }
        });
        // timer thread
        tokio::spawn(async move {
            let interval_duration = this_time_gap.to_std().unwrap();

            // log the instant by human readable format

            loop {
                let (next_time_point, next_timestamp) =
                    next_interval_time_point(interval_duration).await;
                // info!("Current time is: {:?}", pipeline_utils::get_current_time());
                tokio::time::sleep_until(next_time_point).await;
                if should_quit_clone.load(Ordering::Relaxed) {
                    break;
                }
                // allow 15ms/ 100 more runs or the fetched data's stamp is greater than the next time point
                let counter = 200;
                let duration = tokio::time::Duration::from_millis(100);
                let (tx, mut rx) = tokio::sync::mpsc::channel(100);
                let inner_clone_v2 = inner_clone_v2.clone();
                let buffered_unused = unused_buffer.clone();
                let updater_task = tokio::spawn(async move {
                    for _ in 1..=counter {
                        let mut ticket = inner_clone_v2.lock().await;
                        let agg_trade = ticket.input[0].recv().await;
                        drop(ticket);
                        if agg_trade.is_none() {
                            break;
                        }

                        let agg_trade: AggTradeData = agg_trade.unwrap();
                        if agg_trade.trade_time > next_timestamp {
                            buffered_unused.lock().await.push(agg_trade);
                            break;
                        } else {
                            tx.send(agg_trade).await.expect("Failed to send data");
                        }
                    }
                });
                // this guratnees that the agg_trade is in current time scope
                tokio::select! {
                    _ = updater_task => {
                        let mut data = data_clone_v2.load();
                        while let Some(agg_trade) = rx.recv().await {
                            data.update_from_agg_trade(agg_trade);
                        }
                    }
                    _ = tokio::time::sleep(duration) => {
                        let mut data = data_clone_v2.load();
                        while let Some(agg_trade) = rx.recv().await {
                            data.update_from_agg_trade(agg_trade);
                        }
                    }
                }

                // these are the final steps
                while atomic_lock_timer
                    .compare_exchange_weak(false, true, Ordering::Acquire, Ordering::Relaxed)
                    .is_err()
                {}
                sent_to_downstream_clone.notify_one();
            }
        });

        let tx = self.output[0].clone();
        // sender thread
        let name = self.name.clone();
        tokio::spawn(async move {
            loop {
                notify_clone.notified().await;
                let this_data = data_clone.load();
                data_clone.store(DataSlice::default());
                if tx
                    .send(this_data.to_bar_data_with_symbol(name.clone()))
                    .await
                    .is_err()
                {
                    should_quit.store(true, Ordering::Relaxed);
                    break;
                }
            }
        });
        Ok(())
    }
}

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
        let mut tasks = self
            .transformers
            .iter()
            .map(|x| x.transform())
            .collect::<Vec<_>>();
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
