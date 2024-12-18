pub mod trade {
    include!("../../../proto/generated_code/trade.rs");
}

use std::any::Any;
use std::ops::Add;
use std::result::Result::Ok;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
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
use trade::Column;
use trade::{BarData, BarDataWithLogExtra};

use super::utils as pipeline_utils;
use pipeline_utils::{
    get_next_instant_and_timestamp, instant_from_timestamp, next_interval, this_period_start,
    AtomicLock, CleanupTask, AggTradeType,
};

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
    missing_agg_trade_start_id: u64,
    missing_agg_trade_end_id: u64,
    first_agg_trade_id: u64,
    last_agg_trade_id: u64,
    taker_buy_big_quote_asset_volume : f64,
    taker_buy_mid_quote_asset_volume : f64,
    taker_buy_sml_quote_asset_volume : f64,
    taker_buy_tny_quote_asset_volume : f64,

    taker_sell_big_quote_asset_volume : f64,
    taker_sell_mid_quote_asset_volume : f64,
    taker_sell_sml_quote_asset_volume : f64,
    taker_sell_tny_quote_asset_volume : f64,
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
            min_id: u64::MAX,
            max_id: u64::MIN,
            missing_count: 0,
            open_time: 0,
            close_time: 0,
            last: None,
            missing_agg_trade_start_id: 0,
            missing_agg_trade_end_id: 0,
            symbol: "",
            first_agg_trade_id: 0,
            last_agg_trade_id: 0,
            taker_buy_big_quote_asset_volume : 0.0,
            taker_buy_mid_quote_asset_volume : 0.0,
            taker_buy_sml_quote_asset_volume : 0.0,
            taker_buy_tny_quote_asset_volume : 0.0,

            taker_sell_big_quote_asset_volume : 0.0,
            taker_sell_mid_quote_asset_volume : 0.0,
            taker_sell_sml_quote_asset_volume : 0.0,
            taker_sell_tny_quote_asset_volume : 0.0,
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
        self.min_id = u64::MAX;
        self.max_id = u64::MIN;
        self.missing_agg_trade_end_id = 0;
        self.missing_agg_trade_start_id = 0;
        self.missing_count = 0;
        self.open_time = 0;
        self.close_time = 0;
        self.last = None;
        self.taker_buy_big_quote_asset_volume = 0.0;
        self.taker_buy_mid_quote_asset_volume =  0.0;
        self.taker_buy_sml_quote_asset_volume  = 0.0;
        self.taker_buy_tny_quote_asset_volume = 0.0;

        self.taker_sell_big_quote_asset_volume = 0.0;
        self.taker_sell_mid_quote_asset_volume = 0.0;
        self.taker_sell_sml_quote_asset_volume = 0.0;
        self.taker_sell_tny_quote_asset_volume = 0.0;
    }

    pub fn update_from_agg_trade(&mut self, agg_trade: AggTradeData) {
        if self.first_agg_trade_id == 0 {
            self.first_agg_trade_id = agg_trade.aggregated_trade_id;
        }
        self.last_agg_trade_id = self.last_agg_trade_id.max(agg_trade.aggregated_trade_id);
        if self.number_of_trades == 0 {
            // self.open_time = agg_trade.trade_time;
            self.open_price = agg_trade.price;
        }
        if agg_trade.first_break_trade_id < self.min_id {
            self.min_id = agg_trade.first_break_trade_id;
            self.open_price = agg_trade.price;
        }

        if agg_trade.last_break_trade_id > self.max_id {
            self.max_id = agg_trade.last_break_trade_id;
            self.close_price = agg_trade.price;
        }
        self.number_of_trades += agg_trade.last_break_trade_id - agg_trade.first_break_trade_id + 1;

        // self.close_time = agg_trade.trade_time;

        if self.last.is_none() {
            self.last = Some(agg_trade.aggregated_trade_id);
        }
        if self.last.unwrap() < agg_trade.aggregated_trade_id {
            self.last = Some(agg_trade.aggregated_trade_id);
        }
        self.low_price = self.low_price.min(agg_trade.price);
        self.high_price = self.high_price.max(agg_trade.price);
        self.volume += agg_trade.quantity;
        let quote_volume = agg_trade.price * agg_trade.quantity;
        self.quote_asset_volume += quote_volume;
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
        
        match crate::pipelines::utils::get_agg_trade_type(quote_volume) {
            AggTradeType::BIG => {
                if !agg_trade.is_buyer_maker {
                    self.taker_buy_big_quote_asset_volume += quote_volume;
                } else {
                    self.taker_sell_big_quote_asset_volume += quote_volume;
                }
            },
            AggTradeType::MIDDLE => {
                if !agg_trade.is_buyer_maker {
                    self.taker_buy_mid_quote_asset_volume += quote_volume;
                } else {
                    self.taker_sell_mid_quote_asset_volume += quote_volume;
                }
            },
            AggTradeType::SMALL => {
                if !agg_trade.is_buyer_maker {
                    self.taker_buy_sml_quote_asset_volume += quote_volume;
                } else {
                    self.taker_sell_sml_quote_asset_volume += quote_volume;
                }
            },
            _ => {
                if !agg_trade.is_buyer_maker {
                    self.taker_buy_tny_quote_asset_volume += quote_volume;
                } else {
                    self.taker_sell_tny_quote_asset_volume += quote_volume;
                }
            }
        }
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
            missing_agg_trade_start_id: self.missing_agg_trade_start_id,
            missing_agg_trade_end_id: self.missing_agg_trade_start_id,
            first_agg_trade_id: self.first_agg_trade_id,
            last_agg_trade_id: self.last_agg_trade_id,
            taker_buy_big_quote_asset_volume : self.taker_buy_big_quote_asset_volume,
            taker_buy_mid_quote_asset_volume : self.taker_buy_mid_quote_asset_volume,
            taker_buy_sml_quote_asset_volume : self.taker_buy_sml_quote_asset_volume,
            taker_buy_tny_quote_asset_volume : self.taker_buy_tny_quote_asset_volume,

            taker_sell_big_quote_asset_volume : self.taker_sell_big_quote_asset_volume,
            taker_sell_mid_quote_asset_volume : self.taker_sell_mid_quote_asset_volume,
            taker_sell_sml_quote_asset_volume : self.taker_sell_sml_quote_asset_volume,
            taker_sell_tny_quote_asset_volume : self.taker_sell_tny_quote_asset_volume,
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
            missing_agg_trade_start_id: self.missing_agg_trade_start_id,
            missing_agg_trade_end_id: self.missing_agg_trade_end_id,
            first_agg_trade_id: self.first_agg_trade_id,
            last_agg_trade_id: self.last_agg_trade_id,
            taker_buy_big_quote_asset_volume : self.taker_buy_big_quote_asset_volume,
            taker_buy_mid_quote_asset_volume : self.taker_buy_mid_quote_asset_volume,
            taker_buy_sml_quote_asset_volume : self.taker_buy_sml_quote_asset_volume,
            taker_buy_tny_quote_asset_volume : self.taker_buy_tny_quote_asset_volume,

            taker_sell_big_quote_asset_volume : self.taker_sell_big_quote_asset_volume,
            taker_sell_mid_quote_asset_volume : self.taker_sell_mid_quote_asset_volume,
            taker_sell_sml_quote_asset_volume : self.taker_sell_sml_quote_asset_volume,
            taker_sell_tny_quote_asset_volume : self.taker_sell_sml_quote_asset_volume,
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
    output: Vec<mpsc::Sender<BarDataWithLogExtra>>,
    name: String,
    market_client: binance::market::Market,
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
        output: Vec<mpsc::Sender<BarDataWithLogExtra>>,
        granularity: chrono::Duration,
        name: String,
        client: binance::market::Market,
    ) -> Self {
        ResamplingTransformerWithTiming {
            inner: Arc::new(Mutex::new(ResamplingTransformerInnerV2 {
                input,
                granularity,
            })),
            output,
            name,
            market_client: client,
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
        let time_gap_in_seconds = this_time_gap.num_seconds();
        drop(this);
        // create a shared data structure to allow routinely update
        let atomic_cursor = Arc::new(AtomicU64::new(0));
        let atomic_cursor_clone = atomic_cursor.clone();
        let data = Arc::new(AtomicCell::new(DataSlice::default()));
        let inner_clone = self.inner.clone();
        let inner_clone_v2 = inner_clone.clone();
        let data_clone = data.clone();
        let data_clone_v2 = data_clone.clone();
        let atomic_lock = Arc::new(AtomicBool::new(false));
        let atomic_lock_timer = atomic_lock.clone();
        let should_quit = Arc::new(AtomicBool::new(false));
        let should_quit_clone = should_quit.clone();
        let inner_clone = self.inner.clone();

        let should_init_sender_thread = Arc::new(AtomicLock::new());
        let should_init_sender_thread_clone = should_init_sender_thread.clone();

        let mut unused_agg_trade = Arc::new(tokio::sync::Mutex::<Vec<AggTradeData>>::new(vec![]));
        let mut unused_buffer = unused_agg_trade.clone();
        // timer task
        let notify = Arc::new(tokio::sync::Notify::new());
        let notify_clone = notify.clone();
        let lock_notify = Arc::new(tokio::sync::Notify::new());
        let lock_notify_clone = lock_notify.clone();
        let window_left = Arc::new(AtomicU64::new(0));
        let window_right = Arc::new(AtomicU64::new(0));
        let l = window_left.clone();
        let r = window_right.clone();

        // this notify will trigger the timer thread to get out of the sleep
        let stale_notify = Arc::new(tokio::sync::Notify::new());
        let stale_notify_clone = stale_notify.clone();

        let done_notify = Arc::new(tokio::sync::Notify::new());
        let done_notify_clone = done_notify.clone();
        // use the record the missing segments from previous data
        let missing_data = Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new()));
        let missing_data_clone = missing_data.clone();
        let missing_data_clone_v2 = missing_data.clone();
        tokio::spawn(async move {
            // let mut missing_segments = vec![];
            let mut nr_missing = 0;
            let in_range = |x: u64| -> bool {
                let left = l.load(Ordering::SeqCst);
                let right = r.load(Ordering::SeqCst);
                if left == 0 || right == 0 {
                    return true;
                }
                x <= right
            };

            let is_from_previous_interval = |x: u64| -> bool {
                let left = l.load(Ordering::SeqCst);
                let right = r.load(Ordering::SeqCst);
                if left == 0 || right == 0 {
                    return false;
                }
                x < left
            };
            let mut max_latency = 0;
            let default_timeout = tokio::time::Duration::from_secs(1);
            loop {
                let mut is_timeout = false;
                let agg_trade = if let Some(agg_trade) = unused_agg_trade.lock().await.pop() {
                    // tracing::error!("agg id is {:?}", agg_trade.aggregated_trade_id);
                    agg_trade
                } else {
                    let mut ticket = inner_clone.lock().await;
                    let agg_trade = match tokio::time::timeout(default_timeout, ticket.input[0].recv()).await {
                        Ok(t) => t,
                        Err(_) =>{
                            is_timeout = true;
                            None
                        } 
                    };
                    drop(ticket);
                    if is_timeout {
                        if should_init_sender_thread.test() {
                            should_init_sender_thread.unlock();
                            lock_notify.notify_one();
                            while atomic_lock
                                .compare_exchange_weak(true, false, Ordering::Relaxed, Ordering::Relaxed)
                                .is_err()
                            {
                                tokio::task::yield_now().await;
                            }
                            notify.notify_one();
                            done_notify.notified().await;
                        } else {
                            continue;
                        }
                    }
                    if agg_trade.is_none() {
                        break;
                    }
                    // tracing::warn!("agg id is {:?}", agg_trade.as_ref().unwrap().aggregated_trade_id);
                    let agg_trade: AggTradeData = agg_trade.unwrap();
                    agg_trade
                };
                // check for the max latency, if greater than maxx, update maxx and print error log
                // check the cursor and update the cursor
                let cursor = atomic_cursor_clone.load(Ordering::Relaxed);
                let trade_id = agg_trade.aggregated_trade_id;
                if trade_id < cursor {
                    tracing::error!(
                        "[CURSOR ERROR] {:?} < {:?}",
                        agg_trade.aggregated_trade_id,
                        cursor
                    );
                    continue;
                } else {
                    if trade_id > cursor + 1  && cursor != 0{
                        let this_data = data.load();
                        let mut copied_data = this_data;
                        copied_data.missing_agg_trade_start_id = cursor + 1;
                        copied_data.missing_agg_trade_end_id = trade_id; // this means there is some data loss due to a variety of reasons
                        data.store(copied_data);
                    }
                    atomic_cursor_clone.store(agg_trade.aggregated_trade_id, Ordering::Relaxed);
                }

                if is_from_previous_interval(agg_trade.trade_time) {
                    let parent_interval =
                        this_period_start(agg_trade.trade_time, time_gap_in_seconds as u32);
                    // update missing_data
                    (*(missing_data.write().await))
                        .entry(parent_interval)
                        .or_insert(DataSlice::default())
                        .update_from_agg_trade(agg_trade);
                    continue;
                }
                if !in_range(agg_trade.trade_time) {
                    stale_notify.notify_one();

                    lock_notify.notify_one();
                    while atomic_lock
                        .compare_exchange_weak(true, false, Ordering::Relaxed, Ordering::Relaxed)
                        .is_err()
                    {
                        tokio::task::yield_now().await;
                    }
                    notify.notify_one();
                    done_notify.notified().await;
                    should_init_sender_thread.unlock();
                }
                // info!("agg_trade: {:?}", agg_trade);
                let this_data = data.load();
                let mut copied_data = this_data;
                // update the cursor
                // if cursor == 0 {
                //     cursor = agg_trade.aggregated_trade_id;
                // } else {
                //     if agg_trade.aggregated_trade_id < cursor {
                //         panic!("{:?} < {:?}", agg_trade.aggregated_trade_id, cursor);
                //     } else if agg_trade.aggregated_trade_id - cursor > 1 {
                //         if let Ok(missed) =
                //             pipeline_utils::Segment::try_new(cursor, agg_trade.aggregated_trade_id)
                //         {
                //             missing_segments.push(missed.clone());
                //             nr_missing += missed.size();
                //         }
                //     }
                //     // cursor = cursor.max(agg_trade.aggregated_trade_id);
                // }

                copied_data.update_from_agg_trade(agg_trade);
                // write back to the shared data structure
                data.store(copied_data);
                // if the lock is acquired, notify the other task
                // if atomic_lock
                //     .compare_exchange_weak(true, false, Ordering::Relaxed, Ordering::Relaxed)
                //     .is_ok()
                // {
                //     notify.notify_one();
                //     done_notify.notified().await;
                //     tracing::error!("lock released");
                // }
                if should_init_sender_thread.test() {
                    should_init_sender_thread.unlock();
                    lock_notify.notify_one();
                    while atomic_lock
                        .compare_exchange_weak(true, false, Ordering::Relaxed, Ordering::Relaxed)
                        .is_err()
                    {
                        tokio::task::yield_now().await;
                    }
                    notify.notify_one();
                    done_notify.notified().await;
                }
            }
        });

        let d = this_time_gap.to_std().unwrap();
        let interval_duration = d.as_secs();

        tokio::spawn(async move {
            let time_gap_in_seconds = interval_duration;
            loop {
                let current_left = window_left.load(Ordering::SeqCst);
                let current_right = window_right.load(Ordering::SeqCst);
                let next_timestamp = if current_left == 0 {
                    let (_, next_timestamp) = get_next_instant_and_timestamp(interval_duration);
                    window_left.store(
                        next_timestamp - interval_duration * 1000 + 1,
                        Ordering::Relaxed,
                    );
                    window_right.store(next_timestamp, Ordering::SeqCst);
                    next_timestamp
                } else {
                    window_left.store(current_left + interval_duration * 1000, Ordering::SeqCst);
                    window_right.store(current_right + interval_duration * 1000, Ordering::SeqCst);
                    current_right + interval_duration * 1000
                };

                let next_instant = instant_from_timestamp(next_timestamp);
                // info!("the next timestamp is: {:?}", next_timestamp);

                // info!("now timestamp is: {:?}", std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis());
                // info!(
                //     "Next instant is: {:?}, next timestamp is: {:?}",
                //     next_instant, next_timestamp
                // );
                tokio::select! {
                    _ = tokio::time::sleep_until(next_instant) => {
                        stale_notify_clone.notified().await; // consume the stale notify
                    }
                    _ = stale_notify_clone.notified() => {

                    }
                }
                if should_quit_clone.load(Ordering::Relaxed) {
                    break;
                }

                should_init_sender_thread_clone.lock();
                lock_notify_clone.notified().await;
                // allow 15ms/ 100 more runs or the fetched data's stamp is greater than the next time point
                let counter = 500;
                let duration = tokio::time::Duration::from_millis(100);
                let inner_clone_v2 = inner_clone_v2.clone();
                let buffered_unused = unused_buffer.clone();
                let current_bar = data_clone_v2.clone();
                let cancel_notify = Arc::new(AtomicBool::new(false));
                let cancel_notify_clone = cancel_notify.clone(); // these two are used to gracefully cancel the task
                let cancel_done_notify = Arc::new(tokio::sync::Notify::new());
                let cancel_done_notify_clone = cancel_done_notify.clone();
                {
                    let mut temp_data = data_clone_v2.load();
                    temp_data.open_time = window_left.load(Ordering::SeqCst);
                    temp_data.close_time = window_right.load(Ordering::SeqCst);
                    data_clone_v2.store(temp_data);
                }
                let atomic_cursor = atomic_cursor.clone();
                let missing_data = missing_data_clone_v2.clone();
                let updater_task = tokio::spawn(tokio::task::unconstrained(async move {
                    let mut ticket = inner_clone_v2.lock().await;
                    let default_timeout = tokio::time::Duration::from_millis(5);
                    for _ in 1..=counter {
                        if cancel_notify_clone.load(Ordering::Relaxed) {
                            break;
                        }
                        let mut is_timeout = false;
                        let agg_trade = match tokio::time::timeout(default_timeout, ticket.input[0].recv()).await {
                            Ok(t) => t,
                            Err(_) =>{
                                is_timeout = true;
                                None
                            } 
                        };
                        if is_timeout {
                            continue;
                        }
                        if agg_trade.is_none() {
                            break;
                        }

                        let agg_trade: AggTradeData = agg_trade.unwrap();
                        let trade_id = agg_trade.aggregated_trade_id;
                        if agg_trade.trade_time < next_timestamp - interval_duration * 1000 + 1 {
                            let parent_interval =
                                this_period_start(agg_trade.trade_time, time_gap_in_seconds as u32);
                            // update missing_data
                            (*(missing_data.write().await))
                                .entry(parent_interval)
                                .or_insert(DataSlice::default())
                                .update_from_agg_trade(agg_trade);
                            continue;
                        }
                        if agg_trade.trade_time > next_timestamp {
                            buffered_unused.lock().await.push(agg_trade);
                            break;
                        } else {
                            let mut copied_data = current_bar.load();
                            copied_data.update_from_agg_trade(agg_trade);
                            
                            let cursor = atomic_cursor.load(Ordering::Relaxed);
                            if trade_id < cursor {
                                tracing::error!("[CURSOR ERROR] {:?} < {:?}", trade_id, cursor);
                                continue;
                            } else {
                                if trade_id > cursor + 1 && cursor != 0 {
                                    copied_data.missing_agg_trade_start_id = cursor + 1;
                                    copied_data.missing_agg_trade_end_id = trade_id;
                                }
                                atomic_cursor.store(trade_id, Ordering::Relaxed);
                            }
                            current_bar.store(copied_data);
                        }
                    }
                    cancel_done_notify_clone.notify_one();
                }));
                // this guratnees that the agg_trade is in current time scope
                tokio::select! {
                    _ = updater_task => {

                    }
                    _ = tokio::time::sleep(duration) => {
                        cancel_notify.store(true, Ordering::Relaxed);
                        cancel_done_notify.notified().await;
                        let mut temp_data = data_clone_v2.load();
                        data_clone_v2.store(temp_data);
                    }
                }
                // these are the final steps
                while atomic_lock_timer
                    .compare_exchange(false, true, Ordering::Acquire, Ordering::Acquire)
                    .is_err()
                {}
            }
        });

        let tx = self.output[0].clone();
        // sender thread
        let name = self.name.clone();
        let market_client = self.market_client.clone();
        tokio::spawn(async move {
            let mut last_data = DataSlice::default();
            let name_clone = name.clone();
            let mut is_first_bar = true;
            loop {
                notify_clone.notified().await;
                let mut this_data = data_clone.load();
                let mut default_slice = DataSlice::default();
                data_clone.store(default_slice);
                done_notify_clone.notify_one();
                
                if this_data.number_of_trades == 0 {
                    if last_data.number_of_trades != 0 {
                        this_data.open_price = last_data.close_price;
                        this_data.close_price = last_data.close_price;
                        this_data.high_price = last_data.close_price;
                        this_data.low_price = last_data.close_price;
                    } else {
                        this_data.open_price = 99998.0;
                        this_data.close_price = 99998.0;
                        this_data.high_price = 99998.0;
                        this_data.low_price = 99998.0;
                        // let mut last_1000_agg_trade = market_client
                        //     .get_agg_trades(name_clone.clone(), None, None, None, 1000)
                        //     .await
                        //     .unwrap_or(vec![])
                        //     .into_iter()
                        //     .map(|x| AggTradeData {
                        //         symbol: name.clone(),
                        //         price: x.price,
                        //         quantity: x.qty,
                        //         trade_time: x.time,
                        //         event_type: "aggTrade".to_string(),
                        //         is_buyer_maker: x.maker,
                        //         first_break_trade_id: x.first_id,
                        //         last_break_trade_id: x.last_id,
                        //         aggregated_trade_id: x.agg_id,
                        //     })
                        //     .collect::<Vec<AggTradeData>>();
                        // let mut is_first_trade = true;
                        // let mut left_bound = 0;
                        // while let Some(agg_trade) = last_1000_agg_trade.pop() {
                        //     if is_first_trade {
                        //         left_bound = this_period_start(
                        //             agg_trade.trade_time - interval_duration * 1000,
                        //             interval_duration as u32,
                        //         );
                        //         is_first_trade = false;
                        //     }
                        //     if agg_trade.trade_time < left_bound {
                        //         break;
                        //     }
                        //     this_data.update_from_agg_trade(agg_trade);
                        // }
                    }
                }
                let collected_missing_data = missing_data_clone
                    .write()
                    .await
                    .drain()
                    .map(|(k, v)| (k, v.to_bar_data_with_symbol(name.clone())))
                    .collect::<std::collections::HashMap<u64, BarData>>();
                let bar_data = this_data.to_bar_data_with_symbol(name.clone());
                let response_with_log_extra = BarDataWithLogExtra {
                    data: Some(bar_data),
                    missing_data: collected_missing_data,
                };
                if !is_first_bar  {
                    is_first_bar = false;
                    continue;
                }
                if let Err(_) = tx.send(response_with_log_extra).await {
                    should_quit.store(true, Ordering::Relaxed);
                    break;
                }
                last_data = this_data;
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
