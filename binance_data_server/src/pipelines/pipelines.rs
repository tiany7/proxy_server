pub mod trade {
    include!("../../../proto/generated_code/trade.rs");
}

use std::any::Any;

use std::io::Write;
use std::sync::Arc;
use std::vec::Vec;

use anyhow::{Ok, Result};

use arrow::record_batch::RecordBatch;
use async_trait::async_trait;

use futures::SinkExt;
use tokio::sync::{mpsc, Mutex};
use tracing::info;

use metrics_server::MISSING_VALUES_COUNT;

// 定义一个动态数据类型
type DynData = Box<dyn Any + Send + Sync>;

// 定义一个数据结构，包装了动态数据
pub struct ChannelData(DynData);

use trade::AggTradeData;
use trade::BarData;
use trade::Column;

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

impl From<ChannelData> for SampleData {
    fn from(val: ChannelData) -> Self {
        *val.0.downcast::<SampleData>().unwrap()
    }
}

impl From<ChannelData> for RecordBatch {
    fn from(val: ChannelData) -> Self {
        *val.0.downcast::<RecordBatch>().unwrap()
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
    output: Vec<mpsc::Sender<ChannelData>>,
    granularity: chrono::Duration,
}

pub struct CompressionTransformerInner {
    // in milliseconds, this is used to control the granularity of the data
    input: Vec<mpsc::Receiver<ChannelData>>,
    output: Vec<mpsc::Sender<ChannelData>>,
}

pub struct ResamplingTransformer {
    inner: Arc<Mutex<ResamplingTransformerInner>>,
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
                output,
                granularity,
            })),
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

    fn record_batch_to_bytes(record_batch: &RecordBatch) -> Result<Vec<u8>, anyhow::Error> {
        let mut buffer = std::io::Cursor::new(Vec::new());
        // 创建IPC文件写入器
        let mut writer =
            arrow::ipc::writer::FileWriter::try_new(&mut buffer, &record_batch.schema())?;
        writer.write(record_batch)?;
        writer
            .finish()
            .map_err(|e| anyhow::anyhow!("finish error: {}", e))?;
        drop(writer);
        let buffer = buffer.into_inner();
        Ok(buffer)
    }
}

#[async_trait]
impl Transformer for ResamplingTransformer {
    async fn transform(&self) -> Result<()> {
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

            // check granularity
            if open_time > 0 && agg_trade.trade_time - open_time >= this_time_gap {
                close_price = agg_trade.price;
                close_time = agg_trade.trade_time;
                let bar_data = BarData {
                    open: open_price,
                    high: high_price,
                    low: low_price,
                    close: close_price,
                    volume,
                    quote_asset_volume,
                    number_of_trades,
                    taker_buy_base_asset_volume,
                    taker_buy_quote_asset_volume,
                    min_id,
                    max_id,
                    missing_count,
                    open_time,
                    close_time,
                };
                let ticket = self.inner.lock().await;
                ticket.output[0].send(ChannelData::new(bar_data)).await?;
                if missing_count > 0 {
                    MISSING_VALUES_COUNT.inc_by(missing_count);
                }
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
            // count number of trades
            number_of_trades += 1;

            // update the min_id and max_id
            if last.is_some() {
                missing_count += agg_trade.aggregated_trade_id - last.unwrap() - 1;
            }

            last = Some(agg_trade.aggregated_trade_id);

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
        }

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
    #[test]
    async fn test_pipeline_uncompress() {
        let mut agg_trade = AggTradeData {
            symbol: "BTCUSDT".to_string(),
            aggregated_trade_id: 1,
            price: 114514.0,
            quantity: 1.0,
            trade_time: 1000,
            is_buyer_maker: false,
            event_type: "trade".to_string(),
            first_break_trade_id: 0,
            last_break_trade_id: 0,
        };
        let mut agg_trade2 = AggTradeData {
            symbol: "BTCUSDT".to_string(),
            aggregated_trade_id: 2,
            price: 1919810.0,
            quantity: 1.0,
            trade_time: 114514,
            is_buyer_maker: false,
            event_type: "trade".to_string(),
            first_break_trade_id: 0,
            last_break_trade_id: 0,
        };

        let (tx, rx) = create_channel(10);
        let (tx2, mut rx2) = create_channel(10);
        let resample_trans = ResamplingTransformer::new(
            vec![rx],
            vec![tx2],
            chrono::Duration::try_seconds(1).expect("Failed to create duration"),
        );

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

        let open = data
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(open.value(0), 114514.0);
        info!("{:?}", open.value(0));
    }
}
