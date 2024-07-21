use anyhow::Result;
use pyo3::prelude::*;
use tokio::sync::mpsc::Receiver;
use tokio_stream::StreamExt;
use std::collections::HashMap;

use super::trade as trade_pb;
use crate::TradeClient;

#[pyclass]
#[derive(Default)]
pub struct PyBarData {
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
}

#[pyclass]
pub struct StreamReader {
    stub : Receiver<trade_pb::BarData>,
}

#[pymethods]
impl StreamReader {
    fn next(&mut self, py: Python) -> PyResult<PyObject> {
        pyo3_asyncio::tokio::future_into_py(py, async move {
            match self.stub.recv().await {
                Some(data) => Ok(PyBarData{
                    low_price: data.low,
                    high_price: data.high,
                    open_price: data.open,
                    close_price: data.close,
                    volume: data.volume,
                    quote_asset_volume: data.quote_asset_volume,
                    number_of_trades: data.number_of_trades,
                    taker_buy_base_asset_volume: data.taker_buy_base_asset_volume,
                    taker_buy_quote_asset_volume: data.taker_buy_quote_asset_volume,
                    min_id: data.min_id,
                    max_id: data.max_id,
                    missing_count: data.missing_count,
                    open_time: data.open_time,
                    close_time: data.close_time,
                }),
                None => Ok(PyBarData::default()),
            }
        }).map(|py_any| py_any.to_object(py))
    }
}

pub async fn dispatch_readers(symbols: Vec<String>, mut rx : tonic::Streaming<trade_pb::GetMarketDataResponse>) -> HashMap<String, StreamReader> {
    let mut proxies = HashMap::new();
    let mut readers = HashMap::new();
    let mut receivers = symbols.iter().map(|symbol| {
        let (tx, rx) = tokio::sync::mpsc::channel(10);
        proxies.insert(symbol.clone(), tx); 
        (symbol.clone(), rx)
    }).collect::<HashMap<_, _>>();

    tokio::spawn(async move{
        while let Some(Ok(data)) = rx.next().await {
            let data = data.data.unwrap();
            if let Some(tx) = proxies.get_mut(&data.symbol) {
                tx.send(data).await.expect("Failed to send data");
            }
        }
    });
    
    for symbol in symbols {
        let (symbol_tx, mut symbol_rx) = tokio::sync::mpsc::channel(10);
        let mut rx = receivers.remove(&symbol).unwrap();
        tokio::spawn(async move{
            while let Some(data) = rx.recv().await {
                symbol_tx.send(data).await.expect("Failed to send data");
            }
        });
        readers.insert(symbol, StreamReader{stub: symbol_rx});
    }
    readers
}