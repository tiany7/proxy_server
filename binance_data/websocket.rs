use std::env::var;

use anyhow::Error;
use binance_async::{
    rest::usdm::StartUserDataStreamRequest, websocket::usdm::WebsocketMessage, Binance,
    BinanceWebsocket,
};
use fehler::throws;
use futures::StreamExt;

mod websocket_manager;

#[throws(Error)]
#[tokio::main]
async fn main() {
    env_logger::init();

    let binance = websocket_manager::BinanceWebsocketManager::new()
                    .await;
    let mut rx = binance.subscribe(websocket_manager::BinanceWebsocketOption::AggTrage("btcusdt".to_string()))
                    .await?;
    while let Some(msg) = rx.recv().await {
        println!("{:?}", msg);
    }
}
