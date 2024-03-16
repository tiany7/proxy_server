

use anyhow::Error;

use fehler::throws;

use websocket_manager::BinanceServerConfig;

mod websocket_manager;

#[throws(Error)]
#[tokio::main]
async fn main() {
    env_logger::init();

    let binance = websocket_manager::BinanceWebsocketManager::new(BinanceServerConfig::default())
                    .await;
    let mut rx = binance.subscribe(websocket_manager::BinanceWebsocketOption::AggTrade("btcusdt".to_string()))
                    .await?;
    while let Some(msg) = rx.recv().await {
        println!("{:?}", msg);
    }
}
