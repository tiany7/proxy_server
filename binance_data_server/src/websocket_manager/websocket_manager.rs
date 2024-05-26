


use std::fmt::{self, Debug, Formatter};
use std::sync::atomic::AtomicBool;

use anyhow::Error;
use binance::ws_model::{TradesEvent, TradeEvent, WebsocketEvent};
use binance::websockets::{WebSockets, agg_trade_stream, trade_stream};


fn default_port() -> usize {
    10000
}

fn default_api_key() -> String {
    "not set".to_string()
}

fn default_buffer_size() -> usize {
    10
}

fn default_max_threads() -> usize {
    4
}   

fn default_metrics_server_port() -> usize {
    8080
}

#[derive(Debug, serde::Deserialize, Clone)]
pub struct BinanceServerConfig {
    pub port: usize,
    pub api_key: String,
    pub default_buffer_size: usize,
    pub max_threads: usize,
    pub metrics_server_port: usize,
}

impl Default for BinanceServerConfig {
    fn default() -> BinanceServerConfig {
        BinanceServerConfig {
            port: default_port(),
            api_key: default_api_key(),
            default_buffer_size: default_buffer_size(),
            max_threads: default_max_threads(),
            metrics_server_port: default_metrics_server_port(),
        }
    }
}


#[allow(dead_code)]
#[derive(Clone)]
pub enum BinanceWebsocketOption{
    AggTrade(String),
    Trade(String),
    Null,
}
#[allow(dead_code)]
pub struct BinanceWebsocketManager {
    config: BinanceServerConfig,
    // this will record the connection by kind of the coin
    // use map's default function as indicator that of the connection is already created
    connections: dashmap::DashMap<String, async_broadcast::Receiver<WebsocketEvent>>,
    // ws: BinanceWebsocket<WebsocketMessage>,
}

impl Debug for BinanceWebsocketManager {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("BinanceWebsocketManager")
            .field("listen_key", &self.config)
            .finish()
    }
}

impl BinanceWebsocketManager {
    pub async fn new(config: BinanceServerConfig) -> Self {
        BinanceWebsocketManager {
            config,
            connections: dashmap::DashMap::new(),
        }
    }

    pub async fn subscribe(&self, option: BinanceWebsocketOption) -> Result<async_broadcast::Receiver<WebsocketEvent>, Error> {
        let key = match option.clone() {
            BinanceWebsocketOption::AggTrade(symbol) => format!("{}@aggTrade", symbol),
            BinanceWebsocketOption::Trade(symbol) => format!("{}@trade", symbol),
            BinanceWebsocketOption::Null => {
                return Err(anyhow::anyhow!("Invalid option"));
            }
        };
        let buffer_size = self.config.default_buffer_size;
        let rx = self.connections
            .entry(key.clone())
            .or_insert_with(|| {
                let (tx, mut rx) = async_broadcast::broadcast(buffer_size);
                
                let _ = tokio::spawn(async move {
                    let keep_running = AtomicBool::new(true);
                    let listen_key = match option {
                        BinanceWebsocketOption::AggTrade(symbol) => agg_trade_stream(symbol.as_str()),
                        BinanceWebsocketOption::Trade(symbol) => trade_stream(symbol.as_str()),
                        _ => "".to_string(), // this will not be touched anyway
                    };
                    let (task_tx, mut task_rx) = crossbeam_channel::unbounded();
                    let mut ws = WebSockets::new(move |event| {
                        // TODO(yuanhan): this is very hacky, we might need another solution to solve this
                        task_tx.send(event).map_err(|e| anyhow::anyhow!("Failed to send event: {:?}", e)).unwrap();
                        Ok(())
                    });
                    let _ = tokio::spawn(async move {
                        while let Ok(event) = task_rx.recv() {
                            tx.broadcast(event).await.expect("Failed to send event");
                        }
                    });
                    ws.connect(&listen_key).await.expect("Failed to connect");
                    if let Err(e) = ws.event_loop(&keep_running).await {
                        tracing::warn!("the websocket connection is closed: {:?}", e);
                    }
                    ws.disconnect().await.expect("Failed to connect");
                });
                rx
            })
            .value()
            .clone();
    

        Ok(rx)
    }

}
    




            
