


use std::fmt::{self, Debug, Formatter};

use anyhow::Error;
use binance_async::{
    rest::usdm::StartUserDataStreamRequest, websocket::usdm::WebsocketMessage, Binance,
    BinanceWebsocket,
};
use futures::StreamExt;



fn default_port() -> usize {
    10000
}

fn default_api_key() -> String {
    "not set".to_string()
}

fn default_buffer_size() -> usize {
    10
}

#[derive(Debug, serde::Deserialize, Clone)]
pub struct BinanceServerConfig {
    pub port: usize,
    pub api_key: String,
    pub default_buffer_size: usize,
}

impl Default for BinanceServerConfig {
    fn default() -> BinanceServerConfig {
        BinanceServerConfig {
            port: default_port(),
            api_key: default_api_key(),
            default_buffer_size: default_buffer_size(),
        }
    }
}


#[allow(dead_code)]
pub enum BinanceWebsocketOption{
    AggTrade(String),
    BookTicker(String),
    Null,
}
#[allow(dead_code)]
pub struct BinanceWebsocketManager {
    config: BinanceServerConfig,
    binance: Binance,
    listen_key: String,
    // ws: BinanceWebsocket<WebsocketMessage>,
}

impl Debug for BinanceWebsocketManager {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("BinanceWebsocketManager")
            .field("listen_key", &self.listen_key)
            .finish()
    }
}


impl BinanceWebsocketManager {
    pub async fn new(config: BinanceServerConfig) -> Self {
        // this will panic if the environment variable is not set
        let binance = Binance::with_key(&config.api_key);
        let listen_key = binance.request(StartUserDataStreamRequest {}).await.unwrap();
        BinanceWebsocketManager {
            config,
            binance,
            listen_key: listen_key.listen_key,
        }
    }

    pub async fn subscribe(&self, option: BinanceWebsocketOption) -> Result<tokio::sync::mpsc::Receiver<WebsocketMessage>, Error> {
        let mut ws = match option {
            BinanceWebsocketOption::AggTrade(symbol) => {
                let ws = BinanceWebsocket::new(&[self.listen_key.as_str(), &format!("{}@aggTrade", symbol)]).await.expect("error establishing");
                Ok(ws)
            }
            BinanceWebsocketOption::BookTicker(symbol) => {
                let ws = BinanceWebsocket::new(&[self.listen_key.as_str(), &format!("{}@bookTicker", symbol)]).await?;
                Ok(ws)
            }
            _ => {
                Err(Error::msg("Unsupported option!!"))
            }
        }?;

        let (tx, rx) = tokio::sync::mpsc::channel(self.config.default_buffer_size);
        tokio::spawn(async move {
            while let Some(msg) = ws.next().await {
                match msg {
                    Ok(msg) => {
                        tx.send(msg).await.unwrap();
                    }
                    Err(e) => {
                        println!("channel closed: {}", e);
                        break;
                    }
                }
            }
        });

        Ok(rx)
    }

}
    




            
