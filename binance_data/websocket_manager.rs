use std::env::var;
use std::fmt::{self, Debug, Formatter};

use anyhow::Error;
use binance_async::{
    rest::usdm::StartUserDataStreamRequest, websocket::usdm::WebsocketMessage, Binance,
    BinanceWebsocket,
};
use futures::StreamExt;



pub enum BinanceWebsocketOption{
    AggTrage(String),
    bookTicker(String),
    NULL,
}

pub struct BinanceWebsocketManager {
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
    pub async fn new() -> Self {
        // this will panic if the environment variable is not set
        let binance = Binance::with_key(&var("BINANCE_KEY").expect("api key not set"));
        let listen_key = binance.request(StartUserDataStreamRequest {}).await.unwrap();
        BinanceWebsocketManager {
            binance,
            listen_key: listen_key.listen_key,
        }
    }

    pub async fn subscribe(&self, option: BinanceWebsocketOption) -> Result<tokio::sync::mpsc::Receiver<WebsocketMessage>, Error> {
        let mut ws = match option {
            BinanceWebsocketOption::AggTrage(symbol) => {
                let mut ws = BinanceWebsocket::new(&[self.listen_key.as_str(), &format!("{}@aggTrade", symbol)]).await?;
                Ok(ws)
            }
            BinanceWebsocketOption::bookTicker(symbol) => {
                let ws = BinanceWebsocket::new(&[self.listen_key.as_str(), &format!("{}@bookTicker", symbol)]).await?;
                Ok(ws)
            }
            _ => {
                Err(Error::msg("Unsupported option!!"))
            }
        }?;

        let (tx, rx) = tokio::sync::mpsc::channel(4);
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
    




            
