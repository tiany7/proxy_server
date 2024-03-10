mod websocket_manager;
mod binance_server_config;

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::fs::File;
use std::io::BufReader;


use binance_async::websocket::usdm::WebsocketMessage::AggregateTrade;
use tokio::sync::{mpsc, Mutex};
use tonic::{transport::Server, Request, Response, Status};
use tokio_stream::{wrappers::ReceiverStream, Stream};
use trade::{GetAggTradeRequest, GetAggTradeResponse};
use trade::trade_server::{Trade, TradeServer};
use rust_decimal::{Decimal, prelude::ToPrimitive}; // Import the Decimal type
use binance_server_config::BinanceServerConfig;
use serde_yaml;

use crate::Trade as my_trade; 


pub mod trade {
    tonic::include_proto!("trade");
}


#[derive(Debug)]
pub struct TradeService {
    config: BinanceServerConfig,
    binance_mgr: Arc<Mutex<websocket_manager::BinanceWebsocketManager>>,
}


#[tonic::async_trait]
impl Trade for TradeService {
    type GetAggTradeStreamStream = ReceiverStream<Result<GetAggTradeResponse, Status>>;

    async fn get_agg_trade_stream(
        &self,
        request: Request<GetAggTradeRequest>,
    ) -> Result<Response<Self::GetAggTradeStreamStream>, Status> {
        let (tx, rx) = mpsc::channel(4);
        
        let binance_mgr_clone = self.binance_mgr.clone();
        let requested_symbol = request.into_inner().symbol;
        tokio::spawn(async move {
            let mut binance_mgr = binance_mgr_clone.lock().await;
            let mut ws = binance_mgr.subscribe(websocket_manager::BinanceWebsocketOption::AggTrage(requested_symbol))
                .await
                .unwrap();
            while let Some(msg) = ws.recv().await {
                match msg {
                    AggregateTrade(msg) => {
                        let response = GetAggTradeResponse {
                            symbol: msg.symbol,
                            price: msg.price.to_f64().unwrap(),
                            quantity: msg.qty.to_f64().unwrap(),
                            trade_time: msg.event_time,
                            event_type: msg.event_type,
                            is_buyer_maker: msg.is_buyer_maker,
                            first_break_trade_id: msg.first_break_trade_id,
                            last_break_trade_id: msg.last_break_trade_id,
                            aggregated_trade_id: msg.aggregated_trade_id,
                        };
                        tx.send(Ok(response)).await.map_err(|e| Status::internal(e.to_string())).unwrap();
                    }
                    _ => {
                        tx.send(Err(Status::invalid_argument("mismatched type[type != aggTrade]")))
                            .await
                            .map_err(|e| Status::internal(e.to_string())).unwrap();
                    }
                    
                }
                
                
            }
        });
        

        Ok(Response::new(ReceiverStream::new(rx)))
    }

}




#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let working_dir = std::env::current_dir()
                                    .expect("Failed to get current directory")
                                    .join("config")
                                    .join("config.yaml");
    let working_dir = working_dir.to_str().expect("Failed to get current directory");

    let file = File::open(working_dir)
                        .expect("Unable to open config file");
    
    let reader = BufReader::new(file);
    
    let config :BinanceServerConfig = serde_yaml::from_reader(reader).expect("Unable to parse YAML");
    let addr = format!("0.0.0.0:{}", 10000).parse().unwrap();

    let market = TradeService {
        config,
        binance_mgr: Arc::new(Mutex::new(websocket_manager::BinanceWebsocketManager::new().await)),
    };

    let svc = TradeServer::new(market);

    Server::builder().add_service(svc).serve(addr).await?;

    Ok(())
}
