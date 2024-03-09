use std::pin::Pin;
use std::sync::Arc;


use binance_async::websocket::usdm::WebsocketMessage::AggregateTrade;
use tokio::sync::{mpsc, Mutex};
use tonic::{transport::Server, Request, Response, Status};
use tokio_stream::{wrappers::ReceiverStream, Stream};

use trade::{GetAggTradeRequest, GetAggTradeResponse};
use trade::trade_server::{Trade, TradeServer};
use crate::Trade as my_trade; 
use rust_decimal::{Decimal, prelude::ToPrimitive}; // Import the Decimal type

mod websocket_manager;

pub mod trade {
    tonic::include_proto!("trade");
}


#[derive(Debug)]
pub struct TradeService {
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
    let addr = "[::1]:10000".parse().unwrap();

    let market = TradeService {
        binance_mgr: Arc::new(Mutex::new(websocket_manager::BinanceWebsocketManager::new().await)),
    };

    let svc = TradeServer::new(market);

    Server::builder().add_service(svc).serve(addr).await?;

    Ok(())
}