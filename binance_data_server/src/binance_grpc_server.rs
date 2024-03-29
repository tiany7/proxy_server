
mod websocket_manager;
use std::sync::Arc;
use std::fs::File;
use std::pin::Pin;
use std::io::BufReader;


use binance_async::websocket::usdm::WebsocketMessage::AggregateTrade;
use pipeline_utils::Transformer;
use tokio::sync::{mpsc, Mutex};
use tonic::{transport::Server, Request, Response, Status};
use tokio_stream::{wrappers::ReceiverStream, Stream, StreamExt};
use trade::{GetAggTradeRequest, AggTradeData, GetAggTradeResponse,GetHeartbeatRequest, GetHeartbeatResponse, GetMarketDataRequest, GetMarketDataResponse};
use trade::trade_server::{Trade, TradeServer};
use rust_decimal::{prelude::ToPrimitive}; // Import the Decimal type
use serde_yaml;
use async_stream::try_stream;
use arrow::record_batch::RecordBatch;






pub mod trade {
    tonic::include_proto!("trade");
}


#[derive(Debug, Clone)]
pub struct TradeService {
    config: websocket_manager::BinanceServerConfig,
    binance_mgr: Arc<Mutex<websocket_manager::BinanceWebsocketManager>>,
}



#[tonic::async_trait]
impl Trade for TradeService {
    type GetAggTradeStreamStream = ReceiverStream<Result<GetAggTradeResponse, Status>>;

    async fn get_agg_trade_stream(
        &self,
        request: Request<GetAggTradeRequest>,
    ) -> Result<Response<Self::GetAggTradeStreamStream>, Status> {
        let (tx, rx) = mpsc::channel(self.config.default_buffer_size);
        
        let binance_mgr_clone = self.binance_mgr.clone();
        let requested_symbol = request.into_inner().symbol;
        tokio::spawn(async move {
            let binance_mgr = binance_mgr_clone.lock().await;
            let mut ws = binance_mgr.subscribe(websocket_manager::BinanceWebsocketOption::AggTrade(requested_symbol))
                .await
                .unwrap();

            // leave this scope to avoid deadlock
            drop(binance_mgr);
            while let Some(msg) = ws.recv().await {
                match msg {
                    AggregateTrade(msg) => {
                        let inner = AggTradeData {
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
                        let response = GetAggTradeResponse {
                            data: Some(inner),
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

    type GetClientHeartbeatStream = Pin<Box<dyn Stream<Item = Result<GetHeartbeatResponse, Status>> + Send  + 'static>>;
    async fn get_client_heartbeat(
        &self,
        request: Request<tonic::Streaming<GetHeartbeatRequest>>,
    ) -> Result<Response<Self::GetClientHeartbeatStream>, Status> {
        let mut stream = request.into_inner();
        let output = async_stream::try_stream!{
            while let Some(request) = stream.next().await {
                let request = request?;
                let response = GetHeartbeatResponse {
                    pong: format!("Received: {}", request.ping),
                };
                yield response;
            }
        };
        Ok(Response::new(Box::pin(output)
        as Self::GetClientHeartbeatStream))
    }


    type GetMarketDataStream = ReceiverStream<Result<GetMarketDataResponse, Status>>;

    async fn get_market_data(
        &self,
        request: Request<GetMarketDataRequest>,
    ) -> Result<Response<Self::GetMarketDataStream>, Status> {
        let (tx, rx) = mpsc::channel(self.config.default_buffer_size);
        
        let binance_mgr_clone = self.binance_mgr.clone();
        let requested_symbol = request.into_inner().symbol;
        // let (agg_tx, agg_rx) = mpsc::channel(self.config.default_buffer_size);
        let (resample_tx, resample_rx) = mpsc::channel(self.config.default_buffer_size);
        let (convert_tx, convert_rx) = mpsc::channel(self.config.default_buffer_size);
        let mut trans = pipeline_utils::ResamplingTransformer::new(vec![resample_rx], vec![convert_tx], chrono::Duration::seconds(1));
        let _ = trans.transform();
        tokio::spawn(async move {
            let binance_mgr = binance_mgr_clone.lock().await;
            let mut ws = binance_mgr.subscribe(websocket_manager::BinanceWebsocketOption::AggTrade(requested_symbol))
                .await
                .unwrap();
            // leave this scope to avoid deadlock
            drop(binance_mgr);
            while let Some(AggregateTrade(msg)) = ws.recv().await {
                let agg_trade = AggTradeData {
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
                resample_tx.send(pipeline_utils::ChannelData::new(agg_trade)).await.unwrap();
                let resampled_data = convert_rx.recv().await.unwrap();
                let resampled_data: RecordBatch = resampled_data.into();
                
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
    
    let config :crate::websocket_manager::BinanceServerConfig = serde_yaml::from_reader(reader).expect("Unable to parse YAML");
    let addr = format!("0.0.0.0:{}", config.port).parse().unwrap();

    let market = TradeService {
        config: config.clone(),
        binance_mgr: Arc::new(Mutex::new(websocket_manager::BinanceWebsocketManager::new(config).await)),
    };

    let svc = TradeServer::new(market);

    Server::builder().add_service(svc).serve(addr).await?;

    Ok(())
}
