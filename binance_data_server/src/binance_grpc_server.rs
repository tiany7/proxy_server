
mod websocket_manager;
pub mod trade {
    include!("../../proto/generated_code/trade.rs");
}
use std::sync::Arc;
use std::fs::File;
use std::pin::Pin;
use std::io::BufReader;
use std::time::Duration;


use binance_async::websocket::usdm::WebsocketMessage::AggregateTrade;
use pipelines::Transformer;
use tokio::sync::{mpsc, Mutex};
use tonic::{transport::Server, Request, Response, Status};
use tokio_stream::{wrappers::ReceiverStream, Stream, StreamExt};
use trade::{GetAggTradeRequest, AggTradeData, GetAggTradeResponse,GetHeartbeatRequest, GetHeartbeatResponse, GetMarketDataRequest, GetMarketDataResponse};
use trade::trade_server::{Trade, TradeServer};
use rust_decimal::{prelude::ToPrimitive}; // Import the Decimal type
use serde_yaml;
use async_stream::try_stream;
use arrow::record_batch::RecordBatch;





#[derive(Debug, Clone)]
pub struct TradeService {
    pub config: Arc<Mutex<websocket_manager::BinanceServerConfig>>,
}



#[tonic::async_trait]
impl Trade for TradeService {
    type GetAggTradeStreamStream = ReceiverStream<Result<GetAggTradeResponse, Status>>;

    async fn get_agg_trade_stream(
        &self,
        request: Request<GetAggTradeRequest>,
    ) -> Result<Response<Self::GetAggTradeStreamStream>, Status> {
        let this_config = self.config.clone();
        let config_ticket = this_config.lock().await;
        let this_config = config_ticket.clone();
        drop(config_ticket);
        let (tx, rx) = mpsc::channel(this_config.default_buffer_size);
        
        let requested_symbol = request.into_inner().symbol;
        tokio::spawn(async move {
            
            let binance_mgr = websocket_manager::BinanceWebsocketManager::new(this_config.clone()).await;
            let mut ws = binance_mgr.subscribe(websocket_manager::BinanceWebsocketOption::AggTrade(requested_symbol))
                .await
                .unwrap();
            
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
        let this_config = self.config.clone();
        let config_ticket = this_config.lock().await;
        let this_config = config_ticket.clone();
        drop(config_ticket);
        let (tx, rx) = mpsc::channel(this_config.default_buffer_size);
        
        let request = request.into_inner();
        let requested_symbol = request.symbol;
        // this pipe passes data from the websocket to the resampling transformer
        let (resample_tx, mut resample_rx) = mpsc::channel(this_config.default_buffer_size);
        // this pipe passes data from the resampling transformer to the compressor transformer
        let (compress_tx, mut compress_rx) = mpsc::channel(this_config.default_buffer_size);
        // this pipe passes data from the compressor transformer to the grpc server's response
        let (convert_tx, mut convert_rx) = mpsc::channel(this_config.default_buffer_size);
        let mut resample_trans = pipelines::ResamplingTransformer::new(vec![resample_rx], vec![compress_tx], chrono::Duration::try_seconds(1).expect("Failed to create duration"));
        let mut compressor_trans = pipelines::CompressionTransformer::new(vec![compress_rx], vec![convert_tx]);
        let _ = tokio::spawn(async move {
            resample_trans.transform().await;
        });
        let _ = tokio::spawn(async move {
            compressor_trans.transform().await;
        });

        
        let binance_mgr = websocket_manager::BinanceWebsocketManager::new(this_config.clone()).await;
        let mut ws = binance_mgr.subscribe(websocket_manager::BinanceWebsocketOption::AggTrade(requested_symbol))
                .await
                .unwrap();
        tokio::spawn(async move {
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
                resample_tx.send(pipelines::ChannelData::new(agg_trade)).await.unwrap();
                let compressed_data: Vec<u8> = convert_rx.recv().await
                                                    .expect("Failed to receive compressed data")
                                                    .into();
                let response = GetMarketDataResponse {
                    data: compressed_data,
                };
                tx.send(Ok(response)).await.map_err(|e| Status::internal(e.to_string())).unwrap();
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

async fn start_server(service_inner: TradeService, port: usize) {
    let addr = format!("0.0.0.0:{}", port).parse().unwrap();
    let svc = TradeServer::new(service_inner);
    Server::builder()
        .tcp_keepalive(Some(std::time::Duration::from_secs(10)))
        .http2_keepalive_interval(Some(std::time::Duration::from_secs(10)))
        .timeout(Duration::from_secs(6))
        .add_service(svc)
        .serve(addr)
        .await
        .expect("Failed to start server");
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
    // let max_threads = config.max_threads.clone();
    let port = config.port.clone();
    let market = TradeService {
        config: Arc::new(Mutex::new(config)),
    };


    // new multi-threaded runtime
    // tokio::runtime::Builder::new_multi_thread()
    //     .worker_threads(max_threads)
    //     .enable_all()
    //     .build()
    //     .unwrap()
    //     .block_on(start_server(addr, market));
    start_server(market, port).await;

    Ok(())
}


