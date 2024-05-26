
mod websocket_manager;
mod metrics;
mod pipelines;


use std::sync::Arc;
use std::fs::File;
use std::pin::Pin;
use std::io::BufReader;
use std::time::Duration;


use tokio::sync::{mpsc, Mutex};
use tonic::{transport::Server, Request, Response, Status};
use tokio_stream::{wrappers::ReceiverStream, Stream, StreamExt};
use pipelines::pipelines::trade::{GetAggTradeRequest, AggTradeData, GetAggTradeResponse,GetHeartbeatRequest, GetHeartbeatResponse, GetMarketDataRequest, GetMarketDataResponse, BarData};
use pipelines::pipelines::trade::trade_server::{Trade, TradeServer};
use rust_decimal::{prelude::ToPrimitive}; // Import the Decimal type
use metrics_server::start_server as start_metrics_server;
use serde_yaml;
use binance::ws_model::WebsocketEvent;

use crate::websocket_manager::websocket_manager::BinanceWebsocketManager;
use crate::pipelines::pipelines::{TimeUnit, ChannelData, Transformer, CompressionTransformer, ResamplingTransformer};

fn parse_f64_or_default(input: &str) -> f64 {
    input.parse::<f64>().unwrap_or(0.0)
}

#[derive(Debug, Clone)]
pub struct TradeService {
    pub config: Arc<Mutex<websocket_manager::websocket_manager::BinanceServerConfig>>,
    pub binance_mgr : Arc<Mutex<BinanceWebsocketManager>>,
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
        let binance_mgr_ticket = self.binance_mgr.lock().await;
        let mut ws = binance_mgr_ticket.subscribe(websocket_manager::websocket_manager::BinanceWebsocketOption::AggTrade(requested_symbol))
                .await
                .unwrap();
        drop(binance_mgr_ticket);
        tokio::spawn(async move {
            while let Some(msg) = ws.next().await {
                match msg {
                    WebsocketEvent::AggTrade(msg) => {
                        let inner = AggTradeData {
                            symbol: msg.symbol,
                            price: parse_f64_or_default(&msg.price),
                            quantity: parse_f64_or_default(&msg.qty),
                            trade_time: msg.event_time,
                            event_type: "aggTrade".to_string(),
                            is_buyer_maker: msg.is_buyer_maker,
                            first_break_trade_id: msg.first_break_trade_id,
                            last_break_trade_id: msg.last_break_trade_id,
                            aggregated_trade_id: msg.aggregated_trade_id,
                        };
                        let response = GetAggTradeResponse {
                            data: Some(inner),
                        };
                        if let Err(e) =  tx.send(Ok(response)).await {
                            tracing::warn!("channel closed: {}", e);
                        }
                    },
                    _ => {
                        tracing::warn!("mismatched type[type != aggTrade], please add it to handle it");
                    }
                }; 
                
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
        // metrics::MISSING_VALUES_COUNT.inc();
        
        let output = async_stream::try_stream!{
            while let Some(request) = stream.next().await {
                let request = request?;
                let response = GetHeartbeatResponse {
                    metrics : metrics::metrics::collect_metrics(),
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
        tracing::info!("request came in");
        let this_config = self.config.clone();
        let config_ticket = this_config.lock().await;
        let this_config = config_ticket.clone();
        drop(config_ticket);
        let (tx, rx) = mpsc::channel(this_config.default_buffer_size);
        
        let config_clone = this_config.clone();
        let request = request.into_inner();
        let requested_symbol = request.symbol;
        tracing::info!("Requested symbol: {}", requested_symbol);
        // this pipe passes data from the websocket to the resampling transformer
        let (resample_tx, resample_rx) = mpsc::channel(this_config.default_buffer_size);
        // this pipe passes data from the resampling transformer to the compressor transformer
        let (compress_tx, compress_rx) = mpsc::channel(this_config.default_buffer_size);
        // this pipe passes data from the compressor transformer to the grpc server's response
        let (convert_tx, mut convert_rx) = mpsc::channel(this_config.default_buffer_size);
        let resample_trans = ResamplingTransformer::new(vec![resample_rx], vec![compress_tx], TimeUnit::Second(1));
        let compressor_trans = CompressionTransformer::new(vec![compress_rx], vec![convert_tx]);
        let _ = tokio::spawn(async move {
            let _ = resample_trans.transform().await;
        });
        let _ = tokio::spawn(async move {
            let _ = compressor_trans.transform().await;
        });

        let binance_mgr = BinanceWebsocketManager::new(config_clone).await;
        let mut ws = binance_mgr.subscribe(websocket_manager::websocket_manager::BinanceWebsocketOption::AggTrade(requested_symbol))
                .await
                .unwrap();

        tokio::spawn(async move {
            while let Ok(msg) = ws.recv().await {
                match msg {
                    WebsocketEvent::AggTrade(msg) => {
                        let agg_trade = AggTradeData {
                            symbol: msg.symbol,
                            price: parse_f64_or_default(&msg.price),
                            quantity: parse_f64_or_default(&msg.qty),
                            trade_time: msg.event_time,
                            event_type: "aggTrade".to_string(),
                            is_buyer_maker: msg.is_buyer_maker,
                            first_break_trade_id: msg.first_break_trade_id,
                            last_break_trade_id: msg.last_break_trade_id,
                            aggregated_trade_id: msg.aggregated_trade_id,
                        };
                        if let Err(e) =  resample_tx.send(ChannelData::new(agg_trade)).await {
                            tracing::warn!("channel closed: {}", e);
                        }
                    },
                    _ => {
                        tracing::warn!("mismatched type[type != aggTrade], please add it to handle it");
                    }
                }; 
                
            }
        });
        tokio::spawn(async move {
            loop {
                let data: BarData = convert_rx.recv().await
                                                    .expect("Failed to receive compressed data")
                                                    .into();
                let response = GetMarketDataResponse {
                    data: Some(data),
                };
                // tx.send(Ok(response)).await.map_err(|e| Status::internal(e.to_string()));
                if let Err(e) = tx.send(Ok(response)).await {
                    tracing::warn!("Error: {}", e);
                    break;
                }
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

async fn start_server(service_inner: TradeService, port: usize) -> anyhow::Result<()>{
    let addr = format!("0.0.0.0:{}", port).parse().unwrap();
    let svc = TradeServer::new(service_inner);
    tracing::info!("ready to accept request from {:?}", addr);
    Server::builder()
        .tcp_keepalive(Some(std::time::Duration::from_secs(10)))
        .http2_keepalive_interval(Some(std::time::Duration::from_secs(10)))
        .timeout(Duration::from_secs(6))
        .add_service(svc)
        .serve(addr)
        .await.map_err(|e| anyhow::anyhow!(e))
}


#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let subscriber = tracing_subscriber::FmtSubscriber::new();
    // use that subscriber to process traces emitted after this point
    tracing::subscriber::set_global_default(subscriber)?;
    let working_dir = std::env::current_dir()
                                    .expect("Failed to get current directory")
                                    .join("config")
                                    .join("config.yaml");
    let working_dir = working_dir.to_str().expect("Failed to get current directory");

    let file = File::open(working_dir)
                        .expect("Unable to open config file");
    
    let reader = BufReader::new(file);
    
    let config :crate::websocket_manager::websocket_manager::BinanceServerConfig = serde_yaml::from_reader(reader).expect("Unable to parse YAML");
    // let max_threads = config.max_threads.clone();
    let config_clone = config.clone();
    let port = config_clone.port;
    let metrics_port = config_clone.metrics_server_port;
    let market = TradeService {
        config: Arc::new(Mutex::new(config.clone())),
        binance_mgr: Arc::new(Mutex::new(BinanceWebsocketManager::new(config).await)),
    };
    let server_fut = tokio::spawn(async move{
        let _ = start_server(market, port).await; 
    });
    let metrics_server_fut = tokio::spawn(async move{
        let _ = start_metrics_server(metrics_port).await;
    });
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            tracing::warn!("Ctrl+C received. Aborting tasks.");
            server_fut.abort();
            metrics_server_fut.abort();
        }
    }
    
    Ok(())
}


