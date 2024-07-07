mod binance_data_manager;
mod metrics;
mod pipelines;

use std::fs::File;
use std::io::BufReader;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use binance::ws_model::WebsocketEvent;
use metrics_server::start_server as start_metrics_server;
use pipelines::pipelines::trade::trade_server::{Trade, TradeServer};
use pipelines::pipelines::trade::{
    AggTradeData, BarData, GetAggTradeRequest, GetAggTradeResponse, GetHeartbeatRequest,
    GetHeartbeatResponse, GetMarketDataRequest, GetMarketDataResponse, RegisterSymbolRequest,
    RegisterSymbolResponse,
};
// Import the Decimal type

use tokio::sync::{mpsc, Mutex};
use tokio_stream::{wrappers::ReceiverStream, Stream, StreamExt};
use tonic::{transport::Server, Request, Response, Status};
use tracing_subscriber::layer::SubscriberExt;

use crate::binance_data_manager::binance_data_manager::BinanceDataManager;
use crate::pipelines::pipelines::{ChannelData, DataSlice, ResamplingTransformer, Transformer};

fn parse_f64_or_default(input: &str) -> f64 {
    input.parse::<f64>().unwrap_or(0.0)
}

#[derive(Debug, Clone)]
pub struct TradeService {
    pub config: Arc<Mutex<binance_data_manager::binance_data_manager::BinanceServerConfig>>,
    pub binance_mgr: Arc<Mutex<BinanceDataManager>>,
    // TODO(yuanhan): add a cache layer to control the connection number
    pub connections:
        dashmap::DashMap<String, tokio::sync::broadcast::Sender<GetMarketDataResponse>>,
}

fn fmt_key(symbol: &str, typ: &str, duration: &str) -> String {
    format!("{}-{}-{}", symbol, typ, duration)
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
        let mut ws = binance_mgr_ticket
            .subscribe(
                binance_data_manager::binance_data_manager::BinanceWebsocketOption::AggTrade(
                    requested_symbol,
                ),
            )
            .await
            .unwrap();
        drop(binance_mgr_ticket);
        tokio::spawn(async move {
            while let Ok(msg) = ws.recv().await {
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
                        let response = GetAggTradeResponse { data: Some(inner) };
                        if let Err(e) = tx.send(Ok(response)).await {
                            tracing::warn!("channel closed: {}", e);
                        }
                    }
                    _ => {
                        tracing::warn!(
                            "mismatched type[type != aggTrade], please add it to handle it"
                        );
                    }
                };
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    type GetClientHeartbeatStream =
        Pin<Box<dyn Stream<Item = Result<GetHeartbeatResponse, Status>> + Send + 'static>>;
    async fn get_client_heartbeat(
        &self,
        request: Request<tonic::Streaming<GetHeartbeatRequest>>,
    ) -> Result<Response<Self::GetClientHeartbeatStream>, Status> {
        let mut stream = request.into_inner();
        // metrics::MISSING_VALUES_COUNT.inc();

        let output = async_stream::try_stream! {
            while let Some(request) = stream.next().await {
                let request = request?;
                let response = GetHeartbeatResponse {
                    metrics : metrics::metrics::collect_metrics(),
                };
                yield response;
            }
        };
        Ok(Response::new(
            Box::pin(output) as Self::GetClientHeartbeatStream
        ))
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
        let (tx, rx) = mpsc::channel(100);

        let config_clone = this_config.clone();
        let request = request.into_inner();
        let requested_symbol = request.symbol;
        tracing::info!("Requested symbol: {}", requested_symbol);
        let requested_granularity = request.granularity.unwrap_or(
            // return 15 seconds bar as default
            pipelines::pipelines::trade::TimeDuration {
                unit: pipelines::pipelines::trade::TimeUnit::Seconds.into(),
                value: 15,
            },
        );
        let granularity_unit = requested_granularity.value;
        let granularity =
            match pipelines::pipelines::trade::TimeUnit::from_i32(requested_granularity.unit)
                .expect("unrecognized time type")
            {
                pipelines::pipelines::trade::TimeUnit::Milliseconds => {
                    chrono::Duration::milliseconds(granularity_unit)
                }
                pipelines::pipelines::trade::TimeUnit::Seconds => {
                    chrono::Duration::seconds(granularity_unit)
                }
                pipelines::pipelines::trade::TimeUnit::Minutes => {
                    chrono::Duration::minutes(granularity_unit)
                }
            };
        tracing::info!(
            "requesting {} with time interval of {:?}",
            requested_symbol,
            granularity
        );
        let key = fmt_key(&requested_symbol, "aggTrade", &granularity.to_string());
        let binance_mgr_ptr = self.binance_mgr.clone();
        let mut output = self.connections.entry(key).or_insert_with(|| {
           // this pipe passes data from the websocket to the resampling transformer
            let (broadcast_tx, _) = tokio::sync::broadcast::channel(this_config.default_buffer_size);
            let broadcast_tx_clone = broadcast_tx.clone();
            tokio::spawn(async move{
                let (resample_tx, resample_rx) = mpsc::channel(55);
            // this pipe passes data from the compressor transformer to the grpc server's response
            let (convert_tx, mut convert_rx) = mpsc::channel(55);
            let resample_trans = ResamplingTransformer::new(vec![resample_rx], vec![convert_tx], granularity);
            // start transforming
            tokio::spawn(async move{
                let _ = resample_trans.transform().await;
            });

            let binance_mgr = binance_mgr_ptr.lock().await;
            let mut ws = binance_mgr.subscribe(binance_data_manager::binance_data_manager::BinanceWebsocketOption::AggTrade(requested_symbol))
                 .await
                  .unwrap();
            drop(binance_mgr);
            // TODO(yuanhan): spawn a new task to handle disaster recovery, close it upon termination
            // this background task pulls the info from the websocket and sends it to the resampling transformer
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
                            //    tracing::warn!("channel closed: {}", e);
                         }
                     },
                        _ => {
                          tracing::warn!("mismatched type[type != aggTrade], please add it to handle it");
                        }
                 };
                }
            });
        // this task pulls the data from transformer and sends it to the grpc server
        tokio::spawn(async move {
            let mut system_time = std::time::SystemTime::now();
            loop {
                let data: DataSlice = convert_rx.recv().await
                                                    .expect("Failed to receive compressed data")
                                                    .into();
                let data = data.to_bar_data();
                let  now_time = std::time::SystemTime::now();
                let duration_since_epoch = now_time.duration_since(system_time).expect("Time went backwards").as_millis() as u64;
                system_time = now_time;
                let response = GetMarketDataResponse {
                    data: Some(data),
                };
                // tx.send(Ok(response)).await.map_err(|e| Status::internal(e.to_string()));
                let _ = broadcast_tx.send(response);
            }
        });
            });
        broadcast_tx_clone
        })
        .value()
        .subscribe();
        tokio::task::spawn(async move {
            while let Ok(msg) = output.recv().await {
                if let Err(e) = tx.send(Ok(msg)).await {
                    // tracing::warn!("channel closed: {}", e);
                }
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn register_symbol(
        &self,
        request: Request<RegisterSymbolRequest>,
    ) -> Result<Response<RegisterSymbolResponse>, Status> {
        let this_config = self.config.clone();
        let config_ticket = this_config.lock().await;
        drop(config_ticket);
        let mut ticket = self.binance_mgr.lock().await;
        let mut binance_mgr = (*ticket).clone();
        drop(ticket);
        let requested_symbols = request.into_inner().symbols;
        binance_mgr
            .register_many(
                requested_symbols,
                binance_data_manager::binance_data_manager::BinanceWebsocketOption::AggTrade(
                    "".to_string(),
                ),
            )
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(RegisterSymbolResponse {}))
    }
}

async fn start_server(service_inner: TradeService, port: usize) -> anyhow::Result<()> {
    let addr = format!("0.0.0.0:{}", port).parse().unwrap();
    let svc = TradeServer::new(service_inner);
    tracing::info!("ready to accept request from {:?}", addr);
    Server::builder()
        .tcp_keepalive(Some(std::time::Duration::from_secs(10)))
        .http2_keepalive_interval(Some(std::time::Duration::from_secs(10)))
        .timeout(Duration::from_secs(6))
        .add_service(svc)
        .serve(addr)
        .await
        .map_err(|e| anyhow::anyhow!(e))
}

fn main() {
    let subscriber =
        tracing_subscriber::FmtSubscriber::new().with(tracing_subscriber::fmt::layer().pretty());
    // use that subscriber to process traces emitted after this point
    tracing::subscriber::set_global_default(subscriber).expect("logger cannot be set");
    let working_dir = std::env::current_dir()
        .expect("Failed to get current directory")
        .join("config")
        .join("config.yaml");
    let working_dir = working_dir
        .to_str()
        .expect("Failed to get current directory");

    let file = File::open(working_dir).expect("Unable to open config file");
    let reader = BufReader::new(file);

    let config: crate::binance_data_manager::binance_data_manager::BinanceServerConfig =
        serde_yaml::from_reader(reader).expect("Unable to parse YAML");
    // let max_threads = config.max_threads.clone();
    let config_clone = config.clone();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(config.max_threads) // 设置 worker 线程数量，例如：4
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async move {
        let port = config_clone.port;
        let metrics_port = config_clone.metrics_server_port;
        let market = TradeService {
            config: Arc::new(Mutex::new(config.clone())),
            binance_mgr: Arc::new(Mutex::new(BinanceDataManager::new(config).await)),
            connections: dashmap::DashMap::new(),
        };
        let server_fut = tokio::spawn(async move {
            let _ = start_server(market, port).await;
        });
        let metrics_server_fut = tokio::spawn(async move {
            let _ = start_metrics_server(metrics_port).await;
        });
        tokio::select! {
                _ = tokio::signal::ctrl_c() => {
                    tracing::warn!("Ctrl+C received. Aborting tasks.");
                    server_fut.abort();
                    metrics_server_fut.abort();
            }
        }
    });
}
