use std::collections::HashMap;
use std::fmt::{self, Debug, Formatter};
use std::sync::{atomic::AtomicBool, Arc};

use anyhow::Error;
use binance::api::Binance;
use binance::config::{Config, DATA_REST_ENDPOINT};
use binance::market::Market;
use binance::rest_model::AggTrade;
use binance::websockets::{agg_trade_stream, trade_stream, WebSockets};
use binance::ws_model::WebsocketEvent;
use metrics_server::MISSING_VALUE_BY_CHANNEL;
use tokio::{sync::Mutex, task::JoinHandle};

use crate::AggTradeData;

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

fn default_recovery_batch_size() -> u16 {
    100
}

fn default_restart_interval() -> u64 {
    60
}

#[derive(Debug, serde::Deserialize, Clone)]
pub struct BinanceServerConfig {
    pub port: usize,
    pub api_key: String,
    pub default_buffer_size: usize,
    pub max_threads: usize,
    pub metrics_server_port: usize,
    pub recover_batch_size: u16,
    pub restart_interval: u64,
}

impl Default for BinanceServerConfig {
    fn default() -> BinanceServerConfig {
        BinanceServerConfig {
            port: default_port(),
            api_key: default_api_key(),
            default_buffer_size: default_buffer_size(),
            max_threads: default_max_threads(),
            metrics_server_port: default_metrics_server_port(),
            recover_batch_size: default_recovery_batch_size(),
            restart_interval: default_restart_interval(),
        }
    }
}

#[allow(dead_code)]
#[derive(Clone)]
pub enum BinanceWebsocketOption {
    AggTrade(String),
    Trade(String),
    Null,
}

type ConnectionHandles =
    Arc<tokio::sync::Mutex<Vec<HashMap<String, tokio::sync::broadcast::Sender<AggTradeData>>>>>;
type BinanceConnections = Arc<tokio::sync::Mutex<Vec<JoinHandle<()>>>>;
#[allow(dead_code)]
#[derive(Clone)]
pub struct BinanceDataManager {
    config: BinanceServerConfig,
    // this will do multiplexing, each coin will only have one connection
    connections: dashmap::DashMap<String, tokio::sync::broadcast::Sender<WebsocketEvent>>,
    // market data client
    market_client: Market,
    batch_connections: ConnectionHandles,
    binance_connections: BinanceConnections,
}

impl Debug for BinanceDataManager {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("BinanceDataManager")
            .field("listen_key", &self.config)
            .finish()
    }
}

impl BinanceDataManager {
    pub fn new(config: BinanceServerConfig) -> Self {
        let conf = Config::default().set_rest_api_endpoint(DATA_REST_ENDPOINT);
        let market_client = Binance::new_with_env(&conf);
        BinanceDataManager {
            config,
            connections: dashmap::DashMap::new(),
            market_client,
            batch_connections: Arc::new(tokio::sync::Mutex::new(vec![])), // this is to store the batch connections
            binance_connections: Arc::new(tokio::sync::Mutex::new(vec![])), // this is to store the binance connections
        }
    }

    pub fn new_thread_safe(config: BinanceServerConfig) -> Arc<Mutex<Self>> {
        let restart_interval = config.restart_interval;
        let binance_data_manager = Arc::new(Mutex::new(BinanceDataManager::new(config)));
        let binance_data_manager_clone = binance_data_manager.clone();
        tokio::spawn(async move {
            // use tokio interval to restart the connection
            let d = tokio::time::Duration::from_secs(restart_interval * 60);
            loop {
                tokio::time::sleep(d).await;
                tracing::info!("graceful restart");
                binance_data_manager.lock().await.graceful_restart().await;
            }
        });
        binance_data_manager_clone
    }

    pub fn get_shared_market_client(&self) -> Market {
        self.market_client.clone()
    }

    // will register the symbol in the dict so that we can use it later
    // precondition: the symbol is not registered, which is guaranteed by the filters
    // why not result? cuz I don't see any errors here!
    fn register_symbol_stream(&self, key: &str) -> tokio::sync::mpsc::Sender<WebsocketEvent> {
        let (tx, mut rx) = tokio::sync::mpsc::channel(100);
        let buffer_size = self.config.default_buffer_size;
        let _ = self
            .connections
            .entry(key.to_string())
            .or_insert_with(|| {
                tracing::warn!("registered symbol {}", key);
                let (broadcast_tx, _) = tokio::sync::broadcast::channel(buffer_size);
                let broadcast_tx_clone: tokio::sync::broadcast::Sender<WebsocketEvent> =
                    broadcast_tx.clone();
                tokio::spawn(async move {
                    while let Some(event) = rx.recv().await {
                        if let Err(_e) = broadcast_tx_clone.send(event) {
                            MISSING_VALUE_BY_CHANNEL.inc();
                        }
                    }
                    tracing::warn!("the web connection is closed");
                });
                broadcast_tx
            })
            .value()
            .subscribe();

        tx
    }
    pub async fn subscribe(
        &self,
        option: BinanceWebsocketOption,
    ) -> Result<tokio::sync::broadcast::Receiver<WebsocketEvent>, Error> {
        let key = match option.clone() {
            BinanceWebsocketOption::AggTrade(symbol) => format!("{}@aggTrade", symbol),
            BinanceWebsocketOption::Trade(symbol) => format!("{}@trade", symbol),
            BinanceWebsocketOption::Null => {
                return Err(anyhow::anyhow!("Invalid option"));
            }
        };
        let buffer_size = self.config.default_buffer_size;
        let rx = self
            .connections
            .entry(key.clone())
            .or_insert_with(|| {
                let (tx, _) = tokio::sync::broadcast::channel(buffer_size);
                let tx_clone = tx.clone();
                tokio::spawn(async move {
                    let keep_running = AtomicBool::new(true);
                    let listen_key = match option {
                        BinanceWebsocketOption::AggTrade(symbol) => {
                            agg_trade_stream(symbol.as_str())
                        }
                        BinanceWebsocketOption::Trade(symbol) => trade_stream(symbol.as_str()),
                        _ => "".to_string(), // this will not be touched anyway
                    };
                    let mut ws = WebSockets::new(|event: WebsocketEvent| {
                        if let Err(_e) = tx.send(event) {
                            MISSING_VALUE_BY_CHANNEL.inc();
                        }
                        Ok(())
                    });
                    ws.connect(&listen_key).await.expect("Failed to connect");
                    if let Err(e) = ws.event_loop(&keep_running).await {
                        tracing::warn!("the websocket connection is closed: {:?}", e);
                    }
                    ws.disconnect().await.expect("Failed to connect");
                });
                tx_clone
            })
            .value()
            .subscribe();

        Ok(rx)
    }

    // this function is to register multiple symbols
    // since we are using a single task to handle multiple symbols
    // we can do three things here
    // 1. establish a stream of multiple symbols
    // 2. create a background task as a proxy to dispatch the data to the correct channel
    // 3. do the same thing as above, but in a single task
    pub async fn register_many(
        &mut self,
        symbols: Vec<String>,
        option: BinanceWebsocketOption,
    ) -> Result<(), Error> {
        let endpoints = symbols
            .iter()
            .map(|symbol| agg_trade_stream(symbol).to_string())
            .collect::<Vec<String>>();
        // from single source to multiple
        // since map has already been allocated the memory, we can forget about using dashmap
        let sender_by_symbol: HashMap<String, tokio::sync::mpsc::Sender<WebsocketEvent>> =
            endpoints
                .iter()
                .filter(|key| !self.connections.contains_key(*key))
                .map(|key| {
                    let tx = self.register_symbol_stream(key);

                    (key.clone(), tx)
                })
                .collect();
        // here to delete

        tokio::spawn(async move {
            let keep_running = AtomicBool::new(true);
            let mut ws = WebSockets::new(
                |event: binance::ws_model::CombinedStreamEvent<
                    binance::ws_model::WebsocketEventUntag,
                >| {
                    match event.data {
                        binance::ws_model::WebsocketEventUntag::WebsocketEvent(event) => {
                            match event.clone() {
                                binance::ws_model::WebsocketEvent::AggTrade(msg) => {
                                    if let Some(tx) = sender_by_symbol.get(
                                        agg_trade_stream(msg.symbol.to_lowercase().as_str())
                                            .as_str(),
                                    ) {
                                        if let Err(_e) = tx.try_send(event) {
                                            MISSING_VALUE_BY_CHANNEL.inc();
                                        }
                                    } else {
                                        tracing::error!(
                                            "the symbol {} is not registered",
                                            msg.symbol
                                        );
                                    }
                                }
                                WebsocketEvent::Trade(msg) => {
                                    if let Some(ref tx) = sender_by_symbol
                                        .get(format!("{}@trade", msg.symbol).as_str())
                                    {
                                    } else {
                                        tracing::error!(
                                            "the symbol {} is not registered",
                                            msg.symbol
                                        );
                                    }
                                }
                                _ => {}
                            }
                        }
                        _ => unimplemented!(),
                    }

                    Ok(())
                },
            );
            ws.connect_multiple(endpoints)
                .await
                .expect("Failed to connect");

            if let Err(e) = ws.event_loop(&keep_running).await {
                tracing::warn!("the websocket connection is closed: {:?}", e);
            }
            ws.disconnect().await.expect("Failed to connect");
        });

        Ok(())
    }

    pub async fn subcribe_many(
        &mut self,
        symbols: Vec<String>,
    ) -> Result<HashMap<String, tokio::sync::broadcast::Receiver<AggTradeData>>, Error> {
        let endpoints = symbols
            .iter()
            .map(|symbol| agg_trade_stream(symbol).to_string())
            .collect::<Vec<String>>();
        // from single source to multiple
        // since map has already been allocated the memory, we can forget about using dashmap
        let sender_by_symbol: HashMap<String, tokio::sync::broadcast::Sender<AggTradeData>> =
            symbols
                .iter()
                .filter(|key| !self.connections.contains_key(*key))
                .map(|key| {
                    let (tx, _) = tokio::sync::broadcast::channel(2000);

                    (key.clone(), tx)
                })
                .collect();
        // register the batch connections
        let sender_list = sender_by_symbol.clone();
        let mut batch_connections = self.batch_connections.lock().await;
        batch_connections.push(sender_list);
        drop(batch_connections);

        let receiver_by_symbol: HashMap<String, tokio::sync::broadcast::Receiver<AggTradeData>> =
            sender_by_symbol
                .iter()
                .map(|(key, tx)| {
                    let rx = tx.subscribe();
                    (key.clone(), rx)
                })
                .collect();

        let handle = tokio::spawn(async move {
            let keep_running = AtomicBool::new(true);
            let mut ws = WebSockets::new(
                |event: binance::ws_model::CombinedStreamEvent<
                    binance::ws_model::WebsocketEventUntag,
                >| {
                    match event.data {
                        binance::ws_model::WebsocketEventUntag::WebsocketEvent(event) => {
                            match event.clone() {
                                binance::ws_model::WebsocketEvent::AggTrade(msg) => {
                                    if let Some(tx) =
                                        sender_by_symbol.get(msg.symbol.to_lowercase().as_str())
                                    {
                                        let event = AggTradeData {
                                            symbol: msg.symbol,
                                            price: crate::parse_f64_or_default(&msg.price),
                                            quantity: crate::parse_f64_or_default(&msg.qty),
                                            trade_time: msg.trade_order_time,
                                            event_type: "aggTrade".to_string(),
                                            is_buyer_maker: msg.is_buyer_maker,
                                            first_break_trade_id: msg.first_break_trade_id,
                                            last_break_trade_id: msg.last_break_trade_id,
                                            aggregated_trade_id: msg.aggregated_trade_id,
                                        };
                                        if let Err(_e) = tx.send(event) {
                                            MISSING_VALUE_BY_CHANNEL.inc();
                                        }
                                    } else {
                                        tracing::error!(
                                            "the symbol {} is not registered",
                                            msg.symbol
                                        );
                                    }
                                }
                                binance::ws_model::WebsocketEvent::Trade(msg) => {
                                    unimplemented!();
                                }
                                _ => {}
                            }
                        }
                        _ => unimplemented!(),
                    }

                    Ok(())
                },
            );
            ws.connect_multiple(endpoints)
                .await
                .expect("Failed to connect");

            if let Err(e) = ws.event_loop(&keep_running).await {
                tracing::warn!("the websocket connection is closed: {:?}", e);
            }
            ws.disconnect().await.expect("Failed to connect");
        });

        self.binance_connections.lock().await.push(handle);

        Ok(receiver_by_symbol)
    }

    pub async fn graceful_restart(&self) {
        // 1.kill old connection

        // 3.replace the old connection with the new connection
        let mut join_handles = self.binance_connections.lock().await;

        let deprecated_handles = (*join_handles).drain(..);
        let mut join_handles = self.binance_connections.lock().await;

        // 2.spawn new connection
        let mut batch_connections = self.batch_connections.lock().await;
        while let Some(sender_map) = batch_connections.pop() {
            let handle = tokio::spawn(async move {
                let keep_running = AtomicBool::new(true);
                let mut ws = WebSockets::new(
                    |event: binance::ws_model::CombinedStreamEvent<
                        binance::ws_model::WebsocketEventUntag,
                    >| {
                        match event.data {
                            binance::ws_model::WebsocketEventUntag::WebsocketEvent(event) => {
                                match event.clone() {
                                    binance::ws_model::WebsocketEvent::AggTrade(msg) => {
                                        if let Some(tx) =
                                            sender_map.get(msg.symbol.to_lowercase().as_str())
                                        {
                                            let event = AggTradeData {
                                                symbol: msg.symbol,
                                                price: crate::parse_f64_or_default(&msg.price),
                                                quantity: crate::parse_f64_or_default(&msg.qty),
                                                trade_time: msg.trade_order_time,
                                                event_type: "aggTrade".to_string(),
                                                is_buyer_maker: msg.is_buyer_maker,
                                                first_break_trade_id: msg.first_break_trade_id,
                                                last_break_trade_id: msg.last_break_trade_id,
                                                aggregated_trade_id: msg.aggregated_trade_id,
                                            };
                                            if let Err(_e) = tx.send(event) {
                                                MISSING_VALUE_BY_CHANNEL.inc();
                                            }
                                        } else {
                                            tracing::error!(
                                                "the symbol {} is not registered",
                                                msg.symbol
                                            );
                                        }
                                    }
                                    binance::ws_model::WebsocketEvent::Trade(msg) => {
                                        unimplemented!();
                                    }
                                    _ => {}
                                }
                            }
                            _ => unimplemented!(),
                        }

                        Ok(())
                    },
                );
                ws.connect_multiple(sender_map.keys().cloned().collect())
                    .await
                    .expect("Failed to connect");

                if let Err(e) = ws.event_loop(&keep_running).await {
                    tracing::warn!("the websocket connection is closed: {:?}", e);
                }
                ws.disconnect().await.expect("Failed to connect");
            });
            join_handles.push(handle);
        }
        deprecated_handles.for_each(|handle| {
            handle.abort();
        });
    }

    pub async fn get_most_recent_agg_trades(
        &mut self,
        symbol: &str,
        batch_size: u16,
    ) -> Result<Vec<AggTrade>, Error> {
        let trades = self
            .market_client
            .get_agg_trades(symbol, None, None, None, batch_size)
            .await?;
        Ok(trades)
    }
}
