use std::pin::Pin;
use std::sync::Arc;


use tokio::sync::mpsc;
use tonic::{transport::Server, Request, Response, Status};
use tokio_stream::{wrappers::ReceiverStream, Stream};

use market::{FakeMarketDataRequest, FakeMarketDataResponse};
use market::market_server::{Market, MarketServer};
use crate::market as my_market; // Add the missing import for the `market` module
use chrono::Utc; // Add the missing import for the `Utc` struct from the `chrono` crate

pub mod market {
    tonic::include_proto!("market");
}


#[derive(Debug)]
pub struct MarketService {
    
}


#[tonic::async_trait]
impl Market for MarketService {
    type GetFakeMarketDataStream = ReceiverStream<Result<FakeMarketDataResponse, Status>>;

    async fn get_fake_market_data(
        &self,
        _request: Request<FakeMarketDataRequest>,
    ) -> Result<Response<Self::GetFakeMarketDataStream>, Status> {
        let (tx, rx) = mpsc::channel(4);

        tokio::spawn(async move {
                        loop {
                            // encode timestamp to vec u8
                            let timestamp = Utc::now() // Replace `chrono::Utc::now()` with `Utc::now()`
                                            .timestamp()
                                            .to_string()
                                            .into_bytes();
                            let symbol = "BTCUSDT".to_string();
                            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                            let response = FakeMarketDataResponse {
                                symbol,
                                timestamp,
                            };
                            tx.send(Ok(response)).await.unwrap();
                        };
        });

    Ok(Response::new(ReceiverStream::new(rx)))
    }

}


#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::1]:10000".parse().unwrap();

    let market = MarketService {
    };

    let svc = MarketServer::new(market);

    Server::builder().add_service(svc).serve(addr).await?;

    Ok(())
}