use clap::{App, Arg};

pub mod trade {
    include!("../proto/generated_code/trade.rs");
}

use trade::trade_client::TradeClient;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {

    let requested_symbol = "BTCUSDT";
    let request = tonic::Request::new(trade::GetAggTradeRequest {
        symbol: requested_symbol.to_string(),
    });

    let mut client = TradeClient::connect("http://13.208.211.151:10000").await?;
    let response = client.get_agg_trade_stream(request).await?;
    let mut response = response.into_inner();
    let mut count = 0;
    while let Some(market_data) = response.message().await? {
        println!("{:?}", market_data);
        count += 1;
        if count == 10 {
            break;
        }
    }
    Ok(())
}