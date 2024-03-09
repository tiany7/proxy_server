pub mod trade {
    tonic::include_proto!("trade");
}

use trade::trade_client::TradeClient;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = MarketClient::connect("http://[::1]:10000").await?;
    let request = tonic::Request::new(market::FakeMarketDataRequest {
        symbol: "BTCUSDT".to_string(),
    });
    let response = client.get_fake_market_data(request).await?;
    let mut response = response.into_inner();
    let mut count = 0;
    while let Some(market_data) = response.message().await? {
        let symbol = market_data.symbol;
        let timestamp = String::from_utf8(market_data.timestamp).unwrap();
        // parse timestamp to human readable form
        let timestamp = chrono::NaiveDateTime::parse_from_str(&timestamp, "%s")?;
        println!("Symbol: {}, Timestamp: {}", symbol, timestamp);
        count += 1;
        if count == 10 {
            break;
        }
    }
    Ok(())
}