use clap::{App, Arg};

pub mod trade {
    tonic::include_proto!("trade");
}

use trade::trade_client::TradeClient;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = TradeClient::connect("http://[::1]:10000").await?;
    let matches = App::new("Binance CLI Tool")
    .version("1.0")
    .author("tiany7")
    .about("This is a binance cli tool that could help to get the aggregated trade data from binance.")
    .arg(
        Arg::with_name("symbol")
            .short('s')
            .long("symbol")
            .value_name("SYMBOL")
            .help("The symbol you want to get")
            .takes_value(true),
    )
    .get_matches();

    let requested_symbol = matches.value_of("symbol").unwrap();
    let request = tonic::Request::new(trade::GetAggTradeRequest {
        symbol: requested_symbol.to_string(),
    });

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