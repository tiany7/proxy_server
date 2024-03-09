#include <iostream>
#include "src/binance/binance.h"


int main()
{
    Binance::MarketData market;
    json result;
    std::cout<<"Binance API C++"<<std::endl;
    market.SetApiKeys("2", "1");
    std::cout<<"Binance API C++ after"<<std::endl;

    // test connectivity to the Rest API
    market.TestConnectivity(result);
    std::cout <<"ok "<< result << std::endl;

    // current average price for a symbol
    MarketRecentTradesListParams params;
    params.symbol = "BTCUSDT";
	params.limit = 1000;
    while (true){
		market.RecentTradesList(params, result);
    	std::cout << result << std::endl;
	}

	Binance::BinanceWebsocket ws;

	// stream individual symbol ticker
	std::thread t([&ws](){
		ws.StreamSymbolTicker("btcusdt", [](beast::error_code, json result) -> bool
                     {
                        std::cout << result << std::endl;
						return true;
                     });
	});
	t.detach();

	// run event loop
	ws.Run();

    return 0;
}