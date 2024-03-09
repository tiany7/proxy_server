#include "market_data_manager.h"

template class MarketDataManager<int>;

template <typename T>
MarketDataManager<T>::MarketDataManager(const MarketDataConfig &config) 
            : stop_fetching_(false), thread_pool_(std::make_unique<boost::threadpool::pool>(config.max_threads)) {
    market_data_ = std::make_shared<Binance::MarketData>();
    market_data_->SetApiKeys(config.api_key, config.secret);
}

template <typename T>
MarketDataManager<T>::~MarketDataManager() {
    this->Stop();
}

template <typename T>
std::shared_ptr<boost::lockfree::queue<Result<T>>> MarketDataManager<T>::SubscribeData(const std::string &symbol, const std::string &type = "PERPETUAL") {
    auto async_read = [this, symbol, type] {
        while (!this->stop_fetching_.load()) {
            json result;
            MarketSymbolParams params;
            params.symbol = symbol;
            this->market_data_->CurrentAvgPrice(params, result);
            while (!this->data_queue_->push(Result<T>(result)));
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    };
    return data_queue_;
}
