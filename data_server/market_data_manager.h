#pragma once

#include <atomic>
#include <thread>
#include <chrono>
#include <memory>

#include <boost/lockfree/queue.hpp>
#include <boost/threadpool.hpp>

#include "const_def.h"
#include "errors/error.hpp"


struct MarketDataConfig {
    std::string api_key;
    std::string secret;
    short max_threads;

};

template <typename T>
class MarketDataManager {
private:
    std::unique_ptr<boost::threadpool::pool> thread_pool_;
    std::shared_ptr<Binance::MarketData> market_data_;
    std::atomic<bool> stop_fetching_; // stop fetching data from the source
    void subscribe();
public:
    MarketDataManager(const MarketDataConfig &config);
    ~MarketDataManager();
    std::shared_ptr<boost::lockfree::queue<Result<T>>> SubscribeData(const std::string &symbol, const std::string &type = "PERPETUAL");
    void Stop();
};