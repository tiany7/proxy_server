#pragma once

#include <atomic>
#include <thread>
#include <chrono>
#include <memory>

#include <boost/lockfree/queue.hpp>

#include "const_def.h"
#include "errors/error.hpp"


// assume we have a class that continually sends data to here
template <typename T>
    requires std::is_default_constructible_v<T> && std::is_assignable_v<T&, const T&>
class DataResolver {
private:
    // TODO(yuanhan): consider changing this to atomic shared pointer
    std::shared_ptr<boost::lockfree::queue<Result<T>>> data_queue_;
    std::atomic<bool> stop_fetching_; // stop fetching data from the source

// TODO(yuanhan): think about how to launch a request to binance
// TODO(yuanhan): try multiplexing using shared memory
public:
    DataResolver();
    ~DataResolver();
    std::shared_ptr<boost::lockfree::queue<Result<T>>> SubscribeData();
    std::shared_ptr<boost::lockfree::queue<Result<T>>> MockSubscribeData(T default_value);
    void Stop();
    void request_read();
    void request_mock_read(T value);
};
