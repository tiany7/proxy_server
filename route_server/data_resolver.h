#pragma once
#include <atomic>

#include <boost/lockfree/queue.hpp>

#include "const_def.h"

// assume we have a class that continually sends data to here
template <typename T>
    requires std::is_default_constructible_v<T> && std::is_assignable_v<T&, const T&>
class DataResolver {
private:
    boost::lockfree::queue<T> data_queue_;
    std::atomic<bool> stop_fetching_; // stop fetching data from the source

// TODO(yuanhan): think about how to launch a request to binance
// TODO(yuanhan): try multiplexing using shared memory
public:
    DataResolver();
    ~DataResolver() = default;
    boost::lockfree::queue<T>& SubscribeData();
    void ResolveData();
};
