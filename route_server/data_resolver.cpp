#include "data_resolver.h"
#include "yaml-cpp/yaml.h"
#include "const_def.h"

template class DataResolver<int>;

template <typename T>
DataResolver<T>::DataResolver() :stop_fetching_(false) {
    auto &&config_path = kStaticRoot + kConfigPath;
    auto &&config = YAML::LoadFile(config_path.c_str());
    auto &&route_server_conf =  config["route_server"];
    auto &&default_queue_size = route_server_conf["default_queue_size"].as<short>();


    data_queue_ = std::make_shared<boost::lockfree::queue<Result<T>>>(default_queue_size);
}

template <typename T>
DataResolver<T>::~DataResolver() {
    this->Stop();
}

template <typename T>
std::shared_ptr<boost::lockfree::queue<Result<T>>> DataResolver<T>::SubscribeData() {
    // do something
    return data_queue_;
}

template <typename T>
std::shared_ptr<boost::lockfree::queue<Result<T>>> DataResolver<T>::MockSubscribeData(T default_value) {
    request_mock_read(default_value);
    return data_queue_;
}

template <typename T>   
void DataResolver<T>::Stop() {
    // use a spinlock to stop fetching data
    for (bool expected = false; !stop_fetching_.compare_exchange_strong(expected, true); expected = false);
}

template <typename T>   
void DataResolver<T>::request_read() {
    // do nothing at this moment
}

template <typename T>
void DataResolver<T>::request_mock_read(T value) {
    std::thread t([this, value] {
        while (!this->stop_fetching_.load()) {
            while (!this->data_queue_->push(Result<T>(value)));
            // std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    });
    t.detach();
}