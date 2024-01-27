#include <iostream>
#include <boost/asio.hpp>
#include "proto/route_server.pb.h"

class Client {
public:
    Client(boost::asio::io_context& io_context, const std::string& server_ip, short server_port)
        : io_context_(io_context),
          socket_(io_context),
          response_(std::make_shared<route_server::HeartBeatResponse>()) {
        endpoint_ = boost::asio::ip::tcp::endpoint(boost::asio::ip::make_address(server_ip), server_port);
        doConnect();
    }

    void sendRequest(const route_server::HeartBeatRequest& request) {
        std::cout<<"sent a request"<< request.DebugString()<<std::endl;
        std::string serialized_data;
        if (request.SerializeToString(&serialized_data)) {
            boost::asio::async_write(
                socket_, boost::asio::buffer(serialized_data),
                [this](boost::system::error_code ec, std::size_t /*length*/) {
                    if (ec) {
                        std::cerr << "Error in async_write: " << ec.message() << std::endl;
                    }
                });
        } else {
            std::cerr << "Error serializing HeartBeatRequest" << std::endl;
        }
    }

    void startReading() {
        doRead();
    }

    route_server::HeartBeatResponse getResponse() const {
        return *response_;
    }

private:
    void doConnect() {
        socket_.async_connect(
            endpoint_,
            [this](boost::system::error_code ec) {
                if (!ec) {
                    std::cout << "Connected to server" << std::endl;

                    // 构造 HeartBeatRequest 并发送
                    route_server::HeartBeatRequest request;
                    request.set_msg(42);  // 设置你的实际数据

                    sendRequest(request);

                    // 启动异步读取
                    startReading();
                } else {
                    std::cerr << "Error in async_connect: " << ec.message() << std::endl;
                }
            });
    }

    void doRead() {
        boost::asio::async_read(
            socket_, boost::asio::dynamic_buffer(response_data_),
            [this](boost::system::error_code ec, std::size_t length) {
                if (!ec) {
                    handleResponse();
                    // 继续异步读取下一个响应
                    doRead();
                } else {
                    std::cerr << "Error in async_read: " << ec.message() << std::endl;
                }
            });
    }

    void handleResponse() {
        // 处理从服务器接收到的数据
        if (response_->ParseFromString(response_data_)) {
            std::cout << "Received data from server: " << response_->DebugString() << std::endl;
        } else {
            std::cerr << "Error parsing received data" << std::endl;
        }

        // 清空 response_data_ 以准备接收下一个响应
        response_data_.clear();
    }

    boost::asio::io_context& io_context_;
    boost::asio::ip::tcp::socket socket_;
    boost::asio::ip::tcp::endpoint endpoint_;
    std::shared_ptr<route_server::HeartBeatResponse> response_;
    std::string response_data_;  // 存储接收到的数据
};
