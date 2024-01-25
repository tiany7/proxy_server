#pragma once

#include <iostream>
#include <chrono>
#include <thread>

#include <boost/asio.hpp>


#include "data_resolver.h"
#include "yaml-cpp/yaml.h"
#include "proto/route_server.pb.h"

class Session : public std::enable_shared_from_this<Session> {
public:
    Session(boost::asio::io_context& io_context, boost::asio::ip::tcp::socket socket)
        : data_resolver_(std::make_shared<DataResolver<int>>()),
          socket_(std::move(socket)),
          timer_(io_context) {}

    void Start() {
        doReadHeader();
    }

private:
   void doReadHeader() {
    auto self(shared_from_this());
    boost::asio::async_read(socket_, boost::asio::buffer(&request_, sizeof(request_)),
        [this, self](boost::system::error_code ec, std::size_t length) {
            if (!ec) {
                handleRequest();
                startSendingData();
            } else if (ec == boost::asio::error::eof) {
                // 客户端断开连接，调用 data_resolver_->Stop()
                handleConnectionClosed();
            } else {
                std::cerr << "Error in async_read: " << ec.message() << std::endl;
            }
        });
}

    void handleRequest() {
        std::cout << "Received message from client: " << request_.DebugString() << std::endl;
        auto default_value = request_.msg();

        data_mgr_ = data_resolver_->MockSubscribeData(default_value);
    }

    void startSendingData() {
        auto self(shared_from_this());
        timer_.expires_after(std::chrono::milliseconds(2));  // 设置定时器间隔
        timer_.async_wait([this, self](const boost::system::error_code& ec) {
            if (!ec) {
                // 检查是否有数据可发送
                if (!data_mgr_->empty()) {
                    sendData();
                }

                startSendingData();
            }
        });
    }

    void sendData() {
        // 从数据管理器中取出数据并发送
        Result<int> data;
        while (!data_mgr_->pop(data)) {
            route_server::HeartBeatResponse response;
            if (data.is_ok()) 
                response.set_msg(data.deref());
            else {
                std::string error_msg = std::string("Encountered error: ") + std::to_string(static_cast<int>(data.msg()));
                response.set_error_msg(error_msg);
                boost::asio::async_write(socket_, boost::asio::buffer(response.SerializeAsString()),
                    [this](boost::system::error_code ec, std::size_t length) {
                        if (ec) {
                            std::cerr << "Error in async_write: " << ec.message() << std::endl;
                        }else {
                            this->sendData();
                        }
                    });
            }
                
        } 
    }

    void handleConnectionClosed() {
        std::cout << "Client disconnected" << std::endl;
        data_resolver_->Stop(); // stop fetching data
    }

    boost::asio::ip::tcp::socket socket_;
    route_server::HeartBeatRequest request_;
    std::shared_ptr<DataResolver<int>> data_resolver_;
    std::shared_ptr<boost::lockfree::queue<Result<int>>> data_mgr_;
    boost::asio::steady_timer timer_;
};


class Server {
public:
    Server(boost::asio::io_context& io_context, short port)
        : io_context_(io_context),
          acceptor_(io_context, boost::asio::ip::tcp::endpoint(boost::asio::ip::tcp::v4(), port)) {
        doAccept();
    }

private:
    void doAccept() {
        acceptor_.async_accept(
            [this](boost::system::error_code ec, boost::asio::ip::tcp::socket socket) {
                if (!ec) {
                    std::make_shared<Session>(io_context_, std::move(socket))->Start();
                } else {
                    std::cerr << "Error in async_accept: " << ec.message() << std::endl;
                }

                doAccept();
            });
    }

    boost::asio::io_context& io_context_;
    boost::asio::ip::tcp::acceptor acceptor_;
};