#include <iostream>
#include <chrono>
#include <thread>

#include <boost/asio.hpp>


#include "data_resolver.h"

class Session : public std::enable_shared_from_this<Session> {
public:
    Session(boost::asio::ip::tcp::socket socket)
        : socket_(std::move(socket)) {}

    void start() {
        doRead();
    }

private:
    void doRead() {
        auto self(shared_from_this());
        socket_.async_read_some(boost::asio::buffer(buffer_),
            [this, self](boost::system::error_code ec, std::size_t length) {
                if (!ec) {
                    // 在这里处理接收到的数据
                    handleReceivedData(length);
                    doRead();
                } else if (ec == boost::asio::error::eof) {
                    // 客户端连接断开
                    std::cout << "Client disconnected" << std::endl;
                } else {
                    // 处理其他错误
                    std::cerr << "Error in async_read_some: " << ec.message() << std::endl;
                }
            });
    }

    void handleReceivedData(std::size_t length) {
        std::string receivedData(buffer_.begin(), buffer_.begin() + length);
        std::cout << "Received data from client: " << receivedData << std::endl;
    }

    boost::asio::ip::tcp::socket socket_;
    std::array<char, 1024> buffer_;
};

class Server {
private:
    boost::asio::ip::tcp::acceptor acceptor_;
    boost::asio::ip::tcp::socket socket_;
    std::array<char, 1024> buffer_;
    std::atomic<std::shared_ptr<DataResolver<int>> data_resolver_;


public:
    Server(boost::asio::io_context& io_context, DataResolver* dataResolver, short port)
        : acceptor_(io_context, boost::asio::ip::tcp::endpoint(boost::asio::ip::tcp::v4(), port)),
          socket_(io_context),
          dataResolver_(dataResolver) {
        startAccept();
    }
};