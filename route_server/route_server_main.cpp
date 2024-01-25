#include "session.hpp"

int main() {
    auto &&config_path = kStaticRoot + kConfigPath;
    auto &&config = YAML::LoadFile(config_path.c_str());

    auto &&route_server_conf =  config["route_server"];

    auto port = route_server_conf["port"].as<short>();
    try {
        boost::asio::io_context io_context;
        Server server(io_context, port);  // 请替换成你希望监听的端口号
        io_context.run();
    } catch (std::exception& e) {
        std::cerr << "Exception: " << e.what() << std::endl;
    }
    return 0;
}