#include "grpc_route_server.hpp"

int main() {
    RouteServer server;
    server.Run("0.0.0.0", 8081, 3);
    return 0;
}