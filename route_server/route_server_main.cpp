#include "grpc_route_server.hpp"

int main() {
    RouteServer server;
    server.Run("localhost", 8081, 3);
    return 0;
}