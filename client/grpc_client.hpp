#include <iostream>

#include <grpcpp/grpcpp.h>

#include "proto/route_server.pb.h"
#include "proto/route_server.grpc.pb.h"

class RouteServerClient {
public:
    RouteServerClient(std::shared_ptr<grpc::Channel> channel) : stub_(route_server::RouteServer::NewStub(channel)) {}

    void GetClientHeartBeat() {
        route_server::HeartBeatRequest request;
        route_server::HeartBeatResponse response;
        request.set_msg(666);
        grpc::ClientContext context;
        std::unique_ptr<grpc::ClientReader<route_server::HeartBeatResponse>> reader(stub_->GetClientHeartBeat(&context, request));
        for (int i = 0; i < 10; i++) {
            if (reader->Read(&response)) {
                std::cout << "Received message from server: " << response.DebugString() << std::endl;
            } else {
                std::cout << "RPC finished." << std::endl;
                break;
            }
        }
    }

private:
    std::unique_ptr<route_server::RouteServer::Stub> stub_;
};

