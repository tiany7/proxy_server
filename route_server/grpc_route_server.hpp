#include <iostream>
#include <memory>
#include <string>
#include <grpc++/grpc++.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/server_context.h>
#include <grpcpp/server_posix.h>
#include <boost/thread/thread.hpp>



#include "data_resolver.h"
#include "proto/route_server.grpc.pb.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using route_server::HeartBeatRequest;
using route_server::HeartBeatResponse;


class RouteServerImpl final : public route_server::RouteServer::Service {
private:
    std::shared_ptr<boost::lockfree::queue<Result<int>>> data_mgr_;
public:
    grpc::Status GetClientHeartBeat(grpc::ServerContext* context, const route_server::HeartBeatRequest* request, grpc::ServerWriter<route_server::HeartBeatResponse>* writer) override {
        route_server::HeartBeatResponse response;

        auto default_value = request->msg();
        auto data_resolver = std::make_unique<DataResolver<int>>();
        // points to mock data
        auto handle = std::move(data_resolver->MockSubscribeData(default_value));
        Result<int> result;
        while (true) {
            while (handle->pop(result)) {
                std::cout<<boost::thread::id()<<" Received message from client: " << *result << std::endl;
                response.set_msg(*result);
                auto status = writer->Write(response);
                if (!status) {
                    std::cout<<"Client disconnected"<<std::endl;
                    goto end;
                }
            }
        }
end:
        return grpc::Status::OK;
    }
};

class RouteServer {
public:
    void Run(const std::string &server_addr, short port, short max_threads) {

        grpc::ServerBuilder builder;
        RouteServerImpl service;
        builder.AddListeningPort(server_addr + ":" + std::to_string(port), grpc::InsecureServerCredentials());
        builder.RegisterService(&service);


        boost::thread_group threadPool;


        std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
        
        
        for (int i = 0; i < max_threads; ++i) { 
            threadPool.create_thread([&server]() {
                server->Wait();
            });
        }

        
        threadPool.join_all();
    }
};


