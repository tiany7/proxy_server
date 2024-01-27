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

// Add the missing include for grpcpp/server_thread_pool.h
#include <grpcpp/server_thread_pool.h>

// 其他的头文件和代码


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
        std::atomic<std::shared_ptr<boost::lockfree::queue<Result<int>>>> handle;
        handle.store(std::move(data_resolver->MockSubscribeData(default_value)));
        Result<int> result;
        while (!context->IsCancelled() && !context->IsFinished()) {
            while (handle.load()->pop(result)) {
                response.set_msg(*result);
                writer->Write(response);
            }
        }

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

        std::shared_ptr<grpc::ThreadPoolInterface> thread_pool = grpc::CreateDefaultThreadPool(4);
        builder.SetThreadPool(thread_pool);

        std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
        server->Wait();
    }
}


