#include <iostream>

#include <boost/asio.hpp>

#include "grpc_client.hpp"

int main() {
    grpc::ChannelArguments channel_args;
    channel_args.SetMaxReceiveMessageSize(INT_MAX);
    channel_args.SetMaxSendMessageSize(INT_MAX);

    RouteServerClient client(grpc::CreateCustomChannel("0.0.0.0:8081", grpc::InsecureChannelCredentials(), channel_args));
    client.GetClientHeartBeat();

    return 0;
}
