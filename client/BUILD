cc_library(
    name = "grpc_client",
    hdrs = ["grpc_client.hpp"],
    deps = ["//proto:route_server_cc_grpc",
            "@com_github_grpc_grpc//:grpc++",],
    copts = ["-std=c++20"],
    visibility = ["//visibility:public"]
)



cc_binary(
    name = "client_main",
    srcs = ["client_main.cpp"],
    deps = [":grpc_client",
            ],
    copts = ["-I/usr/include/boost", "-std=c++20"], 
    linkopts = ["-L/usr/include/boost/lib", "-lboost_system", "-lrt"], 
)