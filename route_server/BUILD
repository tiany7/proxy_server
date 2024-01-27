cc_library( 
    name = "data_resolver",
    srcs = ["data_resolver.cpp"],
    hdrs = ["data_resolver.h"],
    deps = ["//:const_def",
            "//errors:errors",
            "@yaml-parser//:yaml-cpp",
            ],
    copts = ["-std=c++20"],
    # copts = ["-I/usr/include/boost", "-std=c++20"], 
    # linkopts = ["-L/usr/include/boost/lib", "-lboost_system", "-lrt"], 
    visibility = ["//visibility:public"]
)

cc_binary(
    name = "server",
    srcs = ["server_main.cpp"],
    deps = [":data_resolver",
        ],
    copts = ["-I/usr/include/boost", "-std=c++20"], 
    linkopts = ["-L/usr/include/boost/lib", "-lboost_system", "-lrt", "-lpthread"], 
)

cc_library(
    name = "session_server",
    hdrs = ["session.hpp"],
    deps = [":data_resolver",
            "//proto:route_server_proto",
            ],
    copts = ["-std=c++20"],
    # copts = ["-I/usr/include/boost", "-std=c++20"], 
    # linkopts = ["-L/usr/include/boost/lib", "-lboost_system", "-lrt"], 
    visibility = ["//visibility:public"]
)

cc_library(
    name = "grpc_route_server",
    hdrs = ["grpc_route_server.hpp"],
    deps = [":data_resolver",
            "//proto:route_server_cc_grpc",
            "@com_github_grpc_grpc//:grpc++",
            "@com_github_grpc_grpc//:grpc++_reflection",],
    includes = ["@com_github_grpc_grpc//src/cpp/server"], 
    copts = ["-std=c++20"],
)


cc_binary(
    name = "route_server_main",
    srcs = ["route_server_main.cpp"],
    deps = [":grpc_route_server",
            ],
    copts = ["-I/usr/include/boost", "-std=c++20"], 
    linkopts = ["-L/usr/include/boost/lib", "-lboost_system", "-lrt"], 
)