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