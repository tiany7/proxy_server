# BUILD

cc_library(
    name = "const_def",
    hdrs = ["const_def.h"],
    visibility = ["//visibility:public"],
)

cc_binary(
    name = "test_main",
    srcs = ["test_main.cc"],
    deps = [
        "@yaml-parser//:yaml-cpp",
        ":const_def",
    ],
    copts = ["-I/usr/include/boost", "-std=c++20"],  # 添加Boost头文件路径
    linkopts = ["-L/usr/include/boost/lib", "-lboost_system", "-lrt"], 
)
