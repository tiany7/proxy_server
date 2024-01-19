# BUILD

cc_binary(
    name = "test_main",
    srcs = ["test_main.cc"],
    copts = ["-I/usr/include/boost"],  # 添加Boost头文件路径
    linkopts = ["-L/usr/include/boost/lib", "-lboost_system", "-lrt"], 
)
