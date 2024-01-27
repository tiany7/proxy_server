package(default_visibility = ["//visibility:public"])

load("@rules_proto//proto:defs.bzl", "proto_library")
load("@com_github_grpc_grpc//bazel:cc_grpc_library.bzl", "cc_grpc_library")

proto_library(
    name = "route_server",
    srcs = ["route_server.proto"],
)

cc_proto_library(
    name = "route_server_proto",
    deps = [":route_server"],
)

cc_grpc_library(
    name = "route_server_cc_grpc",
    srcs = [":route_server"],
    grpc_only = True,
    deps = [":route_server_proto"],
)