package(default_visibility = ["//visibility:public"])

load("@rules_proto//proto:defs.bzl", "proto_library")

proto_library(
    name = "route_server",
    srcs = ["route_server.proto"],
)

cc_proto_library(
    name = "route_server_proto",
    deps = [":route_server"],
)