load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load('@bazel_tools//tools/build_defs/repo:git.bzl', 'git_repository')

git_repository(
    name = "yaml-parser",
    remote = "https://github.com/jbeder/yaml-cpp.git",
    commit = "9eb1142900096b9115ba2db2a521c76c117cacd9",
)


git_repository(
    name = "binance-cpp",
    remote = "https://github.com/tiany7/binance-cpp.git",
    commit = "7d4e4c3b7992801f46d46735a68bb8343aafb644",
)

http_archive(
    name = "com_google_googletest",
    urls = ["https://github.com/google/googletest/archive/16f637fbf4ffc3f7a01fa4eceb7906634565242f.zip"],
    strip_prefix = "googletest-16f637fbf4ffc3f7a01fa4eceb7906634565242f",
    sha256 = "002d540f947e5981f54ddaab476d87b113d2a14822f21a34dca30f24c9492a24",
    )

http_archive(
    name = "com_google_protobuf",
    sha256 = "3bd7828aa5af4b13b99c191e8b1e884ebfa9ad371b0ce264605d347f135d2568",
    urls = ["https://github.com/protocolbuffers/protobuf/archive/v3.19.4.tar.gz"],
    strip_prefix = "protobuf-3.19.4",
    )

http_archive(
    name = "com_github_grpc_grpc",
    urls = ["https://github.com/grpc/grpc/archive/44c40ac23023b7b3dd82744372c06817cc203898.tar.gz",],
    strip_prefix = "grpc-44c40ac23023b7b3dd82744372c06817cc203898",
)

load("@com_google_protobuf//:protobuf_deps.bzl", "protobuf_deps")
protobuf_deps()

load("@com_github_grpc_grpc//bazel:grpc_deps.bzl", "grpc_deps")
grpc_deps()
load("@com_github_grpc_grpc//bazel:grpc_extra_deps.bzl", "grpc_extra_deps")
grpc_extra_deps()


load("@com_google_protobuf//:protobuf_deps.bzl", "protobuf_deps")
protobuf_deps()

git_repository(
    name = "json_lib",
    remote = "https://github.com/nlohmann/json.git",
    commit = "a3e6e26dc83a726b292f5be0492fcc408663ce55",
)