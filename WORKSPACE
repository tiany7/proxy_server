load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load('@bazel_tools//tools/build_defs/repo:git.bzl', 'git_repository')

git_repository(
    name = "yaml-parser",
    remote = "https://github.com/jbeder/yaml-cpp.git",
    commit = "9eb1142900096b9115ba2db2a521c76c117cacd9",
)

http_archive(
    name = "com_google_protobuf",
    sha256 = "3bd7828aa5af4b13b99c191e8b1e884ebfa9ad371b0ce264605d347f135d2568",
    urls = ["https://github.com/protocolbuffers/protobuf/archive/v3.19.4.tar.gz"],
    strip_prefix = "protobuf-3.19.4",
)

load("@com_google_protobuf//:protobuf_deps.bzl", "protobuf_deps")
protobuf_deps()
