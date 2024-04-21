# proxy_server
A proxy server

# Tutorial
## Requirement
- rust (follow the tutorial [here](https://www.rust-lang.org/tools/install))
- python3
- pip3
- protobuf-compiler
- grpcio-tools
- docker-families


## How to run
1. Clone the repository
2. Lauch the metrics server
```bash
sh scripts/init_metrics_server.sh start
```
3. cargo run
```bash
cargo run --bin data_server
```
# Stop the metrics server
```bash
sh scripts/init_metrics_server.sh stop
```




