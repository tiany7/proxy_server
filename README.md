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

# Python Client
## Requirement
Requirements can be found in `requirements.txt` in `python_lib` folder

## How to run
1. Install the requirements
```bash
pip3 install -r python_lib/requirements.txt
```

2. Run the client
```bash
python3 python_lib/client.py
```

TODOs: 
- Build the client using rust software
- Use FFI to create a python client



