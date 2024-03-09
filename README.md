# proxy_server
A proxy server

# Tutorial
## Requirement
- rust (follow the tutorial [here](https://www.rust-lang.org/tools/install))

## How to run
1. Clone this repository
2. Run the server in the background
```bash
cargo run --bin binance_server &
```
3. Build the client in another terminal
```bash
cargo build --bin binance_client
```

4. Run the client
```bash
target/bin/binance_client --symbol BTCUSDT
```
