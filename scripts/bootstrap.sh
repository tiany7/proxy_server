#!/bin/bash

BINANCE_KEY=$BINANCE_KEY

if [ -z "$BINANCE_KEY" ]; then
    echo "BINANCE_KEY 环境变量未设置，请设置后再运行该脚本。"
    exit 1
else
    echo "BINANCE_KEY 已设置为: $BINANCE_KEY"
fi


cargo run --bin binance_server &

