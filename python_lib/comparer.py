# prepare
import asyncio.runners
import os
import grpc
import pandas as pd
import time
import asyncio
import websocket
import json


if os.path.exists("trade_pb2.py"):
    os.remove("trade_pb2.py")
if os.path.exists("trade_pb2_grpc.py"):
    os.remove("trade_pb2_grpc.py")
os.system("python3 -m grpc_tools.protoc -I../proto/ --python_out=. --grpc_python_out=. trade.proto")

from trade_pb2 import GetAggTradeRequest, GetMarketDataRequest, TimeUnit, TimeDuration, GetMarketDataBatchRequest
from trade_pb2_grpc import TradeStub
import trade_pb2

class TradeClient:
    def __init__(self, host='localhost', port=10000):
        self.channel = grpc.insecure_channel(f"{host}:{port}")
        self.stub = TradeStub(self.channel)

    def get_agg_trade_stream(self, symbol):
        request = GetAggTradeRequest(symbol=symbol)
        response_stream = self.stub.GetAggTradeStream(request)
        for response in response_stream:
            yield response
    
    def get_market_data(self, symbol, time_interval=1, timeUnit=trade_pb2.SECONDS):
        print(f"Requesting market data for {symbol} with time interval {time_interval} seconds")
        duration = trade_pb2.TimeDuration(value=time_interval, unit=trade_pb2.SECONDS)
        request = GetMarketDataRequest(symbol=symbol, granularity = duration)
        response_stream = self.stub.GetMarketData(request)
        for rb in response_stream:
            yield rb
    
    def register_symbols(self, symbols):
        request = trade_pb2.RegisterSymbolRequest(symbols=symbols)
        response = self.stub.RegisterSymbol(request)
        return response

    def get_market_data_with_timing(self, symbols, time_interval=1, timeUnit=trade_pb2.SECONDS):
        print(f"Requesting market data for {symbol} with time interval {time_interval} seconds")
        duration = trade_pb2.TimeDuration(value=time_interval, unit=trade_pb2.SECONDS)
        request = GetMarketDataRequest(symbol=symbol, granularity = duration)
        response_stream = self.stub.GetMarketDataWithTiming(request)
        for rb in response_stream:
            yield rb
    def get_market_data_by_batch(self, symbols=['btcusdt'], time_interval=1, timeUnit=trade_pb2.SECONDS):
        print(f"Requesting market data for {symbols} with time interval {time_interval} seconds")
        duration = trade_pb2.TimeDuration(value=time_interval, unit=trade_pb2.SECONDS)
        request = GetMarketDataBatchRequest(symbols=symbols, granularity = duration)
        response_stream = self.stub.GetMarketDataByBatch(request)
        for rb in response_stream:
            yield rb

def maintain_latest_1000_entries(filename, new_entries):
    # create the file if not exists
    if not os.path.exists(filename):
        with open(filename, 'w') as file:
            file.write("")
    with open(filename, 'r') as file:
        lines = file.readlines()

    lines.extend(new_entries)

    if len(lines) > 1000:
        lines = lines[-1000:]

    with open(filename, 'w') as file:
        file.writelines(lines)

queue_from_binance = asyncio.Queue()

last_kline_data = None

import threading
import queue
kline_queue = queue.Queue()
def on_message(ws, message):
    global last_kline_data
    kline_data = json.loads(message)
    # print_kline(data)

    if last_kline_data is not None and kline_data['k']['t'] != last_kline_data['k']['t']:
        kline_queue.put(last_kline_data['k'])
        last_kline_data = kline_data
    else:
        last_kline_data = kline_data

def on_error(ws, error):
    print(error)

def on_close(ws):
    print("### closed ###")

def on_open(ws):
    print("### connected ###")

def binance_kline_websocket(symbol):
    socket_url = f"wss://stream.binance.com:9443/ws/{symbol}@kline_1m"
    print("connecting to ", socket_url) 
    ws = websocket.WebSocketApp(socket_url, on_message=on_message, on_error=on_error, on_close=on_close)  

    ws.run_forever()  

def print_kline(kline_data):
    kline = kline_data['k']  # 'k' 字段包含K线的具体数据
    is_final = kline['x']    # 'x' 字段表示K线是否已经结束
    if is_final:
        print("K线结束:")
    else:
        print("K线更新中:")
    
    print(f"开盘时间: {kline['t']}")
    print(f"开盘价: {kline['o']}")
    print(f"最高价: {kline['h']}")
    print(f"最低价: {kline['l']}")
    print(f"收盘价: {kline['c']}")
    print(f"成交量: {kline['v']}")
    print("----------------------")

def main():
    symbol = "btcusdt"
    interval = "1m"
    # create a task for the websocket
    # task_rpc = asyncio.create_task(generate_kline_from_rpc(queue_from_rpc))
    websocket_thread = threading.Thread(target=binance_kline_websocket, args=(symbol,))
    websocket_thread.start()
    
    # execute the task
    # start compare the data
    global last_kline_data
    client = TradeClient(host = "localhost", port=10000)
    for data in client.get_market_data_by_batch(["btcusdt"], time_interval=60):
        data_from_rpc = data.data
        print("rpc data " + str(data_from_rpc))
        print("getting binanance data")
        data_from_binance = None
        # while the queue is empty, wait
        while kline_queue.empty():
            pass
        data_from_binance = kline_queue.get()
        print("binance data " , str(data_from_binance))
        high_price_diff = abs(data_from_rpc.high - float(data_from_binance['h']))
        low_price_diff = abs(data_from_rpc.low - float(data_from_binance['l']))
        open_price_diff = abs(data_from_rpc.open - float(data_from_binance['o']))
        close_price_diff = abs(data_from_rpc.close- float(data_from_binance['c']))
        volume_diff = abs(data_from_rpc.volume - float(data_from_binance['v']))
        # dump it to disk with current timestamp
        # maintain 1000 entries
        diff = {}
        diff['high_price_diff'] = high_price_diff
        diff['low_price_diff'] = low_price_diff
        diff['open_price_diff'] = open_price_diff
        diff['close_price_diff'] = close_price_diff
        diff['volume_diff'] = volume_diff
        diff['timestamp'] = data_from_rpc.open_time
        # make it to json
        diff_json = json.dumps(diff)
        # maintain the latest 1000 entries
        print(data_from_rpc.open_time, data_from_binance['t'])
        maintain_latest_1000_entries("diff_btc.txt", [diff_json + "\n"])
        
        


async def generate_kline_from_rpc(queue):
    client = TradeClient(host = "localhost", port=10000)
    for data in client.get_market_data_by_batch(["btcusdt"], time_interval=60):
        print(data)
        await queue.put(data.data)
        
    

if __name__ == '__main__':
    # asyncio.run(main())
    main()
    # asyncio.run(binance_kline_websocket())
    