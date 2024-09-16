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

last_grouped_kline_data = {}

import threading
import queue
kline_queue = queue.Queue()
group_kline_queue = {}
def on_message(ws, message):
    global last_kline_data
    kline_data = json.loads(message)
    symbol = kline_data['s']

    if last_kline_data is not None and kline_data['k']['t'] != last_kline_data['k']['t']:
        kline_queue.put(last_kline_data['k'])
        last_kline_data = kline_data
    else:
        last_kline_data = kline_data

def on_multiple_message(ws, message):
    global last_grouped_kline_data
    kline_data = json.loads(message)['data']
    # print("kline data ", kline_data)
    symbol = str(kline_data['s']).lower()
    if last_grouped_kline_data.get(symbol) is None:
        print("init queue for ", symbol)
        group_kline_queue[symbol] = queue.Queue()
    elif last_grouped_kline_data[symbol]['k']['t'] != kline_data['k']['t']:
        group_kline_queue[symbol].put(last_grouped_kline_data[symbol]['k'])
    last_grouped_kline_data[symbol] = kline_data

def on_error(ws, error):
    pass

def on_close(ws):
    print("### closed ###")

def on_open(ws):
    print("### connected ###")

def binance_kline_websocket(symbol):
    socket_url = f"wss://stream.binance.com:9443/ws/{symbol}@kline_1m"
    print("connecting to ", socket_url) 
    ws = websocket.WebSocketApp(socket_url, on_message=on_message, on_error=on_error, on_close=on_close)  

    ws.run_forever()  

def binance_combined_kline_websocket(symbols):
    # Create combined stream URL for multiple symbols
    streams = "/".join([f"{symbol}@kline_1m" for symbol in symbols])
    socket_url = f"wss://stream.binance.com:9443/stream?streams={streams}"
    print("connecting to ", socket_url)
    # websocket.enableTrace(True)
    ws = websocket.WebSocketApp(socket_url, on_message=on_multiple_message, on_error=on_error, on_close=on_close)
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

import datetime
import pytz
def convert_timestamp_to_pst(unix_timestamp_ms):
    timestamp_sec = unix_timestamp_ms / 1000.0
    utc_time = datetime.datetime.fromtimestamp(timestamp_sec, pytz.utc)
    west_coast_time = utc_time.astimezone(pytz.timezone('America/Los_Angeles'))
    return west_coast_time




def main():
    symbol = "btcusdt"
    interval = "1m"
    # create a task for the websocket
    # task_rpc = asyncio.create_task(generate_kline_from_rpc(queue_from_rpc))
    symbols = ['btcusdt' , 'ethusdt', 'bnbusdt', 'adausdt', 'dogeusdt', 'solusdt']
    batch_symbols = ['1000LUNCUSDT', '1000SHIBUSDT', '1000XECUSDT', '1INCHUSDT', 'AAVEUSDT', 'ADAUSDT', 'ALGOUSDT', 'ALICEUSDT', 'ALPHAUSDT', 'ANKRUSDT', 'APEUSDT', 'API3USDT', 'ARPAUSDT', 'ATAUSDT', 'ATOMUSDT', 'AVAXUSDT', 'AXSUSDT', 'BAKEUSDT', 'BALUSDT', 'BATUSDT', 'BCHUSDT', 'BELUSDT', 'BLZUSDT', 'BNBUSDT', 'BTCUSDT', 'C98USDT', 'CELOUSDT', 'CELRUSDT', 'CHRUSDT', 'CHZUSDT']
    batch_symbol_lower = [symbol.lower() for symbol in batch_symbols]
    websocket_thread = threading.Thread(target=binance_combined_kline_websocket, args=(symbols,))
    # websocket_thread = threading.Thread(target=binance_kline_websocket, args=(symbol,))
    websocket_thread.start()
    
    # execute the task
    # start compare the data
    global last_kline_data
    client = TradeClient(host = "localhost", port=10000)
    need_to_track_first_agg_trade_id = False
    last_agg_trade_id = 0
    for data in client.get_market_data_by_batch(symbols, time_interval=60):
        outstanding = False
        data_from_rpc = data.data
        print("rpc data " + str(data_from_rpc))
        print("getting binanance data")
        symbol = data_from_rpc.symbol.lower()
        data_from_binance = None
        # while the queue is empty, wait
        while group_kline_queue.get(symbol) is None or group_kline_queue[symbol].empty():
            pass
        data_from_binance = group_kline_queue[symbol].get()
        print("binance data " , str(data_from_binance))

        print(data_from_rpc.high," vs ",  float(data_from_binance['h']))
        print(data_from_rpc.low," vs ",  float(data_from_binance['l']))
        print(data_from_rpc.open," vs ",  float(data_from_binance['o']))
        print(data_from_rpc.close," vs ",  float(data_from_binance['c']))
        high_price_diff = data_from_rpc.high - float(data_from_binance['h'])
        low_price_diff = data_from_rpc.low - float(data_from_binance['l'])
        open_price_diff = data_from_rpc.open - float(data_from_binance['o'])
        close_price_diff = data_from_rpc.close- float(data_from_binance['c'])
        volume_diff = data_from_rpc.volume - float(data_from_binance['v'])
        high_price_diff_abs = abs(high_price_diff)
        low_price_diff_abs = abs(low_price_diff)
        open_price_diff_abs = abs(open_price_diff)
        close_price_diff_abs = abs(close_price_diff)
        volume_diff_abs = abs(volume_diff)
        eps = 1e-6
        if high_price_diff_abs > eps or low_price_diff_abs > eps or open_price_diff_abs > eps or close_price_diff_abs > eps or volume_diff_abs > eps:
            outstanding = True
        # dump it to disk with current timestamp
        # maintain 1000 entries
        diff = {}
        diff['high_price_diff'] = high_price_diff
        diff['low_price_diff'] = low_price_diff
        diff['open_price_diff'] = open_price_diff
        diff['close_price_diff'] = close_price_diff
        diff['volume_diff'] = volume_diff
        diff['timestamp'] = str(convert_timestamp_to_pst(data_from_rpc.open_time))
        # make it to json
        diff["first_agg_trade_id"] = data_from_rpc.first_agg_trade_id
        diff["last_agg_trade_id"] = data_from_rpc.last_agg_trade_id
        diff_json = json.dumps(diff)
        # maintain the latest 1000 entries
        print(data_from_rpc.open_time, data_from_binance['t'])
        if need_to_track_first_agg_trade_id:
            output = f"previous last_agg_trade_id {last_agg_trade_id} current first_agg_trade_id {data_from_rpc.first_agg_trade_id}"
            maintain_latest_1000_entries(f"diff_{data_from_rpc.symbol}_outstanding_trade_id.txt", [output + "\n"])

        if outstanding:
            maintain_latest_1000_entries(f"diff_{data_from_rpc.symbol}_outstanding.txt", [diff_json + "\n"])
        else:
            maintain_latest_1000_entries(f"diff_{data_from_rpc.symbol}.txt", [diff_json + "\n"])
        
        


async def generate_kline_from_rpc(queue):
    client = TradeClient(host = "localhost", port=10000)
    for data in client.get_market_data_by_batch(["btcusdt"], time_interval=60):
        print(data)
        await queue.put(data.data)
        
    

if __name__ == '__main__':
    # asyncio.run(main())
    main()
    # asyncio.run(binance_kline_websocket())
    