# prepare
import asyncio.runners
import os
import grpc
import pandas as pd
import time
import asyncio
import websocket
import json
import requests

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

field_names = [
    'open_time', 'open', 'high', 'low', 'close', 'volume',
    'close_time', 'quote_asset_volume', 'number_of_trades',
    'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore'
]

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


historical_bar_from_rpc = {}
limit = 2
# check the historical data if all symbols in this map has 30 entries
def check_historical_data():
    for symbol, data in historical_bar_from_rpc.items():
        if len(data) < limit:
            return False
    return True

# compare the first 29 entries of the historical data with the binance data and remove them, but not the last one
def compare_historical_data():
    if not check_historical_data():
        return
    first_timestamp = historical_bar_from_rpc[list(historical_bar_from_rpc.keys())[0]][0].open_time
    last_timestamp = historical_bar_from_rpc[list(historical_bar_from_rpc.keys())[0]][-2].close_time
    print("first timestamp ", first_timestamp)
    print("last timestamp ", last_timestamp)
    # get the data from binance
    symbols = list(historical_bar_from_rpc.keys())
    binance_data = get_klines(symbols, first_timestamp, last_timestamp)
    print("binance data ", binance_data)
    print("data type ", type(binance_data))
    for symbol, data in historical_bar_from_rpc.items():
        if len(data) < limit:
            return False
        for i in range(limit - 1):
            compare_and_log_individual_data(data[i], binance_data[symbol][i])
        # remove the first 29 entries
        historical_bar_from_rpc[symbol] = data[29:]
    return True

def compare_and_log_individual_data(data_from_rpc, data_from_binance):
    print(data_from_binance)
    high_price_diff = data_from_rpc.high - float(data_from_binance['high'])
    low_price_diff = data_from_rpc.low - float(data_from_binance['low'])
    open_price_diff = data_from_rpc.open - float(data_from_binance['open'])
    close_price_diff = data_from_rpc.close- float(data_from_binance['close'])
    volume_diff = data_from_rpc.volume - float(data_from_binance['volume'])
    high_price_diff_abs = abs(high_price_diff)
    low_price_diff_abs = abs(low_price_diff)
    open_price_diff_abs = abs(open_price_diff)
    close_price_diff_abs = abs(close_price_diff)
    volume_diff_abs = abs(volume_diff)
    outstanding = False
    eps = 1e-6
    if high_price_diff_abs > eps or low_price_diff_abs > eps or open_price_diff_abs > eps or close_price_diff_abs > eps or volume_diff_abs > eps:
        outstanding = True
    else:
        outstanding = False
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
    diff_json = json.dumps(diff)
    # maintain the latest 1000 entries
    if outstanding:
        maintain_latest_1000_entries(f"diff_{data_from_rpc.symbol}_outstanding.txt", [diff_json + "\n"])
    else:
        maintain_latest_1000_entries(f"diff_{data_from_rpc.symbol}.txt", [diff_json + "\n"])


def get_klines(symbols, start_time_ms, end_time_ms, interval='1m', limit=1000, contract_type='PERPETUAL'):
    base_url = 'https://fapi.binance.com/fapi/v1/continuousKlines'
    field_names = ['open_time', 'open', 'high', 'low', 'close', 'volume', 'close_time', 
                   'quote_asset_volume', 'number_of_trades', 'taker_buy_base_asset_volume', 
                   'taker_buy_quote_asset_volume', 'ignore']
    all_klines = {}

    for symbol in symbols:
        klines = []
        current_start_time = start_time_ms

        while current_start_time < end_time_ms:
            params = {
                'pair': symbol.upper(),  # 使用 'pair' 而不是 'symbol'
                'contractType': contract_type,  # 合约类型
                'interval': interval,
                'startTime': int(current_start_time),
                'endTime': int(end_time_ms),
                'limit': limit
            }
            
            try:
                response = requests.get(base_url, params=params)
                data = response.json()
            except Exception as e:
                print(f"请求 {symbol} 数据时发生异常: {e}")
                break
            
            if response.status_code != 200:
                print(f"请求 {symbol} 数据时出错: {data}")
                break

            if not data:
                break

            for each_data in data:
                new_data = {}
                for i in range(len(field_names)):
                    new_data[field_names[i]] = each_data[i]
                klines.append(new_data)
            print(f"获取 {symbol} 数据: {klines} ...")
            # 如果返回的数据少于限制，说明已经获取完所有数据
            if len(data) < limit:
                break

            # 更新下一个请求的开始时间为最后一条 K 线的结束时间
            last_end_time = data[-1][6]
            if last_end_time == current_start_time:
                # 防止死循环
                break
            current_start_time = last_end_time

            # 避免请求过快，遵守 Binance API 的速率限制
            time.sleep(0.5)

        all_klines[symbol] = klines

    return all_kline


def get_future_klines(symbols, start_time_ms, end_time_ms, interval='1m', limit=1000):
    base_url = 'https://fapi.binance.com/fapi/v1/continuousKlines'
    field_names = ['open_time', 'open', 'high', 'low', 'close', 'volume', 'close_time', 
                   'quote_asset_volume', 'number_of_trades', 'taker_buy_base_asset_volume', 
                   'taker_buy_quote_asset_volume', 'ignore']
    all_klines = {}

    for symbol in symbols:
        # 解析符号，提取交易对和合约类型
        if '_' in symbol:
            parts = symbol.split('_')
            if len(parts) == 2:
                pair = parts[0].upper()
                contract_type = parts[1].upper()
            else:
                print(f"符号格式错误: {symbol}")
                continue
        else:
            print(f"符号格式不包含下划线: {symbol}")
            continue
        
        klines = []
        current_start_time = start_time_ms

        while current_start_time < end_time_ms:
            params = {
                'pair': pair, 
                'contractType': 'PERPETUAL',
                'interval': interval,
                'startTime': int(current_start_time),
                'endTime': int(end_time_ms),
                'limit': limit
            }
            
            try:
                response = requests.get(base_url, params=params)
                data = response.json()
            except Exception as e:
                print(f"请求 {symbol} 数据时发生异常: {e}")
                break
            
            if response.status_code != 200:
                print(f"请求 {symbol} 数据时出错: {data}")
                break

            if not data:
                break

            for each_data in data:
                new_data = {}
                for i in range(len(field_names)):
                    new_data[field_names[i]] = each_data[i]
                klines.append(new_data)
            print(f"获取 {symbol} 数据: {klines[:1]} ...")  # 为了简洁，只打印第一条数据
                
            if len(data) < limit:
                break

            # 更新下一个请求的开始时间为最后一条 K 线的结束时间
            last_end_time = data[-1][6]
            if last_end_time == current_start_time:
                # 防止死循环
                break
            current_start_time = last_end_time

            # 避免请求过快，遵守 Binance API 的速率限制
            time.sleep(0.5)

        all_klines[symbol] = klines

    return all_klines

def find_time_windows_with_trade_size(symbol, start_time_ms, end_time_ms, desired_trade_size, interval_ms=60000):
    base_url = 'https://api.binance.com/api/v3/aggTrades'
    found_windows = []

    current_start_time = start_time_ms
    all_agg_trades = []
    while current_start_time < end_time_ms:
        current_end_time = current_start_time + interval_ms
        if current_end_time > end_time_ms:
            current_end_time = end_time_ms

        params = {
            'symbol': symbol.upper(),
            'startTime': int(current_start_time),
            'endTime': int(current_end_time),
            'limit': 1000  # 最大值为1000
        }

        try:
            response = requests.get(base_url, params=params)
            data = response.json()
        except Exception as e:
            print(f"请求 {symbol} 数据时发生异常: {e}")
            break

        if response.status_code != 200:
            print(f"请求 {symbol} 数据时出错: {data}")
            break

        if not data:
            # 如果没有数据，移动时间窗口
            current_start_time += interval_ms
            continue

        for each_data in data:
            first_trade_id = each_data['f']
            last_trade_id = each_data['l']
            if last_trade_id - first_trade_id + 1 == desired_trade_size:
                found_windows.append(each_data)
                volume = 0
                volume = each_data['q']
                each_data['volume'] = volume
        # name the fields
        new_data = data
        all_agg_trades.extend(new_data)

        # 移动时间窗口
        current_start_time += interval_ms

        # 遵守 API 速率限制
        time.sleep(0.2)

    return found_windows, all_agg_trades


def main():
    found_window, all_trade = find_time_windows_with_trade_size('ethusdt', 1727649960000, 1727649960000 + 60000 - 1, 681)
    print("found window ")
    for trade in found_window:
        print(trade)
    print("all trade ", len(all_trade))
    # last 6 records
    for trade in all_trade[-20:]:
        print(trade, "with volume = ", trade['q'])
        


async def generate_kline_from_rpc(queue):
    client = TradeClient(host = "localhost", port=10000)
    for data in client.get_market_data_by_batch(["btcusdt"], time_interval=60):
        print(data)
        await queue.put(data.data)
        
    

if __name__ == '__main__':
    # asyncio.run(main())
    main()
    # asyncio.run(binance_kline_websocket())
    