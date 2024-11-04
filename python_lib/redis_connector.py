import redis
import os
if os.path.exists("trade_pb2.py"):
    os.remove("trade_pb2.py")
if os.path.exists("trade_pb2_grpc.py"):
    os.remove("trade_pb2_grpc.py")
os.system("python3 -m grpc_tools.protoc -I../proto/ --python_out=. --grpc_python_out=. trade.proto")

from trade_pb2 import GetAggTradeRequest, GetMarketDataRequest, TimeUnit, TimeDuration, GetMarketDataBatchRequest
from trade_pb2_grpc import TradeStub
import trade_pb2
import time
from trade_pb2 import BarData
import requests
from http.server import BaseHTTPRequestHandler, HTTPServer
import urllib


import redis
import os
import json
import time
from flask import Flask, request, jsonify
from google.protobuf.json_format import MessageToJson
from trade_pb2 import BarData
import unittest
from unittest.mock import patch, MagicMock
import ccxt
from threading import Semaphore

app = Flask(__name__)

class RedisReader:
    def __init__(self, host='localhost', port=6379, db=0):
        self.redis_client = redis.StrictRedis(host=host, port=port, db=db)
    
    def kline_key(self, symbol: str, granularity:int):
        return f"{symbol}-aggTrade-PT{granularity}S"
    
    def get_earliest_timestamp(self):
        key = "linausdt-aggTrade-PT60S"
        zset_items = self.redis_client.zrevrange(key, 1, 1, withscores=True)
        if len(zset_items) == 0:
            print("insufficient data")
            return
        for member, _ in zset_items:
            bar_data = BarData()
            bar_data.ParseFromString(member)
            return bar_data.open_time

    def get_latest_kline_records(self, key, count=1500):
        zset_items = self.redis_client.zrevrange(key, 0, count - 1, withscores=True)
        bar_data_list = []
        for member, _ in zset_items:
            bar_data = BarData()
            bar_data.ParseFromString(member)
            bar_data_list.append(bar_data)
        return bar_data_list

r = RedisReader()

semaphore = Semaphore(10)  # Limit concurrent requests to 10


def fetch_agg_trades(symbol, from_trade_id, to_trade_id, limit=100):
    symbol = symbol.replace('/', '')
    all_trades = []
    current_id = from_trade_id

    base_url = "https://fapi.binance.com/fapi/v1/aggTrades"

    while current_id <= to_trade_id:
        acquired = semaphore.acquire(timeout=0.4)
        if not acquired:
            time.sleep(0.1)
            continue
        try:
            params = {
                'symbol': symbol,
                'fromId': current_id,
                'limit': limit
            }
            response = requests.get(base_url, params=params)
            trades = response.json()

            for trade in trades:
                if current_id > to_trade_id:
                    break
                if from_trade_id <= trade['a'] <= to_trade_id:
                    all_trades.append({
                        'trade_id': trade['a'],
                        'price': trade['p'],
                        'quantity': trade['q'],
                        'first_trade_id': trade['f'],
                        'last_trade_id': trade['l'],
                        'timestamp': trade['T'],
                        'is_buyer_maker': trade['m'],
                    })
                current_id = trade['a'] + 1
            if not trades or trades[-1]['a'] >= to_trade_id:
                break
        finally:
            semaphore.release()

    return all_trades

def update_bar_data_with_agg_trades(bar_data, agg_trades):
    for trade in agg_trades:
        price = trade['price']
        quantity = trade['quantity']

        if bar_data.open == 0:
            bar_data.open = price

        bar_data.close = price
        bar_data.high = max(bar_data.high, price)
        bar_data.low = min(bar_data.low, price) if bar_data.low != 0 else price
        bar_data.volume += quantity
        bar_data.number_of_trades += 1
        bar_data.taker_buy_base_asset_volume += quantity if trade['is_buyer_maker'] else 0
        bar_data.taker_buy_quote_asset_volume += price * quantity if trade['is_buyer_maker'] else 0
        bar_data.last_agg_trade_id = trade['trade_id']
        if bar_data.first_agg_trade_id == 0:
            bar_data.first_agg_trade_id = trade['trade_id']

def fix_the_missing_data(bar_data_list, symbol, limit=100):
    missing_bars = []

    for i in range(1, len(bar_data_list) - 1):
        current_bar = bar_data_list[i]
        prev_bar = bar_data_list[i - 1]
        next_bar = bar_data_list[i + 1]

        if (current_bar.last_agg_trade_id + 1 != next_bar.first_agg_trade_id or
                current_bar.missing_agg_trade_start_id != 0 or
                current_bar.missing_agg_trade_end_id != 0):
            missing_bars.append(current_bar)

    for bar in missing_bars:
        from_trade_id = bar.missing_agg_trade_start_id if bar.missing_agg_trade_start_id != 0 else bar.first_agg_trade_id
        to_trade_id = bar.missing_agg_trade_end_id if bar.missing_agg_trade_end_id != 0 else bar.last_agg_trade_id

        agg_trades = fetch_agg_trades(symbol, from_trade_id, to_trade_id, limit)
        filtered_trades = [trade for trade in agg_trades if bar.open_time <= trade['timestamp'] <= bar.close_time]
        update_bar_data_with_agg_trades(bar, filtered_trades)

@app.route('/binance_um/kline', methods=['GET'])
def get_kline():
    symbol = request.args.get('symbol', '')
    count = request.args.get('count', '1500') 
    granularity = request.args.get('granularity', '60')  
    is_reverse = request.args.get('is_reverse', '0')  
    
    if not symbol or not count or not granularity:
        return jsonify({"error": "Missing parameters"}), 400
    

    redis_key = f"{symbol}-aggTrade-PT{granularity}S"

    try:
        results = r.get_latest_kline_records(redis_key, int(count) + 1) # get one more to do error handling
        fix_the_missing_data(results, symbol)
        json_array = [MessageToJson(res, including_default_value_fields=True) for res in results]
        if is_reverse == '1':
            json_array.reverse()
        return jsonify(json_array)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8088)



class TestBarDataFunctions(unittest.TestCase):
    @patch('requests.get')
    def test_fetch_agg_trades(self, mock_get):
        mock_response = MagicMock()
        mock_response.json.return_value = [
            {
                'a': 1000001,
                'p': "40000.0",
                'q': "0.5",
                'f': 1000000,
                'l': 1000001,
                'T': 1620000000000,
                'm': True,
                'M': True
            }
        ]
        mock_get.return_value = mock_response

        trades = fetch_agg_trades('BTC/USDT', 1000000, 1000010)
        self.assertEqual(len(trades), 1)
        self.assertEqual(trades[0]['trade_id'], 1000001)
        self.assertEqual(trades[0]['price'], 40000.0)
        self.assertEqual(trades[0]['quantity'], 0.5)
    def test_update_bar_data_with_agg_trades(self):
        bar_data = BarData(
            open=0,
            high=0,
            low=0,
            close=0,
            volume=0,
            number_of_trades=0,
            taker_buy_base_asset_volume=0,
            taker_buy_quote_asset_volume=0,
            first_agg_trade_id=0,
            last_agg_trade_id=0
        )

        agg_trades = [
            {'trade_id': 1000001, 'price': 40000.0, 'quantity': 0.5, 'first_trade_id': 1000000, 'last_trade_id': 1000001, 'timestamp': 1620000000000, 'is_buyer_maker': True, 'is_best_match': True},
            {'trade_id': 1000002, 'price': 41000.0, 'quantity': 1.0, 'first_trade_id': 1000001, 'last_trade_id': 1000002, 'timestamp': 1620000005000, 'is_buyer_maker': False, 'is_best_match': True}
        ]

        updated_bar_data = update_bar_data_with_agg_trades(bar_data, agg_trades)
        self.assertEqual(updated_bar_data.open, 40000.0)
        self.assertEqual(updated_bar_data.close, 41000.0)
        self.assertEqual(updated_bar_data.high, 41000.0)
        self.assertEqual(updated_bar_data.low, 40000.0)
        self.assertEqual(updated_bar_data.volume, 1.5)
        self.assertEqual(updated_bar_data.number_of_trades, 2)
        self.assertEqual(updated_bar_data.taker_buy_base_asset_volume, 0.5)
        self.assertEqual(updated_bar_data.taker_buy_quote_asset_volume, 40000.0 * 0.5)
        self.assertEqual(updated_bar_data.first_agg_trade_id, 1000001)
