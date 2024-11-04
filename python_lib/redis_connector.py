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
        results = r.get_latest_kline_records(redis_key, int(count))
        json_array = [MessageToJson(res, including_default_value_fields=True) for res in results]
        if is_reverse == '1':
            json_array.reverse()
        return jsonify(json_array)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8088)

