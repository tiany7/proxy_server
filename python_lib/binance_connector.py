# prepare
import os
import grpc
import lz4.frame
import pandas as pd
import time

if os.path.exists("trade_pb2.py"):
    os.remove("trade_pb2.py")
if os.path.exists("trade_pb2_grpc.py"):
    os.remove("trade_pb2_grpc.py")
os.system("python3 -m grpc_tools.protoc -I../proto/ --python_out=. --grpc_python_out=. trade.proto")

from trade_pb2 import GetAggTradeRequest, GetMarketDataRequest, TimeUnit, TimeDuration
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

if __name__ == '__main__':
    client = TradeClient(host = "localhost", port=10000)
    start_time = time.time()
    import datetime
    for trade in client.get_market_data('btcusdt', time_interval=3):
        # if you want to convert to pandas dataframe
        # df = trade.to_pandas()
        # print(trade)
        pass
        # time_from_timestamp = datetime.datetime.fromtimestamp(trade.data.close_time/1e3)
        # current_time = datetime.datetime.now()
        # time_difference = current_time - time_from_timestamp
        # print(f"Time difference: {time_difference.total_seconds() * 1e3}")