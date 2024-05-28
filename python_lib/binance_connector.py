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

from trade_pb2 import GetAggTradeRequest, GetMarketDataRequest
from trade_pb2_grpc import TradeStub

class TradeClient:
    def __init__(self, host='localhost', port=10000):
        self.channel = grpc.insecure_channel(f"{host}:{port}")
        self.stub = TradeStub(self.channel)

    def get_agg_trade_stream(self, symbol):
        request = GetAggTradeRequest(symbol=symbol)
        response_stream = self.stub.GetAggTradeStream(request)
        for response in response_stream:
            yield response
    
    def get_market_data(self, symbol):
        request = GetMarketDataRequest(symbol=symbol)
        response_stream = self.stub.GetMarketData(request)
        for rb in response_stream:
            yield rb
if __name__ == '__main__':
    client = TradeClient(host = "localhost", port=10000)
    start_time = time.time()
    for trade in client.get_market_data('ethusdt'):
        now = time.time()
        print(f"Elapsed time: {now - start_time:.2f} seconds")
        start_time = now
        # if you want to convert to pandas dataframe
        # df = trade.to_pandas()
        # print(df)
    
