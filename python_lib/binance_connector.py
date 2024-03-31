# prepare
import os
import grpc
import lz4
import pyarrow as pa
import pandas as pd


if os.path.exists("trade_pb2.py"):
    os.remove("trade_pb2.py")
if os.path.exists("trade_pb2_grpc.py"):
    os.remove("trade_pb2_grpc.py")
os.system("python3 -m grpc_tools.protoc -I../proto/ --python_out=. --grpc_python_out=. trade.proto")

from trade_pb2 import GetAggTradeRequest
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
    def decode_bytestream(self, bytestream):
        return lz4.frame.decompress(bytestream)
    
    def get_arrow_batch_from_bytes(self, bytestream):
        decompressed = self.decode_bytestream(bytestream)
        return pa.ipc.open_stream(decompressed).read_all()
    
    def get_market_data(self, symbol):
        request = GetAggTradeRequest(symbol=symbol)
        response_stream = self.stub.GetAggTradeStream(request)
        for response in response_stream:
            byte_data = response.data
            decoded_byte_data = self.decode_bytestream(byte_data)
            rb = pa.get_arrow_batch_from_bytes(decoded_byte_data)
            yield rb
if __name__ == '__main__':
    client = TradeClient(host = "", port=10000)
    print(client)
    for trade in client.get_market_data('BTCUSDT'):
        print(trade)
        # if you want to convert to pandas dataframe
        df = trade.to_pandas()
        print(df)
    
