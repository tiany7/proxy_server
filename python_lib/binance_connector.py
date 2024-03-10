import grpc
from trade_pb2 import GetAggTradeRequest
from trade_pb2_grpc import TradeStub

class TradeClient:
    def __init__(self, host='localhost', port=10000):
        self.channel = grpc.insecure_channel(f'{host}:{port}')
        self.stub = TradeStub(self.channel)

    def get_agg_trade_stream(self, symbol):
        request = GetAggTradeRequest(symbol=symbol)
        response_stream = self.stub.GetAggTradeStream(request)
        for response in response_stream:
            yield response

if __name__ == '__main__':
    client = TradeClient(host = "13.231.201.123", port=10000)
    for trade in client.get_agg_trade_stream('BTCUSDT'):
        print(trade)