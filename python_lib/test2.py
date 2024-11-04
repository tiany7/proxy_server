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

def export_zset_data(redis_client):
    # 获取所有键
    keys = redis_client.keys('*')

    # 存放所有 ZSET 数据的字典
    zset_data = {}

    # 遍历所有键
    for key in keys:
        # 判断键的类型
        key_type = redis_client.type(key)
        
        # 如果是 ZSET 类型
        if key_type == b'zset':
            # 获取 ZSET 中的所有元素及其分数
            zset_items = redis_client.zrange(key, 0, -1, withscores=True)
            # 存入字典
            zset_data[key.decode('utf-8', 'ignore')] = zset_items
    
    return zset_data

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
    # 1. 连接到 Redis 服务器
    redis_client = redis.StrictRedis(host='localhost', port=6379, db=0)
    # results = redis_client.zrangebyscore('1000luncusdt-aggTrade-PT60S',1729812240000 - 60000, 1729812240000 + 60000 , withscores=True)
    # # print(results)
    # for member, score in results:
    #     now = BarData()
    #     now.ParseFromString(member)
    #     print(now)
    # return

    # 2. 导出所有 ZSET 数据
    zset_data = export_zset_data(redis_client)


    # 3. 输出 ZSET 数据
    print(f"total symbols {len(zset_data.items())}")
    for zset_key, zset_items in zset_data.items():
        print(f"now symbol {zset_key} with len of {len(zset_items)}")
        continue
        # cnt = 3
        # zset_items.reverse()
        for member, score in zset_items:
            
            now = BarData()
            now.ParseFromString(member)
            print(now)
            input("press the continue")
        
    # fd, all_trades = find_time_windows_with_trade_size("vetusdt", 1728851580000 + 60000, 1728851580000 + 2 * 60000, 0)
    # for trade in all_trades:
    #     if trade['T'] == 1728851646927:
    #         print(trade, 1728851646927 - 1728851580000)

if __name__ == "__main__":
    main()
