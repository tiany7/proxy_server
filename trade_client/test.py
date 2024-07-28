import asyncio
from my_python_package import TradeClientWrapper, add

async def main():
    client = TradeClientWrapper("http://localhost:10000")


    readers = client.get_market_data_by_batch(["btcusdt", "ethusdt"], 15)
    print("Readers:", readers)
    stream_reader = readers["btcusdt"]
    while True:
        data = await stream_reader.next()
        if not data:
            break
        print("Stream data:", data)

    

asyncio.run(main())
