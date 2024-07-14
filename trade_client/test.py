import asyncio
from my_python_package import TradeClientWrapper, add

async def main():
    client = TradeClientWrapper("http://localhost:10000")


    result = await client.add_async(4, 2)
    print(f"Result of add: {result}")

    

asyncio.run(main())
