from nexushub.binance import BinanceWSClient
from nexushub.constants import BinanceAccountType
from nexushub.utils import LiveClock
import asyncio
import numpy as np
import orjson

latency = []
clock = LiveClock()
msg_count = 0

def handler(data):
    try:
        data = orjson.loads(data)['data']

        global msg_count
        msg_count += 1
        latency.append(clock.timestamp_ms() - data["E"])
        if msg_count >= 1000:
            print_latency()
    except Exception:
        pass

def print_latency():
    global latency, msg_count
    length = len(latency)
    avg = np.mean(latency)
    mid = np.median(latency)
    q_99 = np.percentile(latency, 99)
    print(f"--- Latency Statistics (n={length}) ---")
    print(f"Mean: {avg:.4f} ms | Median: {mid:.4f} ms | 99th Percentile: {q_99:.4f} ms")
    print(f"Min: {min(latency):.4f} ms | Max: {max(latency):.4f} ms")
    latency = []  # Reset latency after printing statistics
    msg_count = 0  # Reset message count after printing statistics


async def main():
    binance_client = BinanceWSClient(BinanceAccountType.USD_M_FUTURE, handler=handler, loop=asyncio.get_event_loop())
    await binance_client.connect()
    await asyncio.sleep(5)
    binance_client.subscribe_book_ticker(["BTCUSDT"])

    while True:
        await asyncio.sleep(1)


if __name__ == "__main__":
    asyncio.run(main())
