import asyncio
import msgspec
from nexushub.constants import BinanceAccountType, BinanceKlineInterval
from nexushub.schema import BinanceUMKlineWsResponse
from nexushub.binance import BinanceUMApiClient, BinanceWSClient, BinanceApiClient

decoder = msgspec.json.Decoder(BinanceUMKlineWsResponse)

def handler(msg):
    print(decoder.decode(msg).data.parse_kline())

async def main():
    try:
        api_client = BinanceUMApiClient()
        info = await api_client.exchange_info()
            
        
        client = BinanceWSClient(
            account_type=BinanceAccountType.USD_M_FUTURE,
            handler=handler,
            loop=asyncio.get_event_loop(),
        )
        await client.connect()
        client.subscribe_kline(symbols=info.active_symbols, interval=BinanceKlineInterval.HOUR_1)
        
        
        while True:
            await asyncio.sleep(1)
    except asyncio.CancelledError:
        pass

if __name__ == "__main__":
    asyncio.run(main())



