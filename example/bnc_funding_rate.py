from nexushub.binance import BinanceUMApiClient
from nexushub.utils import Log
from pathlib import Path
import asyncio

Log.setup_logger()
log = Log.get_logger()

folder_path = Path("/share/binance_data/futures/um/daily/funding_rate")
folder_path.mkdir(parents=True, exist_ok=True)

async def process_symbol(client, symbol_info):
    symbol = symbol_info.symbol
    parquet_path = folder_path / f"{symbol}.parquet"
    
    if parquet_path.exists():
        return
    
    try:
        startTime = symbol_info.onboardDate
        funding_rate = await client.funding_rate_history_data(symbol=symbol, startTime=startTime)
        
        df = funding_rate.df
        df.to_parquet(parquet_path)
        log.info(f"Saved {symbol} to {parquet_path}")
    except Exception as e:
        log.error(f"Error processing {symbol}: {e}")

async def main():
    client = BinanceUMApiClient(timeout=10, max_rate=500, period=60 * 5)
    
    try:
        info = await client.exchange_info()
        
        # Create tasks for all symbols
        tasks = [
            process_symbol(client, symbol_info)
            for symbol_info in info.symbols
        ]
        
        # Run tasks concurrently
        await asyncio.gather(*tasks)
        
    finally:
        await client.close_session()

if __name__ == "__main__":
    asyncio.run(main())
