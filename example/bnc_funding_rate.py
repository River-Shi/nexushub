from nexushub.binance import BinanceUMApiClient
from nexushub.utils import Log
from pathlib import Path
import asyncio
import pandas as pd
Log.setup_logger()
log = Log.get_logger()

folder_path = Path("/share/binance_data/futures/um/daily/funding_rate")
folder_path.mkdir(parents=True, exist_ok=True)

async def process_symbol(client, symbol_info):
    symbol = symbol_info.symbol
    parquet_path = folder_path / f"{symbol}.parquet"
    
    try:
        if parquet_path.exists():
            startTime = int(pd.read_parquet(parquet_path).iloc[-1].timestamp.timestamp() * 1000) + 1
        else:
            startTime = symbol_info.onboardDate
        
        funding_rate = await client.funding_rate_history_data(symbol=symbol, startTime=startTime)
        
        if parquet_path.exists():
            df = pd.concat([pd.read_parquet(parquet_path), funding_rate.df])
        else:
            df = funding_rate.df
        
        if df is not None:
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
