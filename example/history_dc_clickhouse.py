import asyncio
import argparse
import os
from nexushub.utils import Log
from nexushub.server import ClickHouseServer
from nexushub.constants import BinanceKlineInterval


CLICKHOUSE_DB = os.getenv("CLICKHOUSE_DB")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD")
CLICKHOUSE_HOST = "localhost"
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT"))


parser = argparse.ArgumentParser()
parser.add_argument("--freq", type=str, default=BinanceKlineInterval.HOUR_1.value)
parser.add_argument("--init_days", type=int, default=120)
parser.add_argument("--redownload", type=bool, default=False)
parser.add_argument("--log-level", type=str, default="INFO")
parser.add_argument("--update_symbol", type=bool, default=True)

async def main():
    try:
        args = parser.parse_args()
        Log.setup_logger(log_level=args.log_level)
        server = ClickHouseServer(
            db_user_name=CLICKHOUSE_USER,
            db_password=CLICKHOUSE_PASSWORD,
            db_host=CLICKHOUSE_HOST,
            db_port=CLICKHOUSE_PORT,
            db_name=CLICKHOUSE_DB,
            freq=BinanceKlineInterval(args.freq),
            init_days=args.init_days,
            redownload=args.redownload,
            update_symbol=args.update_symbol,
        )
        await server.start()
    except asyncio.CancelledError:
        pass
    finally:
        await server.stop()


if __name__ == "__main__":
    asyncio.run(main())
