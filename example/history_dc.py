import asyncio
import argparse
import os
from nexushub.utils import Log
from nexushub.server import HistoryServer
from nexushub.constants import BinanceKlineInterval


TS_DB_NAME = os.getenv("TS_DB_NAME")
TS_DB_USER = os.getenv("TS_DB_USER")
TS_DB_PASSWORD = os.getenv("TS_DB_PASSWORD")
TS_DB_PORT = int(os.getenv("TS_DB_PORT"))


parser = argparse.ArgumentParser()
parser.add_argument("--freq", type=str, default=BinanceKlineInterval.HOUR_1.value)
parser.add_argument("--init_days", type=int, default=120)
parser.add_argument("--redownload", type=bool, default=False)
parser.add_argument("--log-level", type=str, default="INFO")

async def main():
    try:
        args = parser.parse_args()
        Log.setup_logger(log_level=args.log_level)
        server = HistoryServer(
            db_user_name=TS_DB_USER,
            db_password=TS_DB_PASSWORD,
            db_host="127.0.0.1",
            db_port=TS_DB_PORT,
            db_name=TS_DB_NAME,
            freq=BinanceKlineInterval(args.freq),
            init_days=args.init_days,
            redownload=args.redownload,
        )
        await server.start()
    except asyncio.CancelledError:
        pass
    finally:
        await server.stop()


if __name__ == "__main__":
    asyncio.run(main())
