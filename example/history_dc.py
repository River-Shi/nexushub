import asyncio
import argparse
from nexushub.server import HistoryServer
from nexushub.constants import BinanceKlineInterval

parser = argparse.ArgumentParser()
parser.add_argument("--freq", type=str, default=BinanceKlineInterval.HOUR_1.value)
parser.add_argument("--init_days", type=int, default=120)
parser.add_argument("--save_dir", type=str, default="data")


async def main():
    try:
        args = parser.parse_args()
        server = HistoryServer(
            save_dir=args.save_dir,
            freq=BinanceKlineInterval(args.freq),
            init_days=args.init_days,
        )
        await server.start()
    except asyncio.CancelledError:
        pass
    finally:
        await server.stop()


if __name__ == "__main__":
    asyncio.run(main())
