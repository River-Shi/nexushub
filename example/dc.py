import argparse
import asyncio
from nexushub.server import Server
from nexushub.utils import Log


parser = argparse.ArgumentParser(description="NexusHub WebSocket Server")
parser.add_argument("--host", type=str, default="127.0.0.1", help="Host address")
parser.add_argument("--port", type=int, default=9001, help="Port number")
parser.add_argument("--log-level", type=str, default="INFO", help="Log level")
args = parser.parse_args()

Log.setup_logger(log_path="./logs", log_level=args.log_level)


async def main():
    try:
        server = Server()
        await server.start(host=args.host, port=args.port)
    except asyncio.CancelledError:
        pass
    finally:
        await server.stop()


if __name__ == "__main__":
    asyncio.run(main())
