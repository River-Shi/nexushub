import asyncio
import grpc
import statistics

from nexushub.utils import Log, LiveClock
from nexushub.dispatcher import marketdata_pb2
from nexushub.dispatcher import marketdata_pb2_grpc

logger = Log.get_logger()
clock = LiveClock()

latency_list = []

async def run_subscription(stub: marketdata_pb2_grpc.MarketDataStreamerStub, event_type: str, symbols: list[str], interval: str | None = None):
    """Runs a single subscription stream."""
    request = marketdata_pb2.SubscriptionRequest(
        event_type=event_type,
        symbols=symbols,
        interval=interval # Will be None if not provided or not needed
    )

    try:
        stream = stub.SubscribeMarketData(request)
        async for response in stream:
            local = clock.timestamp_ns()
            latency = local - response.kline.timestamp
            latency_list.append(latency)

    except grpc.aio.AioRpcError as e:
        logger.error(f"gRPC Error for subscription: {e.code()} - {e.details()}")
    except Exception as e:
        logger.error(f"An unexpected error occurred during subscription: {e}")

async def main():
    """Connects to the server and starts example subscriptions."""
    # Standard gRPC keepalive options (optional but recommended)
    channel_options = [
        ('grpc.keepalive_time_ms', 10000),
        ('grpc.keepalive_timeout_ms', 5000),
        ('grpc.keepalive_permit_without_calls', True),
        ('grpc.http2.min_time_between_pings_ms', 10000),
        ('grpc.http2.min_ping_interval_without_data_ms', 5000),
    ]
    server_address = 'localhost:50051'
    logger.info(f"Connecting to gRPC server at {server_address}...")
    try:
        async with grpc.aio.insecure_channel(server_address, options=channel_options) as channel:
            stub = marketdata_pb2_grpc.MarketDataStreamerStub(channel)
            await run_subscription(stub, "kline", ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT"], "1m")
    except asyncio.CancelledError:
        pass
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
    finally:
        avg = statistics.mean(latency_list)
        min_value = min(latency_list)
        max_value = max(latency_list)
        median = statistics.median(latency_list)
        print(f"Latency: {avg} ns, min: {min_value} ns, max: {max_value} ns, median: {median} ns")
        

if __name__ == '__main__':
    asyncio.run(main())
