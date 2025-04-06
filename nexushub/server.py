from nexushub.dispatcher import marketdata_pb2, marketdata_pb2_grpc
from nexushub.binance import BinanceWSClient, BinanceAccountType, BinanceKlineInterval
from nexushub.utils import Log, LiveClock

import asyncio
import grpc
import cysimdjson
from collections import defaultdict
from typing import Dict, Set


logger = Log.get_logger()  # Assuming Log is a custom logger utility
# Basic logging config assumed to be set elsewhere or in main block

# Shared state for subscriptions
# Key: actual_binance_stream_name (e.g., "btcusdt@kline_1m")
# Value: Set of asyncio.Queue objects for clients subscribed to this stream
STREAM_SUBSCRIBERS: Dict[str, Set[asyncio.Queue]] = defaultdict(set)

# Lock to protect access to STREAM_SUBSCRIBERS
SUBSCRIPTIONS_LOCK = asyncio.Lock()

# --- Parsing Functions (Keep these as they are needed) ---
# parse_kline_data, parse_trade_data, parse_agg_trade_data, parse_book_ticker_data
# (Assuming these functions exist as defined previously)
# --- Add the parsing functions from the previous example here ---

parser = cysimdjson.JSONParser()
clock = LiveClock()

def parse_kline_data(message, symbol, interval) -> marketdata_pb2.Kline:
    return marketdata_pb2.Kline(
        event_type=message.at_pointer("/e"),
        event_time=message.at_pointer("/E"),
        start_time=message.at_pointer("/k/t"),
        close_time=message.at_pointer("/k/T"),
        symbol=symbol, # Use override if provided (e.g. from stream name)
        interval=interval,
        first_trade_id=message.at_pointer("/k/f"),
        last_trade_id=message.at_pointer("/k/L"),
        open_price=message.at_pointer("/k/o"),
        close_price=message.at_pointer("/k/c"),
        high_price=message.at_pointer("/k/h"),
        low_price=message.at_pointer("/k/l"),
        volume=message.at_pointer("/k/v"),
        number_of_trades=message.at_pointer("/k/n"),
        is_closed=message.at_pointer("/k/x"),
        quote_asset_volume=message.at_pointer("/k/q"),
        taker_buy_volume=message.at_pointer("/k/V"),
        taker_buy_quote_volume=message.at_pointer("/k/Q"),
        timestamp=clock.timestamp_ns()
    )

def parse_trade_data(message, symbol) -> marketdata_pb2.Trade:
    pass

def parse_agg_trade_data(message, symbol) -> marketdata_pb2.AggTrade:
    pass

def parse_book_ticker_data(message, symbol) -> marketdata_pb2.BookTicker:
    pass


class MarketDataStreamerServicer(marketdata_pb2_grpc.MarketDataStreamerServicer):

    def __init__(self, binance_client: BinanceWSClient):
        self.binance_client = binance_client
          # Assuming cysimdjson is used for JSON parsing

    def _binance_data_handler(self, raw: bytes):
        """
        Callback passed to BinanceWSClient. Parses data and dispatches
        to relevant client queues based on the actual stream name.
        (This function remains largely unchanged as its core logic is dispatching).
        """
        # Parse the raw data using cysimdjson
        message = parser.parse(raw)
        
        try:
            event_type = message.at_pointer("/e")
            symbol = message.at_pointer("/s")
            
            if event_type == "kline":
                interval = message.at_pointer("/k/i")
                stream_name = f"{symbol}@kline_{interval}"
            else:
                stream_name = f"{symbol}@{event_type}"
        except KeyError:
            logger.error("Error parsing message: Key not found in the message.")
            return


        logger.debug(f"Handler received: {stream_name}")

        response = marketdata_pb2.MarketDataResponse(stream_name=stream_name)

        parsed = False
        # --- Parsing Logic (same as before) ---
        if event_type == 'kline':
            kline_obj = parse_kline_data(message, symbol, interval)
            response.kline.CopyFrom(kline_obj)
            parsed = True
        elif event_type == 'trade':
            pass
        elif event_type == 'aggTrade':
            pass
        elif event_type == 'bookTicker':
            pass
        else:
            logger.warning(f"Unhandled event type '{event_type}' on stream {stream_name}")
            return

        if not parsed:
            logger.warning(f"Failed to parse '{event_type}' on stream {stream_name}")
            return


        if stream_name in STREAM_SUBSCRIBERS:
            subscribers = STREAM_SUBSCRIBERS[stream_name] 
            # Use list() for safe iteration if needed, though modification during iteration is less likely here
            for queue in subscribers:
                try:
                    # Consider put_nowait if dropping data for slow clients is acceptable
                    queue.put_nowait(response)
                except asyncio.QueueFull:
                    logger.warning(f"Queue full for a subscriber of {stream_name}. Data lost.")
                except Exception as e:
                    logger.error(f"Error putting data onto queue for {stream_name}: {e}")
    
    def _build_subscribption_streams(self, event_type, symbols, interval=None):
        if event_type == "kline":
            return [f"{symbol}@kline_{interval}" for symbol in symbols]
        else:
            return [f"{symbol}@{event_type}" for symbol in symbols]


    async def SubscribeMarketData(
        self,
        request: marketdata_pb2.SubscriptionRequest,
        context: grpc.aio.ServicerContext
    ):
        """
        Handles client subscription requests. Subscribes to Binance streams
        if not already subscribed by the client, adds the client's queue
        to the listeners for those streams, and streams data back.
        Does NOT handle unsubscribing from Binance.
        """
        client_id = context.peer()
        event_type = request.event_type
        req_symbols = list(request.symbols)
        req_interval = request.interval if request.HasField("interval") else None

        # --- Request Validation (same as before) ---
        if not req_symbols:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Symbols list cannot be empty.")
        if event_type == "kline" and not req_interval:
             await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Interval is required for `kline` streams.")
        # --- End Validation ---

        logger.debug(f"Client {client_id} requesting type='{event_type}', symbols={req_symbols}, interval='{req_interval}'")

        client_queue = asyncio.Queue(maxsize=200)
        # Keep track of the actual Binance streams this specific client request maps to,
        # needed only for cleaning up this client's queue from STREAM_SUBSCRIBERS on disconnect.
        try:
            # --- Logic to determine subscribe method and build stream names (same as before) ---
            
            subscribe_streams = self._build_subscribption_streams(event_type, req_symbols, req_interval)
            
            if event_type == "kline":
                kline_interval = BinanceKlineInterval(req_interval) # Assume constructor
                await self.binance_client.subscribe_kline(req_symbols, kline_interval)
            elif event_type == "trade":
                await self.binance_client.subscribe_trade(req_symbols)
            elif event_type == "aggTrade":
                await self.binance_client.subscribe_agg_trade(req_symbols)
            elif event_type == "bookTicker":
                await self.binance_client.subscribe_book_ticker(req_symbols)
            else:
                await context.abort(grpc.StatusCode.INVALID_ARGUMENT, f"Unsupported event_type: {event_type}")
            # --- End Subscribe Method Logic ---
            async with SUBSCRIPTIONS_LOCK:
                for stream_name in subscribe_streams:
                    STREAM_SUBSCRIBERS[stream_name].add(client_queue)

            # 4. Stream data from queue to client
            while True: # Check context activity more proactively
                try:
                    data = await client_queue.get()
                    yield data
                    client_queue.task_done()
                except asyncio.CancelledError:
                    logger.info(f"Subscription task cancelled for client {client_id}.")
                    raise # Propagate cancellation
                except Exception as e:
                    logger.error(f"Error in client {client_id} queue loop: {e}")
                    break # Exit loop

        except grpc.aio.AioRpcError as e:
             # Common if client disconnects uncleanly
             logger.warning(f"gRPC Error for client {client_id}: {e.code()} - {e.details()}")
        except Exception as e:
            # Catch broader exceptions during setup/loop
            logger.error(f"Unexpected error in SubscribeMarketData for {client_id}: {e}")
        finally:
            # Simplified Cleanup: Only remove this client's queue from the listener sets.
            # No attempt to unsubscribe from Binance.
            logger.info(f"Cleaning up listeners for disconnected client {client_id}")
            async with SUBSCRIPTIONS_LOCK:
                for stream_name in subscribe_streams:
                    if stream_name in STREAM_SUBSCRIBERS:
                        STREAM_SUBSCRIBERS[stream_name].discard(client_queue)
                        logger.debug(f"Removed client {client_id} queue from stream {stream_name}")
            logger.info(f"Listener cleanup finished for client {client_id}")
            
"""
client A: BTCUSDT@kline_1m, ETHUSDT@kline_1m, SOLUSDT@kline_1m, XRPUSDT@kline_1m
client B: BTCUSDT@kline_1m, ETHUSDT@kline_1m
client C: BTCUSDT@kline_1m

BTCUSDT@kline_1m -> client A, client B, client C
ETHUSDT@kline_1m -> client A, client B
SOLUSDT@kline_1m -> client A
"""


# --- Main Server Function (serve()) would remain similar ---
# It needs to initialize BinanceWSClient, the Servicer, set the handler,
# start the client and server, and handle shutdown.
# The shutdown logic might skip calling binance_client.stop() if you
# intend the underlying connections to persist even after gRPC server stops.
# However, it's generally better practice to stop the client too.

async def serve():
    loop = asyncio.get_running_loop()

    # Initialize Binance Client - Handler set later
    binance_client = BinanceWSClient(
        account_type=BinanceAccountType.USD_M_FUTURE, # Use your actual type
        handler=None,
        loop=loop
    )

    # Initialize Servicer
    servicer = MarketDataStreamerServicer(binance_client)

    # Set the callback in the client
    binance_client._callback = servicer._binance_data_handler

    # Start Binance client's connection/receiver loop (if needed)
    # await binance_client.start() # Optional, depends on client design

    # Start gRPC server
    server = grpc.aio.server()
    marketdata_pb2_grpc.add_MarketDataStreamerServicer_to_server(servicer, server)
    listen_addr = '[::]:50051'
    server.add_insecure_port(listen_addr)

    logger.debug(f"Starting gRPC server on {listen_addr}")
    await server.start()
    logger.debug("gRPC server started.")

    # Run until interrupted
    try:
        await server.wait_for_termination()
    except asyncio.CancelledError:
        pass
    finally:
        await server.stop(grace=5.0)
        binance_client.disconnect()
        logger.debug("Shutting down complete.")


if __name__ == '__main__':
    asyncio.run(serve())
