import msgspec
import asyncio
import cysimdjson
import pandas as pd
import pathlib
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from typing import Optional, Dict, List
from nexushub.constants import BinanceKlineInterval
from nexushub.schema import SubscriptionRequest
from nexushub.utils import Log
from weakref import ref, ReferenceType
from uuid import uuid4
from collections import defaultdict
from typing import Set
from picows import (
    ws_create_server,
    WSFrame,
    WSTransport,
    WSListener,
    WSMsgType,
    WSUpgradeRequest,
)
from nexushub.binance import BinanceWSClient, BinanceAccountType, BinanceUMApiClient
from nexushub.utils import LiveClock


class ServerClientListener(WSListener):
    def __init__(
        self,
        logger,
        all_clients: Dict[str, ReferenceType["ServerClientListener"]],
        streams_subscribed: Dict[str, Set[str]],
        binance_client: BinanceWSClient,
    ):
        self.transport = None
        self._client_id = str(uuid4())
        self._all_clients = all_clients
        self._streams_subscribed = streams_subscribed
        self._binance_client = binance_client
        self._logger = logger

    def on_ws_connected(self, transport: WSTransport):
        self.transport = transport
        self._all_clients[self._client_id] = ref(self)
        self._logger.info(f"Client {self._client_id} connected")

    def on_ws_disconnected(self, transport: WSTransport):
        stream_to_unsubscribe = []
        self._all_clients.pop(self._client_id)

        for stream in self._streams_subscribed:
            # Remove this client from the subscribers set for this stream
            self._streams_subscribed[stream].discard(self._client_id)
            # If no clients are subscribed to this stream anymore, we might want to unsubscribe from Binance
            if not self._streams_subscribed[stream]:
                self._logger.debug(f"Unsubscribe: {stream}")
                stream_to_unsubscribe.append(stream)

        self._binance_client._unsubscribe(stream_to_unsubscribe)

        for stream in stream_to_unsubscribe:
            self._streams_subscribed.pop(stream)

        self._logger.info(f"Client {self._client_id} disconnected")

    def on_ws_frame(self, transport: WSTransport, frame: WSFrame):
        if frame.msg_type == WSMsgType.CLOSE:
            transport.send_close(frame.get_close_code(), frame.get_close_message())
            transport.disconnect()
        elif frame.msg_type == WSMsgType.TEXT:
            try:
                sub_req = msgspec.json.decode(
                    frame.get_payload_as_bytes(), type=SubscriptionRequest
                )

                streams = sub_req.params

                for stream in streams:
                    self._streams_subscribed[stream].add(self._client_id)
                    self._logger.debug(f"Subscribed to {stream}")

                self._binance_client._subscribe(streams)

            except msgspec.DecodeError:
                self._logger.error("Invalid subscription request")

    def _build_subscribption_streams(
        self,
        event_type: str,
        symbols: List[str],
        interval: BinanceKlineInterval | None = None,
    ):
        if event_type == "kline":
            return [f"{symbol.lower()}@kline_{interval.value}" for symbol in symbols]
        else:
            return [f"{symbol.lower()}@{event_type}" for symbol in symbols]


class Server:
    _all_clients: Dict[str, ReferenceType[ServerClientListener]]
    _asyncio_server: Optional[asyncio.Server]

    def __init__(self):
        self._all_clients = {}
        self._asyncio_server = None
        self._streams_subscribed_map = {
            "/spot": defaultdict(set),
            "/linear": defaultdict(set),
            "/inverse": defaultdict(set),
        }

        self._binance_clients = {
            "/spot": BinanceWSClient(
                BinanceAccountType.SPOT,
                handler=self._handler,
                loop=asyncio.get_event_loop(),
                callback_kwargs={"path": "/spot"},
            ),
            "/linear": BinanceWSClient(
                BinanceAccountType.USD_M_FUTURE,
                handler=self._handler,
                loop=asyncio.get_event_loop(),
                callback_kwargs={"path": "/linear"},
            ),
            "/inverse": BinanceWSClient(
                BinanceAccountType.COIN_M_FUTURE,
                handler=self._handler,
                loop=asyncio.get_event_loop(),
                callback_kwargs={"path": "/inverse"},
            ),
        }
        self._parser = cysimdjson.JSONParser()
        self._logger = Log.get_logger()

    def _handler(self, raw: bytes, path: str):
        message = self._parser.parse(raw)

        try:
            stream = message.at_pointer("/stream")
        except KeyError:
            id = message.at_pointer("/id")
            self._logger.debug(f"id: {id}")
            return

        streams_subscribed = self._streams_subscribed_map[path]
        if stream in streams_subscribed:
            for client_id in streams_subscribed[stream]:
                client = self._all_clients.get(client_id)

                if client:
                    client_ref = client()
                    try:
                        client_ref.transport.send(WSMsgType.TEXT, raw)
                    except (ConnectionError, BrokenPipeError):
                        pass

    async def start(self, host: str = "127.0.0.1", port: int = 9001):
        def listener_factory(r: WSUpgradeRequest):
            path = r.path.decode()
            if path not in ["/spot", "/linear", "/inverse"]:
                self._logger.error(f"Invalid path: {path}")
                return None
            self._logger.info(f"Client connected: {path}")
            return ServerClientListener(
                self._logger,
                self._all_clients,
                self._streams_subscribed_map[path],
                self._binance_clients[path],
            )

        self._asyncio_server = await ws_create_server(listener_factory, host, port)
        for s in self._asyncio_server.sockets:
            self._logger.info(f"Server started on {s.getsockname()}")

        for client in self._binance_clients.values():
            await client.connect()

        await self._asyncio_server.serve_forever()

    async def stop(self):
        for client in self._all_clients.values():
            client_ref = client()
            client_ref.transport.send_close(1000, b"Server is shutting down")

        for ws_client in self._binance_clients.values():
            ws_client.disconnect()

        self._asyncio_server.close()
        self._logger.info("Server stopped")


class HistoryServer:
    """
    1s 20 rate limit
    """

    def __init__(
        self,
        save_dir: str,
        freq: BinanceKlineInterval,
        init_days: int,
        redownload: bool = False,
    ):
        self._rate = 2400 / 60 / 2
        self._logger = Log.get_logger()
        self._api = BinanceUMApiClient()
        self._freq = freq
        self._save_dir = pathlib.Path(save_dir) / f"{freq.value}"
        self._save_dir.mkdir(parents=True, exist_ok=True)

        self._init_days = init_days
        self._clock = LiveClock()
        self._scheduler = AsyncIOScheduler()
        self._event = asyncio.Event()
        self._redownload = redownload

    def _check_rate_limit(self, symbols: List[str]):
        num_symbols = len(symbols)
        request_per_sec = self._rate  # max request can be made per second
        init_days_2_sec = self._init_days * 24 * 60 * 60  # total seconds to download
        per_request_sec = self._freq.seconds * 499  # one request seconds

        num_requests_needed = init_days_2_sec / per_request_sec * num_symbols
        sec_requests_takes = num_requests_needed / request_per_sec

        if sec_requests_takes * 2 > self._freq.seconds:
            raise RuntimeError(
                f"Not recommended to download freq: {self._freq.value} for {self._init_days} days. Please reduce the `init_days` or choose a higher `frequency`."
            )

    async def _download_symbol(self, symbol: str):
        file_path = self._save_dir / f"{symbol}.parquet"

        if file_path.exists() and not self._redownload:
            df = pd.read_parquet(file_path)
            start_time = df['open_time'].values[-1] + 1
            klines = await self._api.kline_candlestick_data(
                symbol,
                self._freq,
                start_time=start_time,
                include_unconfirmed=False,
                limit=99,
            )
            new_df = klines.df
            if new_df is None:
                return
            df = pd.concat([df, new_df])
            df.to_parquet(file_path)
        else:
            start_time = (
                self._clock.timestamp_ms() - 1000 * 60 * 60 * 24 * self._init_days
            )
            klines = await self._api.kline_candlestick_data(
                symbol, self._freq, start_time=start_time, include_unconfirmed=False
            )
            df = klines.df
            if df is None:
                return
            df.to_parquet(file_path)

    async def update(self, symbols: List[str]):
        tasks = []
        for symbol in symbols:
            tasks.append(self._download_symbol(symbol))

        await asyncio.gather(*tasks)
        self._logger.info(f"Updated {len(symbols)} symbols")

    async def start(self):
        try:
            trigger_map = {
                BinanceKlineInterval.MINUTE_1: {"second": "1", "minute": "*"},
                BinanceKlineInterval.MINUTE_3: {"second": "1", "minute": "*/3"},
                BinanceKlineInterval.MINUTE_5: {"second": "1", "minute": "*/5"},
                BinanceKlineInterval.MINUTE_15: {"second": "1", "minute": "*/15"},
                BinanceKlineInterval.MINUTE_30: {"minute": "*/30"},
                BinanceKlineInterval.HOUR_1: {"hour": "*", "minute": "0", "second": "1"},
                BinanceKlineInterval.HOUR_4: {"hour": "*/4", "minute": "0", "second": "1"},
                BinanceKlineInterval.HOUR_8: {"hour": "*/8", "minute": "0", "second": "1"},
                BinanceKlineInterval.HOUR_12: {"hour": "*/12", "minute": "0", "second": "1"},
                BinanceKlineInterval.DAY_1: {"hour": "0", "minute": "0", "second": "1"},
            }

            info = await self._api.exchange_info()
            symbols = info.active_symbols

            self._logger.info(f"Start downloading {len(symbols)} symbols")

            self._check_rate_limit(symbols)

            await self.update(symbols)

            self._scheduler.add_job(
                self.update,
                "cron",
                **trigger_map[self._freq],
                kwargs={"symbols": symbols},
            )
            self._scheduler.start()

            await self._event.wait()
        finally:
            await self.stop()

    async def stop(self):
        await self._api.close_session()
        self._event.set()
