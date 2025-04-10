import msgspec
import asyncio
import cysimdjson
from typing import Optional, Dict, List
from nexushub.constants import SubscriptionRequest, BinanceKlineInterval
from nexushub.utils import Log
from weakref import ref, ReferenceType
from uuid import uuid4
from collections import defaultdict
from typing import Set
import argparse
from picows import (
    ws_create_server,
    WSFrame,
    WSTransport,
    WSListener,
    WSMsgType,
    WSUpgradeRequest,
)

from nexushub.binance import BinanceWSClient, BinanceAccountType

parser = argparse.ArgumentParser(description='NexusHub WebSocket Server')
parser.add_argument('--host', type=str, default='127.0.0.1', help='Host address')
parser.add_argument('--port', type=int, default=9001, help='Port number')
parser.add_argument('--log-level', type=str, default='INFO', help='Log level')
args = parser.parse_args()

Log.setup_logger(log_path="./logs", log_level=args.log_level)

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
        # {client_id: client_ref}
        # client_id is a uuid4
        # client_ref is a weak reference to the client
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
            if path not in ['/spot', '/linear', '/inverse']:
                self._logger.error(f"Invalid path: {r.path}")
                return None
            self._logger.info(f"Client connected: {r.path}")
            return ServerClientListener(
                self._logger,
                self._all_clients,
                self._streams_subscribed_map[path],
                self._binance_clients[path],
            )

        self._asyncio_server = await ws_create_server(
            listener_factory, host, port
        )
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
