import asyncio
from typing import Callable, List
from typing import Any
from aiolimiter import AsyncLimiter


from nexushub.ws_client import WSClient
from nexushub.constants import BinanceAccountType, BinanceKlineInterval


class BinanceWSClient(WSClient):
    def __init__(
        self,
        account_type: BinanceAccountType,
        handler: Callable[..., Any],
        loop: asyncio.AbstractEventLoop,
        callback_args: tuple | None = None,
        callback_kwargs: dict | None = None,
    ):
        self._account_type = account_type
        url = account_type.ws_url
        super().__init__(
            url,
            limiter=AsyncLimiter(max_rate=4, time_period=1),
            handler=handler,
            loop=loop,
            ping_idle_timeout=10,
            ping_reply_timeout=5,
            callback_args=callback_args,
            callback_kwargs=callback_kwargs,
        )
    
    def _send_payload(self, params: List[str], chunk_size: int = 100, method: str = "SUBSCRIBE"):
        # Split params into chunks of 100 if length exceeds 100
        params_chunks = [
            params[i:i + chunk_size] 
            for i in range(0, len(params), chunk_size)
        ]
        
        for chunk in params_chunks:
            payload = {
                "method": method,
                "params": chunk,
                "id": self._clock.timestamp_ms(),
            }
            self._send(payload)

    def _subscribe(self, params: List[str]):
        params = [param for param in params if param not in self._subscriptions]
        
        for param in params:
            self._subscriptions.append(param)
            self._log.debug(f"Subscribing to {param}...")
        
        self._send_payload(params)
    
    def _unsubscribe(self, params: List[str]):
        for param in params:
            self._subscriptions.remove(param)
            self._log.debug(f"Unsubscribing from {param}...")
        self._send_payload(params, method="UNSUBSCRIBE")

    def subscribe_agg_trade(self, symbols: List[str]):
        params = [f"{symbol.lower()}@aggTrade" for symbol in symbols]
        self._subscribe(params)

    def subscribe_trade(self, symbols: List[str]):
        params = [f"{symbol.lower()}@trade" for symbol in symbols]
        self._subscribe(params)

    def subscribe_book_ticker(self, symbols: List[str]):
        params = [f"{symbol.lower()}@bookTicker" for symbol in symbols]
        self._subscribe(params)

    def subscribe_kline(
        self,
        symbols: List[str],
        interval: BinanceKlineInterval,
    ):
        params = [f"{symbol.lower()}@kline_{interval.value}" for symbol in symbols]
        self._subscribe(params)

    async def _resubscribe(self):
        self._send_payload(self._subscriptions)
