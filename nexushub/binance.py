import asyncio
import msgspec
import aiohttp
import sys
import tenacity
from typing import Callable, List
from typing import Any
from aiolimiter import AsyncLimiter

from urllib.parse import urlencode, urljoin
from typing import Dict
from nexushub.schema import (
    BinanceUMExchangeInfoResponse,
    BinanceUMKlineResponse,
    BinanceUMKline,
)
from nexushub.constants import BinanceKlineInterval, BinanceAccountType
from nexushub.ws_client import WSClient
from nexushub.rest_api import ApiClient


class BinanceError(Exception):
    """
    The base class for all Binance specific errors.
    """

    def __init__(self, status, message, headers):
        super().__init__(message)
        self.status = status
        self.message = message
        self.headers = headers


class BinanceServerError(BinanceError):
    """
    Represents an Binance specific 500 series HTTP error.
    """

    def __init__(self, status, message, headers):
        super().__init__(status, message, headers)


class BinanceClientError(BinanceError):
    """
    Represents an Binance specific 400 series HTTP error.
    """

    def __init__(self, status, message, headers):
        super().__init__(status, message, headers)


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

    def _send_payload(
        self, params: List[str], chunk_size: int = 100, method: str = "SUBSCRIBE"
    ):
        # Split params into chunks of 100 if length exceeds 100
        params_chunks = [
            params[i : i + chunk_size] for i in range(0, len(params), chunk_size)
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


class BinanceApiClient(ApiClient):
    def __init__(
        self,
        base_url: str,
        timeout: int = 10,
        max_rate: int | None = None,    
        period: float = 60,
    ):
        super().__init__(
            timeout=timeout,
            max_rate=max_rate,
            period=period,
        )
        self._headers = {
            "Content-Type": "application/json",
            "User-Agent": "TradingBot/1.0",
        }

        self.base_url = base_url

    def _prepare_payload(self, payload: Dict[str, Any]) -> str:
        """Prepare payload by encoding and optionally signing."""
        payload = {
            k: str(v).lower() if isinstance(v, bool) else v for k, v in payload.items()
        }
        return urlencode(payload)

    @tenacity.retry(
        stop=tenacity.stop_after_attempt(3),
        wait=tenacity.wait_exponential(multiplier=1, min=4, max=15),
    )
    async def _fetch(self, method: str, endpoint: str, payload: Dict[str, Any] = None, weight: int = 1):
        """Make an asynchronous HTTP request."""
        if self._limiter:
            await self._limiter.acquire(weight=weight)

        self._init_session()

        url = urljoin(self.base_url, endpoint)
        payload = payload or {}
        encoded_payload = self._prepare_payload(payload)

        if method.upper() == "GET":
            url = f"{url}?{encoded_payload}"
            data = None
        else:
            data = encoded_payload

        self._log.debug(f"Request: {method} {url}")

        try:
            response = await self._session.request(
                method=method,
                url=url,
                headers=self._headers,
                data=data,
            )
            status = response.status
            raw = await response.read()
            if 400 <= status < 500:
                text = await response.text()
                raise BinanceClientError(status, text, self._headers)
            elif status >= 500:
                text = await response.text()
                raise BinanceServerError(status, text, self._headers)
            return raw

        except aiohttp.ClientError as e:
            self._log.error(f"Client Error {method} Url: {url} {e}")
            raise
        except asyncio.TimeoutError:
            self._log.error(f"Timeout {method} Url: {url}")
            raise
        except Exception as e:
            self._log.error(f"Error {method} Url: {url} {e}")
            raise


class BinanceUMApiClient(BinanceApiClient):
    def __init__(
        self,
        timeout: int = 10,
        max_rate: int = 2400,
        period: float = 60,
    ):
        super().__init__(
            base_url="https://fapi.binance.com",
            timeout=timeout,
            max_rate=max_rate,
            period=period,
        )

        self._exchange_info_decoder = msgspec.json.Decoder(
            BinanceUMExchangeInfoResponse
        )
        self._kline_decoder = msgspec.json.Decoder(list[BinanceUMKlineResponse])

    async def exchange_info(self) -> BinanceUMExchangeInfoResponse:
        path = "/fapi/v1/exchangeInfo"
        raw = await self._fetch("GET", path)
        return self._exchange_info_decoder.decode(raw)

    async def get_api_fapi_v1_klines(
        self,
        symbol: str,
        interval: str,
        limit: int | None = None,
        startTime: int | None = None,
        endTime: int | None = None,
    ) -> list[BinanceUMKlineResponse]:
        path = "/fapi/v1/klines"
        payload = {
            "symbol": symbol,
            "interval": interval,
            "limit": limit,
            "startTime": startTime,
            "endTime": endTime,
        }
        weight = (
            5 if limit is None
            else 1 if limit < 100
            else 2 if limit < 500
            else 5 if limit < 1000
            else 10
        )
        
        payload = {k: v for k, v in payload.items() if v is not None}
        raw = await self._fetch("GET", path, payload, weight)
        return self._kline_decoder.decode(raw)

    async def kline_candlestick_data(
        self,
        symbol,
        interval: BinanceKlineInterval,
        start_time: int | None = None,
        end_time: int | None = None,
        limit: int | None = None,
        include_unconfirmed: bool = False,
    ) -> BinanceUMKline:
        end_time_ms = int(end_time) if end_time is not None else sys.maxsize
        limit = (
            int(limit) if limit is not None else 499
        )  # NOTE: 499 only takes 2 weight
        all_klines: list[BinanceUMKlineResponse] = []
        while True:
            klines: list[BinanceUMKlineResponse] = await self.get_api_fapi_v1_klines(
                symbol=symbol,
                interval=interval.value,
                limit=limit,
                startTime=start_time,
                endTime=end_time,
            )

            all_klines.extend(klines)

            # Update the start_time to fetch the next set of bars
            if klines:
                next_start_time = klines[-1].open_time + 1
            else:
                # Handle the case when klines is empty
                break

            # No more bars to fetch
            if (limit and len(klines) < limit) or next_start_time >= end_time_ms:
                break

            start_time = next_start_time

        return BinanceUMKline(symbol, all_klines, include_unconfirmed)

async def main():
    client = BinanceUMApiClient()
    info = await client.exchange_info()
    print(info)
    await client.close_session()

asyncio.run(main())
