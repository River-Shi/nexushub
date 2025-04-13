import aiohttp
from abc import ABC
from typing import Optional
import ssl
import certifi
from nexushub.throttler import AsyncThrottler
from nexushub.utils import Log, LiveClock


class ApiClient(ABC):
    def __init__(
        self,
        timeout: int = 10,
        max_rate: int | None = None,
        period: float = 60,
    ):
        self._timeout = timeout
        self._log = Log.get_logger()
        self._ssl_context = ssl.create_default_context(cafile=certifi.where())
        self._session: Optional[aiohttp.ClientSession] = None
        self._clock = LiveClock()

        if max_rate:
            self._limiter = AsyncThrottler(rate_limit=max_rate, period=period)
        else:
            self._limiter = None

    def _init_session(self):
        if self._session is None:
            timeout = aiohttp.ClientTimeout(total=self._timeout)
            tcp_connector = aiohttp.TCPConnector(
                ssl=self._ssl_context, enable_cleanup_closed=True
            )
            self._session = aiohttp.ClientSession(
                connector=tcp_connector, timeout=timeout
            )

    async def close_session(self):
        if self._session:
            await self._session.close()
            self._session = None
