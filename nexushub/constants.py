import msgspec
from enum import Enum



class BinanceKlineInterval(Enum):
    """
    Represents a Binance kline chart interval.
    """

    SECOND_1 = "1s"
    MINUTE_1 = "1m"
    MINUTE_3 = "3m"
    MINUTE_5 = "5m"
    MINUTE_15 = "15m"
    MINUTE_30 = "30m"
    HOUR_1 = "1h"
    HOUR_2 = "2h"
    HOUR_4 = "4h"
    HOUR_6 = "6h"
    HOUR_8 = "8h"
    HOUR_12 = "12h"
    DAY_1 = "1d"
    DAY_3 = "3d"
    WEEK_1 = "1w"
    MONTH_1 = "1M"

class BinanceAccountType(Enum):
    SPOT = "SPOT"
    USD_M_FUTURE = "USD_M_FUTURE"
    COIN_M_FUTURE = "COIN_M_FUTURE"
    SPOT_TESTNET = "SPOT_TESTNET"
    USD_M_FUTURE_TESTNET = "USD_M_FUTURE_TESTNET"
    COIN_M_FUTURE_TESTNET = "COIN_M_FUTURE_TESTNET"
    
    @property
    def ws_url(self):
         return STREAM_URLS[self]
    
    @property
    def exchange_id(self):
        return "binance"

    @property
    def is_spot(self):
        return self in (self.SPOT, self.SPOT_TESTNET)

    @property
    def is_future(self):
        return self in (
            self.USD_M_FUTURE,
            self.COIN_M_FUTURE,
            self.USD_M_FUTURE_TESTNET,
            self.COIN_M_FUTURE_TESTNET,
        )

    @property
    def is_linear(self):
        return self in (self.USD_M_FUTURE, self.USD_M_FUTURE_TESTNET)

    @property
    def is_inverse(self):
        return self in (self.COIN_M_FUTURE, self.COIN_M_FUTURE_TESTNET)

    @property
    def is_testnet(self):
        return self in (
            self.SPOT_TESTNET,
            self.USD_M_FUTURE_TESTNET,
            self.COIN_M_FUTURE_TESTNET,
        )


STREAM_URLS = {
    BinanceAccountType.SPOT: "wss://stream.binance.com:9443/ws",
    BinanceAccountType.USD_M_FUTURE: "wss://fstream.binance.com/ws",
    BinanceAccountType.COIN_M_FUTURE: "wss://dstream.binance.com/ws",
    BinanceAccountType.SPOT_TESTNET: "wss://testnet.binance.vision/ws",
    BinanceAccountType.USD_M_FUTURE_TESTNET: "wss://stream.binancefuture.com/ws",
    BinanceAccountType.COIN_M_FUTURE_TESTNET: "wss://dstream.binancefuture.com/ws",
}


class SubscriptionRequest(msgspec.Struct):
    event_type: str
    symbols: list[str]
    interval: BinanceKlineInterval | None = None


class BinanceKlineData(msgspec.Struct):
    t: int  # Kline start time
    T: int  # Kline close time
    s: str  # Symbol
    i: BinanceKlineInterval  # Interval
    f: int  # First trade ID
    L: int  # Last trade ID
    o: str  # Open price
    c: str  # Close price
    h: str  # High price
    l: str  # Low price # noqa
    v: str  # Base asset volume
    n: int  # Number of trades
    x: bool  # Is this kline closed?
    q: str  # Quote asset volume
    V: str  # Taker buy base asset volume
    Q: str  # Taker buy quote asset volume
    B: str  # Ignore
    timestamp: int

