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

    @property
    def seconds(self):
        return {
            self.SECOND_1: 1,
            self.MINUTE_1: 60,
            self.MINUTE_3: 180,
            self.MINUTE_5: 300,
            self.MINUTE_15: 900,
            self.MINUTE_30: 1800,
            self.HOUR_1: 3600,
            self.HOUR_2: 7200,
            self.HOUR_4: 14400,
            self.HOUR_6: 21600,
            self.HOUR_8: 28800,
            self.HOUR_12: 43200,
            self.DAY_1: 86400,
            self.DAY_3: 259200,
            self.WEEK_1: 604800,
            self.MONTH_1: 2592000,
        }[self]


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
    BinanceAccountType.SPOT: "wss://stream.binance.com:9443/stream",
    BinanceAccountType.USD_M_FUTURE: "wss://fstream.binance.com/stream",
    BinanceAccountType.COIN_M_FUTURE: "wss://dstream.binance.com/stream",
    BinanceAccountType.SPOT_TESTNET: "wss://testnet.binance.vision/stream",
    BinanceAccountType.USD_M_FUTURE_TESTNET: "wss://stream.binancefuture.com/stream",
    BinanceAccountType.COIN_M_FUTURE_TESTNET: "wss://dstream.binancefuture.com/stream",
}


class SubscriptionRequest(msgspec.Struct):
    method: str
    params: list[str]
    id: int | None = None


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


class ContractStatus(Enum):
    """
    PENDING_TRADING
    TRADING
    PRE_DELIVERING
    DELIVERING
    DELIVERED
    PRE_SETTLE
    SETTLING
    CLOSE
    """

    PENDING_TRADING = "PENDING_TRADING"
    TRADING = "TRADING"
    PRE_DELIVERING = "PRE_DELIVERING"
    DELIVERING = "DELIVERING"
    DELIVERED = "DELIVERED"
    PRE_SETTLE = "PRE_SETTLE"
    SETTLING = "SETTLING"
    CLOSE = "CLOSE"

    @property
    def active(self):
        return self == self.TRADING


class BinanceUMKline(msgspec.Struct, array_like=True):
    """
    [
        1499040000000,      // Open time
        "0.01634790",       // Open
        "0.80000000",       // High
        "0.01575800",       // Low
        "0.01577100",       // Close
        "148976.11427815",  // Volume
        1499644799999,      // Close time
        "2434.19055334",    // Quote asset volume
        308,                // Number of trades
        "1756.87402397",    // Taker buy base asset volume
        "28.46694368",      // Taker buy quote asset volume
        "17928899.62484339" // Ignore.
    ]
    """

    open_time: int
    open: str
    high: str
    low: str
    close: str
    volume: str
    close_time: int
    quote_asset_volume: str
    number_of_trades: int
    taker_base_asset_volume: str
    taker_quote_asset_volume: str
    ignore: str
