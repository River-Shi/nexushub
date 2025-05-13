import msgspec
from typing import Any, List
import time
import pandas as pd
import datetime
from nexushub.constants import ContractStatus, BinanceKlineInterval
from nexushub.utils import safe_timestamp

class BinanceUMFundingRateResponse(msgspec.Struct):
    symbol: str
    fundingRate: str
    fundingTime: int
    markPrice: str

class BinanceUMRateLimit(msgspec.Struct):
    interval: str
    intervalNum: int
    limit: int
    rateLimitType: str


class BinanceUMAsset(msgspec.Struct):
    asset: str
    marginAvailable: bool
    autoAssetExchange: str


class BinanceUMSymbol(msgspec.Struct, kw_only=True):
    symbol: str
    pair: str
    contractType: str
    deliveryDate: int
    onboardDate: int
    status: ContractStatus
    maintMarginPercent: str
    requiredMarginPercent: str
    baseAsset: str
    quoteAsset: str
    marginAsset: str
    pricePrecision: int
    quantityPrecision: int
    baseAssetPrecision: int
    quotePrecision: int
    underlyingType: str
    underlyingSubType: List[str]
    settlePlan: int | None = None
    triggerProtect: str
    filters: List[Any]
    OrderType: List[str] | None = None
    timeInForce: List[str]
    liquidationFee: str
    marketTakeBound: str

    @property
    def on_board_days(self) -> int:
        timestamp = int(time.time() * 1000)
        days = (timestamp - self.onboardDate) / (1000 * 60 * 60 * 24)
        return int(days)


class BinanceUMExchangeInfoResponse(msgspec.Struct):
    exchangeFilters: List[Any]
    rateLimits: List[BinanceUMRateLimit]
    serverTime: int
    assets: List[BinanceUMAsset]
    symbols: List[BinanceUMSymbol]
    timezone: str

    @property
    def all_symbols(self) -> List[str]:
        return [s.symbol for s in self.symbols]

    @property
    def active_symbols(self) -> List[str]:
        return [s.symbol for s in self.symbols if s.status.active]

    def on_board_symbols(self, days: int) -> List[str]:
        return [s.symbol for s in self.symbols if s.on_board_days >= days]


class BinanceUMKlineResponse(msgspec.Struct, array_like=True):
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

    @property
    def confirmed(self) -> bool:
        timestamp = int(time.time() * 1000)
        return self.close_time < timestamp


class SubscriptionRequest(msgspec.Struct):
    method: str
    params: list[str]
    id: int | None = None


class BinanceUMFuningRate(list):
    def __init__(
        self,
        symbol: str,
        funding_rates: list[BinanceUMFundingRateResponse],
    ):
        super().__init__(funding_rates)
        self.symbol = symbol

    @property
    def values(self) -> list[tuple]:
        return [
            (
                safe_timestamp(funding_rate.fundingTime),
                self.symbol,
                funding_rate.fundingRate,
                funding_rate.markPrice,
            )
            for funding_rate in self
        ]

    @property
    def df(self) -> pd.DataFrame | None:
        if not self:
            return None
        df = pd.DataFrame(self.values, columns=["timestamp", "symbol", "funding_rate", "mark_price"])
        return df

class BinanceUMKline(list):
    def __init__(
        self,
        symbol: str,
        klines: list[BinanceUMKlineResponse],
        include_unconfirmed: bool = False,
    ):
        super().__init__(klines)
        self.include_unconfirmed = include_unconfirmed
        self.symbol = symbol

    @property
    def values(self) -> list[tuple]:
        return [
            (
                datetime.datetime.fromtimestamp(
                    kline.open_time / 1000, tz=datetime.timezone.utc
                ),
                self.symbol,
                kline.open_time,
                kline.close_time,
                kline.open,
                kline.high,
                kline.low,
                kline.close,
                kline.volume,
                kline.quote_asset_volume,
                kline.number_of_trades,
                kline.taker_base_asset_volume,
                kline.taker_quote_asset_volume,
            )
            for kline in self
            if self.include_unconfirmed or kline.confirmed
        ]

    @property
    def df(self) -> pd.DataFrame | None:
        data = []
        for kline in self:
            if self.include_unconfirmed or kline.confirmed:
                data.append(
                    {
                        "open_time": kline.open_time,
                        "close_time": kline.close_time,
                        "open": kline.open,
                        "high": kline.high,
                        "low": kline.low,
                        "close": kline.close,
                        "volume": kline.volume,
                        "quote_asset_volume": kline.quote_asset_volume,
                        "number_of_trades": kline.number_of_trades,
                        "taker_base_asset_volume": kline.taker_base_asset_volume,
                        "taker_quote_asset_volume": kline.taker_quote_asset_volume,
                    }
                )
        if not data:
            return None
        df = pd.DataFrame(data)
        df["symbol"] = self.symbol
        df["timestamp"] = pd.to_datetime(df["open_time"], unit="ms")
        df = df[
            [
                "timestamp",
                "symbol",
                "open_time",
                "close_time",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "quote_asset_volume",
                "number_of_trades",
                "taker_base_asset_volume",
                "taker_quote_asset_volume",
            ]
        ]
        return df


class BinanceUMKlineWsMsgData(msgspec.Struct):
    t: int
    T: int
    s: str
    i: BinanceKlineInterval
    f: int
    L: int
    o: str
    c: str
    h: str
    l: str
    v: str
    n: int
    x: bool
    q: str
    V: str
    Q: str
    B: str


class BinanceUMKlineWsMsg(msgspec.Struct):
    e: str
    E: int
    s: str
    k: BinanceUMKlineWsMsgData

    def parse_kline(self) -> BinanceUMKlineResponse:
        return BinanceUMKlineResponse(
            open_time=self.k.t,
            open=self.k.o,
            high=self.k.h,
            low=self.k.l,
            close=self.k.c,
            volume=self.k.v,
            close_time=self.k.T,
            quote_asset_volume=self.k.q,
            number_of_trades=self.k.n,
            taker_base_asset_volume=self.k.V,
            taker_quote_asset_volume=self.k.Q,
            ignore=self.k.B,
        )


class BinanceUMKlineWsResponse(msgspec.Struct):
    stream: str | None = None
    data: BinanceUMKlineWsMsg | None = None
