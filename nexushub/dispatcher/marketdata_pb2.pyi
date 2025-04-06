from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class SubscriptionRequest(_message.Message):
    __slots__ = ("event_type", "symbols", "interval")
    EVENT_TYPE_FIELD_NUMBER: _ClassVar[int]
    SYMBOLS_FIELD_NUMBER: _ClassVar[int]
    INTERVAL_FIELD_NUMBER: _ClassVar[int]
    event_type: str
    symbols: _containers.RepeatedScalarFieldContainer[str]
    interval: str
    def __init__(self, event_type: _Optional[str] = ..., symbols: _Optional[_Iterable[str]] = ..., interval: _Optional[str] = ...) -> None: ...

class Kline(_message.Message):
    __slots__ = ("event_type", "event_time", "start_time", "close_time", "symbol", "interval", "first_trade_id", "last_trade_id", "open_price", "close_price", "high_price", "low_price", "volume", "number_of_trades", "is_closed", "quote_asset_volume", "taker_buy_volume", "taker_buy_quote_volume", "timestamp")
    EVENT_TYPE_FIELD_NUMBER: _ClassVar[int]
    EVENT_TIME_FIELD_NUMBER: _ClassVar[int]
    START_TIME_FIELD_NUMBER: _ClassVar[int]
    CLOSE_TIME_FIELD_NUMBER: _ClassVar[int]
    SYMBOL_FIELD_NUMBER: _ClassVar[int]
    INTERVAL_FIELD_NUMBER: _ClassVar[int]
    FIRST_TRADE_ID_FIELD_NUMBER: _ClassVar[int]
    LAST_TRADE_ID_FIELD_NUMBER: _ClassVar[int]
    OPEN_PRICE_FIELD_NUMBER: _ClassVar[int]
    CLOSE_PRICE_FIELD_NUMBER: _ClassVar[int]
    HIGH_PRICE_FIELD_NUMBER: _ClassVar[int]
    LOW_PRICE_FIELD_NUMBER: _ClassVar[int]
    VOLUME_FIELD_NUMBER: _ClassVar[int]
    NUMBER_OF_TRADES_FIELD_NUMBER: _ClassVar[int]
    IS_CLOSED_FIELD_NUMBER: _ClassVar[int]
    QUOTE_ASSET_VOLUME_FIELD_NUMBER: _ClassVar[int]
    TAKER_BUY_VOLUME_FIELD_NUMBER: _ClassVar[int]
    TAKER_BUY_QUOTE_VOLUME_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    event_type: str
    event_time: int
    start_time: int
    close_time: int
    symbol: str
    interval: str
    first_trade_id: int
    last_trade_id: int
    open_price: str
    close_price: str
    high_price: str
    low_price: str
    volume: str
    number_of_trades: int
    is_closed: bool
    quote_asset_volume: str
    taker_buy_volume: str
    taker_buy_quote_volume: str
    timestamp: int
    def __init__(self, event_type: _Optional[str] = ..., event_time: _Optional[int] = ..., start_time: _Optional[int] = ..., close_time: _Optional[int] = ..., symbol: _Optional[str] = ..., interval: _Optional[str] = ..., first_trade_id: _Optional[int] = ..., last_trade_id: _Optional[int] = ..., open_price: _Optional[str] = ..., close_price: _Optional[str] = ..., high_price: _Optional[str] = ..., low_price: _Optional[str] = ..., volume: _Optional[str] = ..., number_of_trades: _Optional[int] = ..., is_closed: bool = ..., quote_asset_volume: _Optional[str] = ..., taker_buy_volume: _Optional[str] = ..., taker_buy_quote_volume: _Optional[str] = ..., timestamp: _Optional[int] = ...) -> None: ...

class Trade(_message.Message):
    __slots__ = ("event_time", "symbol", "trade_id", "price", "quantity", "trade_time", "is_buyer_maker")
    EVENT_TIME_FIELD_NUMBER: _ClassVar[int]
    SYMBOL_FIELD_NUMBER: _ClassVar[int]
    TRADE_ID_FIELD_NUMBER: _ClassVar[int]
    PRICE_FIELD_NUMBER: _ClassVar[int]
    QUANTITY_FIELD_NUMBER: _ClassVar[int]
    TRADE_TIME_FIELD_NUMBER: _ClassVar[int]
    IS_BUYER_MAKER_FIELD_NUMBER: _ClassVar[int]
    event_time: int
    symbol: str
    trade_id: int
    price: str
    quantity: str
    trade_time: int
    is_buyer_maker: bool
    def __init__(self, event_time: _Optional[int] = ..., symbol: _Optional[str] = ..., trade_id: _Optional[int] = ..., price: _Optional[str] = ..., quantity: _Optional[str] = ..., trade_time: _Optional[int] = ..., is_buyer_maker: bool = ...) -> None: ...

class AggTrade(_message.Message):
    __slots__ = ("event_type", "event_time", "symbol", "trade_id", "price", "quantity", "first_trade_id", "last_trade_id", "trade_time", "is_buyer_maker")
    EVENT_TYPE_FIELD_NUMBER: _ClassVar[int]
    EVENT_TIME_FIELD_NUMBER: _ClassVar[int]
    SYMBOL_FIELD_NUMBER: _ClassVar[int]
    TRADE_ID_FIELD_NUMBER: _ClassVar[int]
    PRICE_FIELD_NUMBER: _ClassVar[int]
    QUANTITY_FIELD_NUMBER: _ClassVar[int]
    FIRST_TRADE_ID_FIELD_NUMBER: _ClassVar[int]
    LAST_TRADE_ID_FIELD_NUMBER: _ClassVar[int]
    TRADE_TIME_FIELD_NUMBER: _ClassVar[int]
    IS_BUYER_MAKER_FIELD_NUMBER: _ClassVar[int]
    event_type: str
    event_time: int
    symbol: str
    trade_id: int
    price: str
    quantity: str
    first_trade_id: int
    last_trade_id: int
    trade_time: int
    is_buyer_maker: bool
    def __init__(self, event_type: _Optional[str] = ..., event_time: _Optional[int] = ..., symbol: _Optional[str] = ..., trade_id: _Optional[int] = ..., price: _Optional[str] = ..., quantity: _Optional[str] = ..., first_trade_id: _Optional[int] = ..., last_trade_id: _Optional[int] = ..., trade_time: _Optional[int] = ..., is_buyer_maker: bool = ...) -> None: ...

class BookTicker(_message.Message):
    __slots__ = ("event_type", "update_id", "symbol", "pair", "bid_price", "bid_qty", "ask_price", "ask_qty", "transaction_time", "event_time")
    EVENT_TYPE_FIELD_NUMBER: _ClassVar[int]
    UPDATE_ID_FIELD_NUMBER: _ClassVar[int]
    SYMBOL_FIELD_NUMBER: _ClassVar[int]
    PAIR_FIELD_NUMBER: _ClassVar[int]
    BID_PRICE_FIELD_NUMBER: _ClassVar[int]
    BID_QTY_FIELD_NUMBER: _ClassVar[int]
    ASK_PRICE_FIELD_NUMBER: _ClassVar[int]
    ASK_QTY_FIELD_NUMBER: _ClassVar[int]
    TRANSACTION_TIME_FIELD_NUMBER: _ClassVar[int]
    EVENT_TIME_FIELD_NUMBER: _ClassVar[int]
    event_type: str
    update_id: int
    symbol: str
    pair: str
    bid_price: str
    bid_qty: str
    ask_price: str
    ask_qty: str
    transaction_time: int
    event_time: int
    def __init__(self, event_type: _Optional[str] = ..., update_id: _Optional[int] = ..., symbol: _Optional[str] = ..., pair: _Optional[str] = ..., bid_price: _Optional[str] = ..., bid_qty: _Optional[str] = ..., ask_price: _Optional[str] = ..., ask_qty: _Optional[str] = ..., transaction_time: _Optional[int] = ..., event_time: _Optional[int] = ...) -> None: ...

class MarketDataResponse(_message.Message):
    __slots__ = ("stream_name", "kline", "trade", "agg_trade", "book_ticker")
    STREAM_NAME_FIELD_NUMBER: _ClassVar[int]
    KLINE_FIELD_NUMBER: _ClassVar[int]
    TRADE_FIELD_NUMBER: _ClassVar[int]
    AGG_TRADE_FIELD_NUMBER: _ClassVar[int]
    BOOK_TICKER_FIELD_NUMBER: _ClassVar[int]
    stream_name: str
    kline: Kline
    trade: Trade
    agg_trade: AggTrade
    book_ticker: BookTicker
    def __init__(self, stream_name: _Optional[str] = ..., kline: _Optional[_Union[Kline, _Mapping]] = ..., trade: _Optional[_Union[Trade, _Mapping]] = ..., agg_trade: _Optional[_Union[AggTrade, _Mapping]] = ..., book_ticker: _Optional[_Union[BookTicker, _Mapping]] = ...) -> None: ...
