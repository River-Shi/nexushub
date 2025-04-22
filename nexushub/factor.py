import asyncio
import msgspec
import clickhouse_connect
import pandas as pd
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from datetime import datetime, timedelta
from nexushub.constants import BinanceKlineInterval
from nexushub.schema import BinanceUMKlineWsResponse, BinanceUMKlineResponse
from nexushub.binance import BinanceWSClient, BinanceAccountType
from concurrent.futures import ThreadPoolExecutor
import os

CLICKHOUSE_DB = os.getenv("CLICKHOUSE_DB")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD")
CLICKHOUSE_HOST = "localhost"
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT"))



class BaseFactor:
    def __init__(
        self,
        freq: BinanceKlineInterval,
        loopback: int,
        db_port: int = CLICKHOUSE_PORT,
        db_host: str = CLICKHOUSE_HOST,
        db_user: str = CLICKHOUSE_USER,
        db_password: str = CLICKHOUSE_PASSWORD,
        db_name: str = CLICKHOUSE_DB,

    ):
        if freq not in BinanceKlineInterval:
            raise ValueError(f"Invalid frequency: {freq}")
        self._freq = freq
        self._table = f"kline_{freq.value}"
        self._loopback = loopback
        self._client = clickhouse_connect.get_client(host=db_host, port=db_port, user=db_user, password=db_password, database=db_name)

    def _get_historical_data(self) -> pd.DataFrame:
        loopback_time = datetime.now() - timedelta(seconds=self._freq.seconds * self._loopback)
        
        sql = f"""
        SELECT * FROM {self._table}
        WHERE time >= '{loopback_time.strftime('%Y-%m-%d %H:%M:%S')} ORDER BY time"
        """
                
        df = self._client.query_df(sql)
        return df

    def on_data(self, latest_data: pd.DataFrame) -> pd.DataFrame:
        historical_data = self._get_historical_data()
        if historical_data.empty:
            kline = latest_data
        else:
            latest_data = latest_data.reindex(columns=historical_data.columns)
            kline = pd.concat([historical_data, latest_data], axis=0)
        return self.calculate(kline, **self._params)

    def calculate(self, kline: pd.DataFrame) -> pd.DataFrame:
        raise NotImplementedError("Subclasses must implement the calculate method.")



class FactorMixer:
    def __init__(self):
        self._factors: list[BaseFactor] = []
        self._kline_data: dict[str, BinanceUMKlineResponse] = {}
        self._ws_kline_decoder = msgspec.json.Decoder(BinanceUMKlineWsResponse)
        self._ws = BinanceWSClient(
            account_type=BinanceAccountType.USD_M_FUTURE,
            handler=self._ws_handler,
            loop=asyncio.get_event_loop(),
        )
        self._scheduler = AsyncIOScheduler()
        
        self._event = asyncio.Event()
    
    @property
    def latest_kline(self):
        data = {
            "symbol": [],
            "open_time": [],
            "close_time": [],
            "open": [],
            "high": [],
            "low": [],
            "close": [],
            "volume": [],
            "quote_asset_volume": [],
            "number_of_trades": [],
            "taker_base_asset_volume": [],
            "taker_quote_asset_volume": [],
        }
        for symbol, kline in self._kline_data.items():
            data['symbol'].append(symbol)
            data['open_time'].append(int(kline.open_time))
            data['close_time'].append(int(kline.close_time))
            data['open'].append(float(kline.open))
            data['high'].append(float(kline.high))
            data['low'].append(float(kline.low))
            data['close'].append(float(kline.close))
            data['volume'].append(float(kline.volume))
            data['quote_asset_volume'].append(float(kline.quote_asset_volume))
            data['number_of_trades'].append(int(kline.number_of_trades))
            data['taker_base_asset_volume'].append(float(kline.taker_base_asset_volume))
            data['taker_quote_asset_volume'].append(float(kline.taker_quote_asset_volume))
            
        df = pd.DataFrame(data)
        df['time'] = pd.to_datetime(df['open_time'], unit='ms', utc=True)
        return df
    
    def _ws_handler(self, msg: bytes):
        kline = self._ws_kline_decoder.decode(msg)
        self._kline_data[kline.data.s] = kline.data.parse_kline()
    
    def add_factor(self, factor: BaseFactor):
        self._factors.append(factor)
    
    def schedule(
        self,
        func: callable,
        trigger: str = "interval",
        **kwargs,
    ):
        if trigger not in ["interval", "cron"]:
            raise ValueError(f"Invalid trigger: {trigger}")
        self._scheduler.add_job(func, trigger=trigger, **kwargs)
    
    def calculate_factor(self):
        latest_kline = self.latest_kline
        with ThreadPoolExecutor() as executor:
            futures = [executor.submit(factor.calculate, latest_kline) for factor in self._factors]
            results = [future.result() for future in futures]
        for res in results:
            print(res)
        

    async def start(self, symbols: list[str], freq: BinanceKlineInterval):
        await self._ws.connect()
        self._ws.subscribe_kline(symbols, freq)
        self._scheduler.start()
        await self._event.wait()
    
    async def stop(self):
        self._ws.disconnect()
        self._scheduler.shutdown()
        self._event.set()
    
    
    

