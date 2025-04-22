import asyncio
from nexushub.factor import BaseFactor, FactorMixer
from nexushub.binance import BinanceUMApiClient
from nexushub.constants import BinanceKlineInterval
import requests
import tenacity
import pandas as pd
import numpy as np



class TakerTurnover(BaseFactor):
    def __init__(self, min_volume: int = 1e6):
        super().__init__(freq=BinanceKlineInterval.HOUR_1, loopback=1)
        self._min_volume = min_volume
        
    def _request_get(self, url, params=None):
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        return data

    @tenacity.retry(
        stop=tenacity.stop_after_attempt(5),
        wait=tenacity.wait_random(min=0.5, max=1),
    )
    def fetch_binance_data_v1(self):
        url = "https://www.binance.com/bapi/apex/v1/friendly/apex/marketing/complianceSymbolList"
        # Fetch data from the API
        response = self._request_get(url)

        if not (response.get("code") == "000000" and response.get("data")):
            self.log.error("Failed to fetch or parse data from Binance API")
            raise

        data = response["data"]
        df = pd.DataFrame(data)
        df = df[["symbol", "circulatingSupply"]]
        df["circulatingSupply"] = df["circulatingSupply"].astype(float)
        return df

    def calculate(self, kline: pd.DataFrame):
        df_mktcap = self.fetch_binance_data_v1()
        df = pd.merge(kline, df_mktcap, on="symbol", how="left")
        df.set_index("symbol", inplace=True)
        
        df['taker_turnover'] = df['taker_base_asset_volume'] / df['circulatingSupply']

        df['taker_turnover'] = np.where(df['quote_asset_volume'] >= self._min_volume, -df['taker_turnover'], np.nan) # NOTE: sign is negative
        return df



class Kurt(BaseFactor):
    def __init__(self, n_period: int = 72, min_volume: int = 20e6 / 24):
        super().__init__(freq=BinanceKlineInterval.HOUR_1, loopback=n_period * 3)
        self._n_period = n_period
        self._min_volume = min_volume

    def calculate(self, kline: pd.DataFrame) -> pd.DataFrame:
        df = kline.copy()
        df['timestamp'] = df['time'] 
        df.sort_values(["symbol", "timestamp"], inplace=True, ignore_index=True)

        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df = df.sort_values(by=['symbol', 'timestamp'])

        
        df['ret'] = df['close'] / df['open'] - 1

        
        # Add BTC returns as a new column
        df['btc_ret'] = df[df['symbol'] == 'BTCUSDT'].set_index('timestamp')['ret'].reindex(df['timestamp']).values


        df['rolling_corr'] = df.groupby('symbol').apply(
            lambda x: x['ret'].rolling(window=self._n_period, min_periods=self._n_period).corr(x['btc_ret'])
        ).reset_index(level=0, drop=True)
        
        # r = alpha + beta * m + error
        df['ret_pure'] = df['ret'] - df['rolling_corr']  * df['btc_ret']
        df = df[df['symbol'] != 'BTCUSDT']

        df['kurt_ret'] = df.groupby('symbol')['ret_pure'].transform(lambda x: x.rolling(window=self._n_period, min_periods=self._n_period).kurt())    
        
        df['kurt_ret'] = np.where(
            (df['quote_asset_volume'] >= self._min_volume),
            df['kurt_ret'] * -1, #NOTE: relation sign = [-1]
            np.nan
        )
        return df

async def main():
    try:
        factor_mixer = FactorMixer()
        api = BinanceUMApiClient()
        
        info = await api.exchange_info()
        symbols = info.active_symbols

        factor_mixer.add_factor(TakerTurnover(min_volume=1e6))
        factor_mixer.add_factor(Kurt(n_period=72, min_volume=20e6 / 24))
        
        factor_mixer.schedule(factor_mixer.calculate_factor, trigger="interval", seconds=60)
        
        await factor_mixer.start(symbols=symbols, freq=BinanceKlineInterval.HOUR_1)
    except asyncio.CancelledError:
        pass
    finally:
        await api.close_session()
        await factor_mixer.stop()

if __name__ == "__main__":
    asyncio.run(main())

