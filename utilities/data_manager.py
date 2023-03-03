import asyncio
from posixpath import dirname
from pathlib import Path
import ccxt.async_support as ccxt
import pytz
import pandas as pd
import os
from datetime import datetime, timedelta
from tqdm.auto import tqdm
import itertools
import timeit
import time


class ExchangeDataManager:

    # Liste des exchanges à supporter
    CCXT_EXCHANGES = {
        "binance": {
            "ccxt_object": ccxt.binance(config={"enableRateLimit": True}),
            "limit_size_request": 1000,
        },
        "binanceusdm": {
            "ccxt_object": ccxt.binanceusdm(config={"enableRateLimit": True}),
            "limit_size_request": 1000,
        },
        "kucoin": {
            "ccxt_object": ccxt.kucoin(config={"enableRateLimit": True}),
            "limit_size_request": 1500,
        },
        "hitbtc": {
            "ccxt_object": ccxt.hitbtc(config={"enableRateLimit": True}),
            "limit_size_request": 1000,
        },
        "bitfinex": {
            "ccxt_object": ccxt.bitfinex(config={"enableRateLimit": True}),
            "limit_size_request": 10000,
        },
        "bybit": {
            "ccxt_object": ccxt.bybit(config={"enableRateLimit": True}),
            "limit_size_request": 200,
        },
        "bitget": {
            "ccxt_object": ccxt.bitget(config={"enableRateLimit": True}),
            "limit_size_request": 100,
        },
        "bitmart": {
            "ccxt_object": ccxt.bitmart(config={"enableRateLimit": True}),
            "limit_size_request": 500,     
        }
    }

    # Liste des intervalles à supporter
    INTERVALS = {
        "1m": {"timedelta": timedelta(minutes=1), "interval_ms": 60000},
        "2m": {"timedelta": timedelta(minutes=2), "interval_ms": 120000},
        "5m": {"timedelta": timedelta(minutes=5), "interval_ms": 300000},
        "15m": {"timedelta": timedelta(minutes=15), "interval_ms": 900000},
        "30m": {"timedelta": timedelta(minutes=30), "interval_ms": 1800000},
        "1h": {"timedelta": timedelta(hours=1), "interval_ms": 3600000},
        "2h": {"timedelta": timedelta(hours=2), "interval_ms": 7200000},
        "4h": {"timedelta": timedelta(hours=4), "interval_ms": 14400000},
        "12h": {"timedelta": timedelta(hours=12), "interval_ms": 43200000},
        "1d": {"timedelta": timedelta(days=1), "interval_ms": 86400000},
        "1w": {"timedelta": timedelta(weeks=1), "interval_ms": 604800000},
        "1M": {"timedelta": timedelta(days=30), "interval_ms": 2629746000},
    }

    def __init__(self, exchange_name, path_download="./") -> None:
        """This method create an ExchangeDataManager object
           Args:
               exchange_name (_type_): the exchange you need for download or load data
               path_download (str, optional): directory path (default "./")

           Raises:
               NotImplementedError: if the exchange is unsupported
        """
        self.exchange_name = exchange_name.lower()
        self.path_download = path_download
        try:
            self.exchange_dict = ExchangeDataManager.CCXT_EXCHANGES[self.exchange_name]
        except Exception:
            raise NotImplementedError(
                f"L'échange {self.exchange_name} n'est pas supporté"
            )
        self.intervals_dict = ExchangeDataManager.INTERVALS

        self.exchange = self.exchange_dict["ccxt_object"]

        self.path_data = str(
            Path(
                os.path.join(dirname(__file__), self.path_download, self.exchange_name)
            ).resolve()
        )
        os.makedirs(self.path_data, exist_ok=True)
        self.pbar = None

    def load_data(
        self, coin, interval, start_date="1990", end_date="2050"
    ) -> pd.DataFrame:
        """This method load the market data between 2 dates

            :param coin: symbol (ex: BTCUSDT)
            :param interval: interval between each point of data (ex: 1h)
            :param start_date: starting date (default 1990)
            :param end_date: end date (default 2050)
            :return pd.DataFrame
        """
        file_path = f"{self.path_data}/{interval}/"
        file_name = f"{file_path}{coin.replace('/', '-')}.csv"
        if not os.path.exists(file_name):
            raise FileNotFoundError(f"Le fichier {file_name} n'existe pas")

        df = pd.read_csv(file_name, index_col=0, parse_dates=True)
        df.index = pd.to_datetime(df.index, unit="ms")
        df = df.groupby(df.index).first()
        df = df.loc[start_date:end_date]
        df = df.iloc[:-1]

        return df

    async def download_data(
        self,
        coins,
        intervals,
        start_date="2017-01-01 00:00:00",
        end_date=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    ) -> None:
        """This method download the market data between 2 dates

            :param coins: list of symbols (ex: [BTCUSDT])
            :param intervals: list of intervals between each point of data (ex: [1h, 1w])
            :param start_date: starting date (ex:  2020-01-01 00:00:00) (default: "2017-01-01 00:00:00")
            :param end_date: end date (ex: 2023-01-01 01:00:00) (default: current timestamp)
            :return None
        """

        await self.exchange.load_markets()

        start_date = datetime.strptime(start_date, "%Y-%m-%d %H:%M:%S")
        end_date = datetime.strptime(end_date, "%Y-%m-%d %H:%M:%S")
   

        for interval in intervals:

            all_dt_intervals = list(
                self.create_intervals(
                    start_date, end_date, self.create_timedelta(interval)
                )
            )
   
            last_dt = all_dt_intervals[-1].astimezone(pytz.utc)
            end_timestamp = int(last_dt.timestamp() * 1000)
          
            for coin in coins:
                print(
                    f"\tRécupération pour la paire {coin} en timeframe {interval} sur l'exchange {self.exchange_name}..."
                )

                file_path = f"{self.path_data}/{interval}/"
                os.makedirs(file_path, exist_ok=True)
                file_name = f"{file_path}{coin.replace('/', '-')}.csv"

                dt_or_false = await self.is_data_missing(file_name, last_dt, str(start_date))
                
                if dt_or_false:

                    tasks = []
                    current_timestamp = int(dt_or_false.timestamp() * 1000)
                    turn = 0
                 
                    while current_timestamp <= end_timestamp:
                        tasks.append(
                            asyncio.create_task(
                                self.download_tf(coin, interval, current_timestamp)
                            )
                        )
                        current_timestamp = (
                            self.exchange_dict["limit_size_request"]
                            * self.intervals_dict[interval]["interval_ms"]
                            + current_timestamp
                        )

                    self.pbar = tqdm(tasks)
                    results = await asyncio.gather(*tasks)
                    await self.exchange.close()
                  
                    results = list(itertools.chain(*results))
               
                    self.pbar.close()

                    if results:
                        final = pd.DataFrame(
                            results,
                            columns=["date", "open", "high", "low", "close", "volume"],
                        )
                        final.set_index('date', drop=False, inplace=True)
                        final = final[~final.index.duplicated(keep='first')]
                        
                        
                        flag_header = (
                            ("a", False) if os.path.exists(file_name) else ("w", True)
                        )
                        with open(file_name, mode=flag_header[0]) as f:
                            final.to_csv(
                                path_or_buf=f, header=flag_header[1], index=False
                            )
                    else:
                        print(f"\tPas de données pour {coin} en {interval} sur cette période")
                else:
                    print("\tDonnées déjà récupérées")

                #print("\033[H\033[J", end="")

    async def download_tf(self, coin, interval, start_timestamp) -> list:
        tests = 0
        while True:
            try:
                r = await self.exchange.fetch_ohlcv(
                    symbol=coin,
                    timeframe=interval,
                    since=start_timestamp,
                    limit=self.exchange_dict["limit_size_request"],
                )
                self.pbar.update(1)
                return r
            except Exception:
                tests += 1
                if tests == 3:
                    raise TooManyError

    async def is_data_missing(self, file_name, last_dt, start_date) -> bool | datetime:

        await self.exchange.close()

        if os.path.isfile(file_name):
            df = pd.read_csv(file_name, index_col=0, parse_dates=True)
            df.index = pd.to_datetime(df.index, unit="ms")
            df = df.groupby(df.index).first()

            if pytz.utc.localize(df.index[-1]) >= last_dt:
                return False
        else:
            return datetime.fromisoformat(start_date)
        return pytz.utc.localize(df.index[-2])

    def create_intervals(self, start_date, end_date, delta):
        current = start_date
        while current <= end_date:
            yield current
            current += delta

    def create_timedelta(self, interval) -> int:
        try:
            return self.intervals_dict[interval]["timedelta"]
        except Exception:
            raise ValueError(f"Intervalle {interval} inconnu")

    def explore_data(self) -> pd.DataFrame:
        files_data = []
        for path, subdirs, files in os.walk(self.path_download):
            for name in files:
                if os.path.join(path, name).endswith(".csv"):
                    current_file = os.path.join(path, name)
                    file_split = current_file.split("\\")
                    try:
                        df_file = pd.read_csv(current_file)
                    except Exception:
                        continue

                    files_data.append(
                        {
                            "exchange": file_split[1],
                            "timeframe": file_split[2],
                            "pair": file_split[3][:-4],
                            "occurences": len(df_file),
                            "start_date": str(
                                datetime.fromtimestamp(df_file.iloc[0]["date"] / 1000)
                            ),
                            "end_date": str(
                                datetime.fromtimestamp(df_file.iloc[-1]["date"] / 1000)
                            ),
                        }
                    )

        return pd.DataFrame(files_data)


class TooManyError(Exception):
    pass
