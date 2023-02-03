import asyncio
from posixpath import dirname
from pathlib import Path
import ccxt.async_support as ccxt
import pytz
import pandas as pd
import os
from datetime import datetime, timedelta
from tqdm.auto import tqdm


class ExchangeDataManager:

    # Liste des exchanges à supporter
    CCXT_EXCHANGES = {
        "binance": {
            "ccxt_object": ccxt.binance(config={'enableRateLimit': True}),
            "limit_size_request": 1000
        },
        "binanceusdm": {
            "ccxt_object": ccxt.binanceusdm(config={'enableRateLimit': True}),
            "limit_size_request": 1000
        },
        "kucoin": {
            "ccxt_object": ccxt.kucoin(config={'enableRateLimit': True}),
            "limit_size_request": 1500
        },
        "hitbtc": {
            "ccxt_object": ccxt.hitbtc(config={'enableRateLimit': True}),
            "limit_size_request": 1000
        },
        "bitfinex": {
            "ccxt_object": ccxt.bitfinex(config={'enableRateLimit': True}),
            "limit_size_request": 10000
        },
        "bitget": {
            "ccxt_object": ccxt.bitget(config={'enableRateLimit': True}),
            "limit_size_request": 1000
        }
    }

    # Liste des intervalles à supporter
    INTERVALS = {
        "1m": {
            "timedelta": timedelta(minutes=1),
            "interval_ms": 60000
        },
        "2m": {
            "timedelta": timedelta(minutes=2),
            "interval_ms": 120000
        },
        "5m": {
            "timedelta": timedelta(minutes=5),
            "interval_ms": 300000
        },
        "15m": {
            "timedelta": timedelta(minutes=15),
            "interval_ms": 900000
        },
        "30m": {
            "timedelta": timedelta(minutes=30),
            "interval_ms": 1800000
        },
        "1h": {
            "timedelta": timedelta(hours=1),
            "interval_ms": 3600000
        },
        "2h": {
            "timedelta": timedelta(hours=2),
            "interval_ms": 7200000
        },
        "4h": {
            "timedelta": timedelta(hours=4),
            "interval_ms": 14400000
        },
        "12h": {
            "timedelta": timedelta(hours=12),
            "interval_ms": 43200000
        },
        "1d": {
            "timedelta": timedelta(days=1),
            "interval_ms": 86400000
        },
        "1w": {
            "timedelta": timedelta(weeks=1),
            "interval_ms": 604800000
        },
        "1M": {
            "timedelta": timedelta(days=30),
            "interval_ms": 2629746000
        }
    }

    def __init__(self, exchange_name, path_download="./") -> None:
        """La fonction prend une chaîne et si possible la convertit en objet ccxt.
        La fonction crée également un chemin vers un dossier appelé nommé dans le répertoire parent
        du répertoire courant, et crée un sous-dossier dans ce dossier avec le nom de l'échange.

        Args:
            cex (_type_): L'échange que vous souhaitez utiliser
            path_download (str, optional): Chemin du dossier à créer exemple ./database. Defaults to "./".

        Raises:
            NotImplementedError: Raise si l'exchange n'est pas paramétré/supporté
        """
        self.exchange_name = exchange_name.lower()
        self.path_download = path_download
        try:
            self.exchange_dict = ExchangeDataManager.CCXT_EXCHANGES[self.exchange_name]
        except Exception:
            raise NotImplementedError(
                f"L'échange {self.exchange_name} n'est pas supporté")
        self.intervals_dict = ExchangeDataManager.INTERVALS
        
        self.exchange = self.exchange_dict["ccxt_object"]
        
        self.path_data = str(
            Path(os.path.join(dirname(__file__), self.path_download, self.exchange_name)).resolve())
        os.makedirs(self.path_data, exist_ok=True)
        self.pbar = None

    def load_data(self, coin, interval, start_date="1990", end_date="2050") -> pd.DataFrame:
        """
        Cette fonction prend une paire, un intervalle, une date de début et une date de fin et renvoie
        une trame de données des données OHLCV pour cette paire

        :param coin: la paire pour laquelle vous souhaitez obtenir des données
        :param interval: l'intervalle de temps entre chaque point de données
        :param start_date: La date de début des données que vous souhaitez charger
        :param end_date: La date à laquelle vous souhaitez mettre fin à vos données
        """
        file_path = f"{self.path_data}/{interval}/"
        file_name = f"{file_path}{coin.replace('/', '-')}.csv"
        if not os.path.exists(file_name):
            raise FileNotFoundError(f"Le fichier {file_name} n'existe pas")

        df = pd.read_csv(file_name, index_col=0, parse_dates=True)
        df.index = pd.to_datetime(df.index, unit='ms')
        df = df.groupby(df.index).first()
        df = df.loc[start_date:end_date]
        df = df.iloc[:-1]

        return df

    async def download_data(
        self,
        coins,
        intervals,
        start_date="2017-01-01 00:00:00",
        end_date=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    ):
        """
        Télécharge les données des API de CEX et les stocke dans des fichiers csv

        :param coins: une liste de paires pour lesquelles télécharger des données
        :param intervals: liste de chaînes, par ex. ['1h', '1d', '5m']
        :param end_date: la date d'arrêt du téléchargement des données. Si aucun, téléchargera les
        données jusqu'à la date actuelle
        """
        await self.exchange.load_markets()
        start_date = datetime.strptime(start_date, "%Y-%m-%d %H:%M:%S")
        end_date = datetime.strptime(end_date, "%Y-%m-%d %H:%M:%S")

        for interval in intervals:

            all_dt_intervals = list(self.create_intervals(
                start_date, end_date, self.create_timedelta(interval)))
            last_dt = all_dt_intervals[-1].astimezone(pytz.utc)

            end_timestamp = int(last_dt.timestamp() * 1000)

            for coin in coins:
                print(
                    f"\tRécupération pour la paire {coin} en timeframe {interval} sur l'exchange {self.exchange_name}...")

                file_path = f"{self.path_data}/{interval}/"
                os.makedirs(file_path, exist_ok=True)
                file_name = f"{file_path}{coin.replace('/', '-')}.csv"

                dt_or_false = await self.is_data_missing(file_name, last_dt)
                if dt_or_false:

                    # print("\tTéléchargement des données")

                    tasks = []
                    current_timestamp = int(dt_or_false.timestamp() * 1000)

                    while True:
                        tasks.append(asyncio.create_task(self.download_tf(
                            coin, interval, current_timestamp)))
                        current_timestamp = min([current_timestamp + self.exchange_dict["limit_size_request"] *
                                                 self.intervals_dict[interval]["interval_ms"], end_timestamp])
                        if current_timestamp >= end_timestamp:
                            break
                    
                    
                    self.pbar = tqdm(tasks)
                    results = await asyncio.gather(*tasks)
                    await self.exchange.close()
                    self.pbar.close()

                    all_df = []
                    for i in results:
                        # Si on n'a aucune donnée on ne fait rien
                        if i:
                            all_df.append(pd.DataFrame(i))

                    # Si il y a des données
                    if all_df:
                        final = pd.concat(all_df, ignore_index=True, sort=False)
                        final.columns = ['date', 'open',
                                         'high', 'low', 'close', 'volume']
                        final.rename(
                            columns={0: 'date', 1: 'open', 2: 'high', 3: 'low', 4: 'close', 5: 'volume'})
                        final.set_index('date', drop=False, inplace=True)
                        final = final[~final.index.duplicated(keep='first')]
                        if os.path.exists(file_name):
                            with open(file_name, mode='a') as f:
                                final.iloc[1:].to_csv(
                                    path_or_buf=f, header=False, index=False)
                        else:
                            with open(file_name, mode='w') as f:
                                final.to_csv(path_or_buf=f, index=False)
                    else:
                        print(
                            f"\tPas de données pour {coin} en {interval} sur cette période")
                else:
                    print("\tDonnées déjà récupérées")
                    
                print("\033[H\033[J", end="")

    async def download_tf(self, coin, interval, start_timestamp):
        """
        Télécharge les données de l'API et les stocke dans une trame de données.

        :param coin: la pièce pour laquelle vous souhaitez télécharger des données
        :param interval: l'intervalle de temps des données que vous souhaitez télécharger
        :param start_timestamp: l'heure de début des données que vous souhaitez télécharger
        """
        tests = 1
        while tests < 3:
            try:
                if self.exchange_name == "bitget":
                    print(coin, start_timestamp, self.exchange_dict["limit_size_request"], {'timeframe': interval})
                    print(await self.exchange.fetch_ohlcv(
                        symbol=coin, since=start_timestamp, limit=self.exchange_dict["limit_size_request"], params={'timeframe': interval}))
                    r = await self.exchange.fetch_ohlcv(
                        symbol=coin, since=start_timestamp, limit=self.exchange_dict["limit_size_request"], params={'timeframe': interval})
                else:
                    r = await self.exchange.fetch_ohlcv(
                        symbol=coin, timeframe=interval, since=start_timestamp, limit=self.exchange_dict["limit_size_request"])

                
                self.pbar.update(1)
                return r
            except Exception:
                tests += 1
                if tests == 3:
                    # print(coin, start_timestamp, self.exchange_dict["limit_size_request"], {'timeframe': interval})
                    raise TooManyError

    async def is_data_missing(self, file_name, last_dt):
        """
        Cette fonction vérifie s'il y a des données manquantes dans la base de données pour une pièce,
        un intervalle et une plage de temps donnés

        :param file_name: Le nom du fichier pour vérifier les données manquantes
        :param coin: la pièce que vous voulez vérifier
        :param interval: l'intervalle des données, par ex. 1m, 5m, 1h, 1j
        :param start_timestamp: L'horodatage de début des données que vous souhaitez vérifier
        :param end_timestamp: L'horodatage du dernier point de données que vous souhaitez vérifier
        """
        # On check la première data dispo sur le CEX

        await self.exchange.close()

        if os.path.isfile(file_name):
            df = pd.read_csv(file_name, index_col=0, parse_dates=True)
            df.index = pd.to_datetime(df.index, unit='ms')
            df = df.groupby(df.index).first()

            if pytz.utc.localize(df.index[-1]) >= last_dt:
                return False
        else:
            # Le fichier n'existe pas, on renvoie la date de début
            return datetime.fromisoformat('2017-01-01')

        return pytz.utc.localize(df.index[-2])

    def create_intervals(self, start_date, end_date, delta):
        """
        Étant donné une date de début, une date de fin et un delta de temps, créez une liste de tuples
        de la forme (start_date, end_date) où chaque tuple représente un intervalle de temps de longueur
        delta

        :param start_date: La date de début de l'intervalle
        :param end_date: La date de fin de l'intervalle
        :param delta: un objet timedelta qui représente la longueur de l'intervalle
        """
        current = start_date
        while current <= end_date:
            yield current
            current += delta

    def create_timedelta(self, interval):
        """
        Retourne un timedelta en fonction de l'intervalle donné

        :param interval: L'intervalle de temps à utiliser dans timedelta
        """
        try:
            return self.intervals_dict[interval]["timedelta"]
        except Exception:
            raise ValueError(f"Intervalle {interval} inconnu")
        
    def explore_data(self): 
        files_data = []
        for path, subdirs, files in os.walk(self.path_download):
            for name in files:
                if os.path.join(path, name).endswith('.csv'):
                    current_file = os.path.join(path, name)
                    file_split = current_file.split("\\")
                    try:
                        df_file = pd.read_csv(current_file)
                    except Exception:
                        continue

                    files_data.append({
                        "exchange": file_split[1],
                        "timeframe": file_split[2],
                        "pair": file_split[3][:-4],
                        "occurences": len(df_file),
                        "start_date": str(datetime.fromtimestamp(df_file.iloc[0]["date"]/1000)),
                        "end_date": str(datetime.fromtimestamp(df_file.iloc[-1]["date"]/1000))
                    })
                    
        return pd.DataFrame(files_data)        


class TooManyError(Exception):
    pass
