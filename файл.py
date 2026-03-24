import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import requests
import time
from datetime import datetime, timedelta

TIKER = 'LKOH'
START_DAY = '2025-10-01'
END_DATE = '2025-11-09'



class VolumeData():
    
    def __init__(self, start_date, end_date, tikers, interval):
        self.start_data = start_date
        self.end_data = end_date
        self.tikers = tikers
        self.interval = interval
    
    ## Выгрузка данных и реализация перехода через контракт.
    base_assets_tikers_dict = {}

    MONTH_CODES = {
        1: 'F', 2: 'G', 3: 'H', 4: 'J', 5: 'K', 6: 'M',
        7: 'N', 8: 'Q', 9: 'U', 10: 'V', 11: 'X', 12: 'Z'
    }

    PERPETUAL_CONTRACTS = {
        'USDRUBF': 'USDRUBF',
        'CNYRUBF': 'CNYRUBF', 
        'EURRUBF': 'EURRUBF',
        'IMOEXF': 'IMOEXF',
        'SBERF': 'SBERF',
        'GAZPF': 'GAZPF',
        'GLDRUBF': 'GLDRUBF'
    }

    ## ОБРАБОТКА И НАХОЖДЕНИЕ ВСЕХ АКТИВОВ
    def safe_request(self, url, retries=10, delay=3):
        for attempt in range(retries):
            try:
                return requests.get(url, timeout=10)
            except:
                if attempt < retries:
                    print(111111)
                    time.sleep(delay)
                else:
                    raise
    
    def get_short_secid_code(self):
        if self.tikers == "all":
            return self._get_all_assets()
        elif isinstance(self.tikers, str):
            return self._get_str_asset()
        else:
            raise ValueError("Неправильный формат тикеров. Используйте 'all_assets', строку с тикером или список тикеров")
    
    def _get_str_asset(self):
        try:
            self.tikers = [ticker.strip() for ticker in self.tikers.split(',')]
        except:
            print('Неправильный формат ввода. Тикеры должны быть записаны через ", " ')
            return []
        
        link = "https://iss.moex.com/iss/engines/futures/markets/forts/securities.json?securities.columns=SECID,ASSETCODE"
        response = requests.get(link)
        data = response.json()
        columns = data['securities']['columns']
        row = data['securities']['data']
        df = pd.DataFrame(row, columns=columns)
        df['BASE_ASSET'] = df['SECID'].str[:2]
        self.base_assets_tikers_dict = df.set_index('BASE_ASSET')['ASSETCODE'].to_dict()
        tikers = df.loc[df['ASSETCODE'].isin(self.tikers)].apply(lambda x: x['ASSETCODE'] if x['ASSETCODE'] in self.PERPETUAL_CONTRACTS else x['BASE_ASSET'], axis=1).tolist()
        tikers = list(set(tikers))
        self.tikers = tikers
        return tikers
        
    def _get_all_assets(self):
        link = "https://iss.moex.com/iss/engines/futures/markets/forts/securities.json?securities.columns=SECID,ASSETCODE"
        response = requests.get(link)
        data = response.json()
        columns = data['securities']['columns']
        row = data['securities']['data']
        df = pd.DataFrame(row, columns=columns)
        df['BASE_ASSET'] = df['SECID'].str[:2]
        self.base_assets_tikers_dict = df.set_index('BASE_ASSET')['ASSETCODE'].to_dict()
        assets = df.apply(lambda x: x['SECID'] if x['SECID'] in self.PERPETUAL_CONTRACTS else x['BASE_ASSET'], axis=1).unique().tolist()
        return df['BASE_ASSET'].unique().tolist()
    

    ## ПАРСИНГ ДАННЫХ С УЧЕТОМ ВВОДИМЫХ ПАРАМЕТРОВ
    def get_data(self):
        df_all = []
        short_assets_code = self.get_short_secid_code()

        for short_secid in short_assets_code:
            print(short_secid)
            if short_secid in self.PERPETUAL_CONTRACTS:
                df_perpetual = self.try_load_candles(secid = short_secid, start_date = self.start_data)
                df_perpetual['ticker'] = short_secid
                df_all.append(df_perpetual)
            else:
                current_date = self.start_data

                secid_df = []
                ## Перебираем все даты до окончания
                while current_date < self.end_data:

                    year, month, day = current_date.split('-')
                    print(str(current_date)+ ' текущий день')
                    month = int(month)
                    year = int(year)
                    ## Оставляем переход через год
                    while month < 13:
                        secid = self.get_contract_code(base = short_secid, month = month, year = year)
                        df = self.try_load_candles(secid = secid, start_date = current_date)
                        if not df.empty:
                            df['ticker'] = secid
                            df['base_asset'] = self.base_assets_tikers_dict[secid[:2]]
                            secid_df.append(df)
                            df['end'] = pd.to_datetime(df['end'])
                            last_date = df['end'].dt.date.max()
                            print(f"  Найдены данные с {df['end'].dt.date.min()} по {last_date}")
                            current_date = (last_date + timedelta(days=1)).strftime('%Y-%m-%d')
                            print(str(current_date)+ ' текущий день после изменения')
                        else:
                            if current_date >= self.end_data:
                                break
                            elif month + 1 < 13:
                                month += 1
                            elif year > int(self.end_data.split('-')[0]) and month > int(self.end_data.split('-')[1]):
                                break
                            else:
                                year += 1
                                month = 1
                    if current_date >= self.end_data:
                        break
                    elif year > int(self.end_data.split('-')[0]) and month > int(self.end_data.split('-')[1]):
                                break
                try:
                    secid_concat = pd.concat(secid_df, ignore_index=True)
                    df_all.append(secid_concat)
                except:
                    df_all.append(pd.DataFrame())
        return df_all

    def try_load_candles(self, secid, start_date):
        all_data = []
        start_idx = 0
        while True:
            link = f'https://iss.moex.com/iss/engines/futures/markets/forts/boards/RFUD/securities/{secid}/candles.json?from={start_date}&till={self.end_data}&interval={self.interval}&start={start_idx}'
            response = self.safe_request(link)
            if response is None:
                break
            data = response.json()
            columns = data['candles']['columns']
            rows = data['candles']['data']
            if not rows:
                break
            df = pd.DataFrame(rows, columns=columns)
            df['close'] = pd.to_numeric(df['close'], errors='coerce')
            df['volume'] = pd.to_numeric(df['volume'], errors='coerce')
            df = df.dropna(subset=['close', 'volume'])
            df = df.drop(['open', 'begin', 'value', 'low', 'high'], axis=1)
            all_data.append(df)
            if len(rows) < 500:
                break
            start_idx += 500
        if all_data:
            return pd.concat(all_data, ignore_index=True)
        else:
            return pd.DataFrame()
    
    def get_contract_code(self, base, year, month):
        month_code = self.MONTH_CODES[month]
        year_code = str(year)[-1]
        return f'{base}{month_code}{year_code}'
    
    def get_df_volume(self):
        dfs = self.get_data()
        df_all = pd.concat(dfs, axis = 0)
        return df_all

volume = VolumeData(
    start_date = START_DAY,
    end_date = END_DATE,
    tikers = TIKER,
    interval = 1
)
df_volumes = volume.get_df_volume()

df_volumes['end'] = pd.to_datetime(df_volumes['end'])
df_volumes['time_only'] = df_volumes['end'].dt.time  
df_volumes['date_only'] = df_volumes['end'].dt.date  
df_volumes['date_only'] = pd.to_datetime(df_volumes['date_only'])
df_weekdays = df_volumes[df_volumes['date_only'].dt.weekday < 5]

df_weekdays.to_excel(f'volume_{TIKER}_{START_DAY}-{END_DATE}.xlsx')








