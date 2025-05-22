import requests
import time
import pandas as pd
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [Bybit API] %(message)s')

def get_bybit_kline_data_raw(
    category: str,
    symbol: str,
    interval: str,
    start_timestamp: int = None,
    end_timestamp: int = None,
    limit: int = 1000,
    max_retries: int = 3,
    delay_between_retries: float = 0.05,
    request_timeout: int = 15
) -> list:
    """
    Отримує сирі дані свічок (kline) з Bybit API.
    """
    base_url = "https://api.bybit.com/v5/market/kline"
    
    params = {
        "category": category,
        "symbol": symbol,
        "interval": interval,
        "limit": limit
    }
    if start_timestamp is not None:
        params["start"] = start_timestamp
    if end_timestamp is not None:
        params["end"] = end_timestamp

    for attempt in range(max_retries):
        try:
            logging.info(f"[Bybit API] Спроба {attempt + 1}/{max_retries}: Запит до {base_url} з параметрами {params}")
            response = requests.get(base_url, params=params, timeout=request_timeout) 
            response.raise_for_status()
            data = response.json()

            if data["retCode"] == 0:
                logging.info(f"[Bybit API] Успішно отримано {len(data['result']['list'])} свічок.")
                return data["result"]["list"]
            else:
                logging.error(f"[Bybit API] Помилка Bybit API (retCode: {data['retCode']}): {data['retMsg']}")
                if attempt < max_retries - 1:
                    time.sleep(delay_between_retries)
                else:
                    return []

        except requests.exceptions.Timeout as e:
            logging.error(f"[Bybit API] Помилка таймауту запиту: {e}. Спроба {attempt + 1}/{max_retries}")
            if attempt < max_retries - 1:
                time.sleep(delay_between_retries)
        except requests.exceptions.RequestException as e:
            logging.error(f"[Bybit API] Мережева помилка запиту: {e}. Спроба {attempt + 1}/{max_retries}")
            if attempt < max_retries - 1:
                time.sleep(delay_between_retries)
        except Exception as e:
            logging.error(f"[Bybit API] Невідома помилка під час запиту: {e}. Спроба {attempt + 1}/{max_retries}", exc_info=True)
            if attempt < max_retries - 1:
                time.sleep(delay_between_retries)
    
    logging.error("[Bybit API] Всі спроби запиту до Bybit API невдалі. Повертаю порожній список.")
    return []

def parse_kline_data_to_df(kline_data_raw: list) -> pd.DataFrame:
    """
    Парсить сирі дані kline у Pandas DataFrame.
    """
    if not kline_data_raw:
        logging.warning("[Bybit API] Порожні сирі дані для парсингу.")
        return pd.DataFrame()

    df = pd.DataFrame(kline_data_raw, columns=[
        'timestamp', 'open', 'high', 'low', 'close', 'volume', 'turnover'
    ])
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
    df.set_index('timestamp', inplace=True)
    
    numeric_cols = ['open', 'high', 'low', 'close', 'volume', 'turnover']
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    
    df.sort_index(inplace=True)
    
    logging.info(f"[Bybit API] Успішно розпарсено {len(df)} свічок у DataFrame.")
    return df