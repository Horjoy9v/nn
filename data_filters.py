import pandas as pd
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [Data Filters] %(message)s')

def filter_incomplete_indicator_data(df: pd.DataFrame, included_indicators: dict) -> pd.DataFrame:

    if df.empty:
        logging.warning("filter_incomplete_indicator_data: Вхідний DataFrame порожній.")
        return df

    df_filtered = df.copy()
    initial_rows = len(df_filtered)
    columns_to_check = []

    if included_indicators.get('MA', False):
        if 'SMA_20' in df_filtered.columns:
            columns_to_check.append('SMA_20')
        if 'EMA_20' in df_filtered.columns:
            columns_to_check.append('EMA_20')

    if included_indicators.get('BB', False):
        if 'BBM_20_2.0' in df_filtered.columns:
            columns_to_check.append('BBM_20_2.0')
        if 'BBU_20_2.0' in df_filtered.columns:
            columns_to_check.append('BBU_20_2.0')
        if 'BBL_20_2.0' in df_filtered.columns:
            columns_to_check.append('BBL_20_2.0')

    if included_indicators.get('RSI', False):
        if 'RSI_14' in df_filtered.columns:
            columns_to_check.append('RSI_14')
            
    if not columns_to_check:
        logging.info("filter_incomplete_indicator_data: Жодні індикатори не були включені або відповідні колонки відсутні. Фільтрація не застосовується.")
        return df_filtered

    columns_to_check = list(set(columns_to_check))

    df_filtered.dropna(subset=columns_to_check, inplace=True)
    
    rows_removed = initial_rows - len(df_filtered)
    if rows_removed > 0:
        logging.info(f"filter_incomplete_indicator_data: Видалено {rows_removed} рядків з неповними даними індикаторів ({', '.join(columns_to_check)}).")
    else:
        logging.info("filter_incomplete_indicator_data: Рядків з неповними даними індикаторів не знайдено.")

    return df_filtered