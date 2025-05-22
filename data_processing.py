import pandas as pd
import numpy as np
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [Data Processing] %(message)s')

def resample_dataframe(df: pd.DataFrame, max_candles: int = 200) -> pd.DataFrame:
    if df.empty:
        logging.warning("[Data Processing] resample_dataframe: Вхідний DataFrame порожній.")
        return df

    current_candles = len(df)
    logging.info(f"[Data Processing] resample_dataframe: Поточна кількість свічок: {current_candles}. Максимум для відображення: {max_candles}.")

    if current_candles <= max_candles:
        logging.info("[Data Processing] resample_dataframe: Кількість свічок в межах ліміту, ресемплінг не потрібен.")
        return df

    resample_factor = np.ceil(current_candles / max_candles).astype(int)
    logging.info(f"[Data Processing] resample_dataframe: Коефіцієнт ресемплінгу: {resample_factor} (свічок на агреговану свічку).")

    df_temp = df.reset_index() 
    df_temp['group_id'] = df_temp.index // resample_factor

    agg_dict = {
        'timestamp': 'first', 
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum'
    }

    fixed_cols = ['timestamp', 'open', 'high', 'low', 'close', 'volume', 'group_id']
    if 'turnover' in df_temp.columns and 'turnover' not in agg_dict:
        agg_dict['turnover'] = 'sum'

    indicator_columns = [col for col in df_temp.columns if col not in fixed_cols and col not in agg_dict]
    
    for col in indicator_columns:
        agg_dict[col] = 'last' 

    resampled_df = df_temp.groupby('group_id').agg(
        **{k: (k, v) for k, v in agg_dict.items()}
    )
    
    resampled_df.set_index('timestamp', inplace=True)
    resampled_df.sort_index(inplace=True) 
    
    logging.info(f"[Data Processing] resample_dataframe: Завершено ресемплінг. Нова кількість свічок: {len(resampled_df)}. Колонок: {resampled_df.columns.tolist()}")
    
    return resampled_df