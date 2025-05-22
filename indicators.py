import pandas as pd
import numpy as np

def calculate_technical_indicators(df: pd.DataFrame, include_ma: bool, include_bb: bool, include_rsi: bool) -> pd.DataFrame:
    if df.empty:
        return df

    df_copy = df.copy()

    if include_ma:
        df_copy['SMA_20'] = df_copy['close'].rolling(window=20).mean()
        df_copy['EMA_20'] = df_copy['close'].ewm(span=20, adjust=False).mean()

    if include_bb:
        window = 20
        num_std_dev = 2.0
        df_copy['BBM_20_2.0'] = df_copy['close'].rolling(window=window).mean()
        rolling_std = df_copy['close'].rolling(window=window).std()
        df_copy['BBU_20_2.0'] = df_copy['BBM_20_2.0'] + (rolling_std * num_std_dev)
        df_copy['BBL_20_2.0'] = df_copy['BBM_20_2.0'] - (rolling_std * num_std_dev)

    if include_rsi:
        window = 14
        delta = df_copy['close'].diff(1)
        gain = delta.where(delta > 0, 0)
        loss = -delta.where(delta < 0, 0)

        avg_gain = gain.ewm(span=window, adjust=False).mean()
        avg_loss = loss.ewm(span=window, adjust=False).mean()

        rs = avg_gain / avg_loss
        df_copy['RSI_14'] = 100 - (100 / (1 + rs))
        df_copy['RSI_14'].replace([np.inf, -np.inf], np.nan, inplace=True)

    return df_copy

#source bybit_app_venv/bin/activate