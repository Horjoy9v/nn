import time
import pandas as pd
import numpy as np
import logging
import mplfinance as mpf
from datetime import datetime

from PyQt6.QtCore import Qt, QThread, pyqtSignal, QObject

from bybit_api import get_bybit_kline_data_raw, parse_kline_data_to_df
from data_processing import resample_dataframe
from indicators import calculate_technical_indicators
from matplotlib.figure import Figure

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [Threads] %(message)s')


class DownloadThread(QThread):
    progress = pyqtSignal(int)
    finished = pyqtSignal(pd.DataFrame)
    error = pyqtSignal(str)
    message = pyqtSignal(str) 

    def __init__(self, category: str, symbol: str, interval: str, 
                 start_time_ms: int, end_time_ms: int):
        super().__init__()
        self.category = category
        self.symbol = symbol
        self.interval = interval
        self.start_time_ms = start_time_ms
        self.end_time_ms = end_time_ms
        self.kline_limit = 1000 
        self._is_running = True
        logging.info("DownloadThread.__init__: Ініціалізація потоку завантаження завершена.")

    def stop(self):
        self._is_running = False
        logging.info("DownloadThread.stop(): Отримано запит на зупинку.")

    def run(self): 
        logging.info("DownloadThread.run(): Метод run почав виконуватися.")
        try:
            self.message.emit("Початок завантаження даних...")
            logging.info(f"DownloadThread: Початок завантаження для {self.symbol} ({self.interval}) "
                         f"з {datetime.fromtimestamp(self.start_time_ms / 1000)} "
                         f"до {datetime.fromtimestamp(self.end_time_ms / 1000)}")

            all_kline_data_raw = []
            current_end_time = self.end_time_ms
            
            downloaded_candles_count = 0
            
            while self._is_running:
                logging.info(f"DownloadThread: Запит свічок до {datetime.fromtimestamp(current_end_time / 1000)}")
                
                kline_batch = get_bybit_kline_data_raw(
                    category=self.category,
                    symbol=self.symbol,
                    interval=self.interval,
                    end_timestamp=current_end_time,
                    limit=self.kline_limit,
                    request_timeout=15 
                )

                if not kline_batch:
                    self.message.emit(f"API повернув порожні дані або помилку. Завантажено {len(all_kline_data_raw)} свічок. Завершення.")
                    logging.info(f"DownloadThread: Bybit API повернув порожні дані або помилку на end_timestamp: {datetime.fromtimestamp(current_end_time / 1000)}. Завершення завантаження.")
                    break

                new_candles_in_batch = []
                for candle in kline_batch:
                    candle_timestamp = int(candle[0])
                    if candle_timestamp >= self.start_time_ms:
                        new_candles_in_batch.append(candle)
                    else:
                        new_candles_in_batch = [c for c in new_candles_in_batch if int(c[0]) >= self.start_time_ms]
                        self._is_running = False 
                        break

                existing_timestamps = {int(c[0]) for c in all_kline_data_raw}
                for candle in new_candles_in_batch:
                    if int(candle[0]) not in existing_timestamps:
                        all_kline_data_raw.append(candle)

                all_kline_data_raw.sort(key=lambda x: int(x[0]))
                
                downloaded_candles_count = len(all_kline_data_raw)
                logging.debug(f"DownloadThread: Додано свічок у батчі: {len(new_candles_in_batch)}. Усього свічок: {downloaded_candles_count}")

                if all_kline_data_raw:
                    overall_oldest_timestamp = int(all_kline_data_raw[0][0])
                    overall_newest_timestamp = int(all_kline_data_raw[-1][0])

                    target_range = self.end_time_ms - self.start_time_ms
                    
                    covered_range = min(self.end_time_ms, overall_newest_timestamp) - max(self.start_time_ms, overall_oldest_timestamp)
                    
                    if target_range > 0 and covered_range > 0:
                        progress_percentage = min(100, int((covered_range / target_range) * 100))
                        self.progress.emit(progress_percentage)
                        self.message.emit(f"Завантаження: {progress_percentage}% ({downloaded_candles_count} свічок)")
                    elif target_range == 0:
                        self.progress.emit(100)
                        self.message.emit(f"Завантаження: 100% ({downloaded_candles_count} свічок)")
                    else: 
                        self.progress.emit(0) 
                        self.message.emit(f"Завантаження: Прогрес не визначено ({downloaded_candles_count} свічок)")

                if not kline_batch or int(kline_batch[-1][0]) <= self.start_time_ms:
                    self.message.emit(f"Досягнуто початкового часу. Завантажено {len(all_kline_data_raw)} свічок. Завершення.")
                    logging.info(f"DownloadThread: Досягнуто початкового часу {datetime.fromtimestamp(self.start_time_ms / 1000)}. Завершення завантаження.")
                    self._is_running = False 
                
                if kline_batch:
                    current_end_time = int(kline_batch[-1][0]) - 1 
                else:
                    break 

                if not self._is_running:
                    break
                
                time.sleep(0.1) 

            df = parse_kline_data_to_df(all_kline_data_raw)
            
            self.message.emit(f"Завантаження даних завершено. Усього {len(df)} свічок.")
            logging.info(f"DownloadThread: Завантаження даних завершено. Усього {len(df)} свічок.")
            self.finished.emit(df)

        except Exception as e:
            error_message = f"Сталася критична помилка при завантаженні даних: {e}"
            self.message.emit(error_message)
            logging.error(f"DownloadThread: {error_message}", exc_info=True) 
            self.error.emit(error_message)


class IndicatorsCalculationThread(QThread):
    finished = pyqtSignal(pd.DataFrame)
    error = pyqtSignal(str)
    message = pyqtSignal(str)

    def __init__(self, df: pd.DataFrame, include_ma: bool, include_bb: bool, include_rsi: bool):
        super().__init__()
        self.data_df = df
        self.include_ma = include_ma
        self.include_bb = include_bb
        self.include_rsi = include_rsi
        logging.info("IndicatorsCalculationThread.__init__: Ініціалізація потоку розрахунку індикаторів завершена.")

    def run(self):
        logging.info("IndicatorsCalculationThread.run(): Метод run почав виконуватися.")
        try:
            self.message.emit("Розрахунок індикаторів...")
            logging.info(f"IndicatorsCalculationThread: Початок розрахунку індикаторів для {len(self.data_df)} свічок.")

            df_with_indicators = calculate_technical_indicators(
                self.data_df.copy(),
                self.include_ma,
                self.include_bb,
                self.include_rsi
            )
            
            self.message.emit("Розрахунок індикаторів завершено.")
            logging.info("IndicatorsCalculationThread: Розрахунок індикаторів завершено.")
            self.finished.emit(df_with_indicators)

        except Exception as e:
            error_message = f"Сталася помилка при розрахунку індикаторів: {e}"
            self.message.emit(error_message)
            logging.error(f"IndicatorsCalculationThread: {error_message}", exc_info=True)
            self.error.emit(error_message)


class ChartRenderThread(QThread):
    finished = pyqtSignal(object, object) 
    error = pyqtSignal(str)
    message = pyqtSignal(str) 

    def __init__(self, df_with_indicators: pd.DataFrame, include_ma: bool, include_bb: bool, include_rsi: bool, symbol_text: str, max_display_candles: int = 200): 
        super().__init__()
        self.data_df_full_with_indicators = df_with_indicators
        self.include_ma = include_ma
        self.include_bb = include_bb
        self.include_rsi = include_rsi
        self.symbol_text = symbol_text
        self.max_display_candles = max_display_candles 
        logging.info("ChartRenderThread.__init__: Ініціалізація потоку побудови графіків завершена.")

    def run(self): 
        logging.info("ChartRenderThread.run(): Метод run почав виконуватися.")
        try:
            self.message.emit("Початок побудови графіків...")
            logging.info("ChartRenderThread: Початок побудови графіків.")
            fig_mpf = None
            fig_rsi = None

            if self.data_df_full_with_indicators.empty:
                self.message.emit("Немає даних для побудови графіків.")
                logging.warning("ChartRenderThread: Немає даних для побудови графіків.")
                self.finished.emit(fig_mpf, fig_rsi)
                return

            df_to_plot = resample_dataframe(self.data_df_full_with_indicators, self.max_display_candles)

            if len(self.data_df_full_with_indicators) > self.max_display_candles:
                self.message.emit(f"Побудова графіків для агрегованих {len(df_to_plot)} свічок (з {len(self.data_df_full_with_indicators)} завантажених)...")
            else:
                self.message.emit(f"Побудова графіків для {len(df_to_plot)} свічок...")

            fluent_dark_style = {
                "base_mpl_style": "dark_background",
                "marketcolors": {
                    "candle": {"up": "#3dc985", "down": "#ef4f60"},
                    "edge": {"up": "#3dc985", "down": "#ef4f60"},
                    "wick": {"up": "#3dc985", "down": "#ef4f60"},
                    "ohlc": {"up": "green", "down": "red"},
                    "volume": {"up": "#247252", "down": "#82333f"},
                    "vcedge": {"up": "green", "down": "red"},
                    "vcdopcod": False,
                    "alpha": 1,
                },
                "mavcolors": ["#ad7739", "#a63ab2", "#62b8ba"],
                "facecolor": "#1b1f24",
                "gridcolor": "#2c2e31",
                "gridstyle": "--",
                "y_on_right": True,
                "rc": {
                    "axes.grid": True,
                    "axes.grid.axis": "y",
                    "axes.edgecolor": "#474d56",
                    "axes.labelcolor": "lightgray",
                    "xtick.color": "lightgray",
                    "ytick.color": "lightgray",
                    "axes.titlecolor": "lightgray",
                    "figure.facecolor": "#161a1e",
                    "figure.titlesize": "large",
                    "figure.titleweight": "normal",
                    "legend.labelcolor": "lightgray",
                    "axes.linewidth": 0.5,
                    "grid.linewidth": 0.5,
                },
                "style_name": "fluent_dark_style",
            }
            s = mpf.make_mpf_style(**fluent_dark_style)

            apds = []
            legend_labels = []

            if self.include_ma:
                if 'SMA_20' in df_to_plot.columns:
                    apds.append(mpf.make_addplot(df_to_plot['SMA_20'], color='blue', panel=0, type='line', width=0.7, secondary_y=False))
                    legend_labels.append('SMA 20')
                if 'EMA_20' in df_to_plot.columns:
                    apds.append(mpf.make_addplot(df_to_plot['EMA_20'], color='lime', panel=0, type='line', width=0.7, secondary_y=False))
                    legend_labels.append('EMA 20')
            
            if self.include_bb:
                if 'BBL_20_2.0' in df_to_plot.columns and 'BBM_20_2.0' in df_to_plot.columns and 'BBU_20_2.0' in df_to_plot.columns:
                    apds.append(mpf.make_addplot(df_to_plot['BBU_20_2.0'], color='red', linestyle='--', panel=0, type='line', width=1.0, secondary_y=False))
                    legend_labels.append('BB Upper')
                    apds.append(mpf.make_addplot(df_to_plot['BBM_20_2.0'], color='red', panel=0, type='line', width=1.0, secondary_y=False))
                    legend_labels.append('BB Middle')
                    apds.append(mpf.make_addplot(df_to_plot['BBL_20_2.0'], color='red', linestyle='--', panel=0, type='line', width=1.0, secondary_y=False))
                    legend_labels.append('BB Lower')

            fig_mpf, axes_mpf = mpf.plot(
                df_to_plot,
                type='candle',
                style=s,
                addplot=apds,
                volume=True,
                figscale=1.5,
                ylabel='Ціна',
                ylabel_lower='Обсяг',
                show_nontrading=False,
                returnfig=True,
                panel_ratios=(6, 1),
                tight_layout=True,
                figratio=(10, 7),
                datetime_format='%b %d, %H:%M',
                xrotation=30,
            )
            
            if axes_mpf is not None and isinstance(axes_mpf, (tuple, list)):
                price_ax = axes_mpf[0]
                
                from matplotlib.lines import Line2D
                custom_handles = []
                ma_bb_colors = {
                    'SMA 20': 'blue',
                    'EMA 20': 'lime',
                    'BB Upper': 'red',
                    'BB Middle': 'red',
                    'BB Lower': 'red'
                }
                
                for label in legend_labels:
                    color = ma_bb_colors.get(label, 'gray')
                    linestyle = '--' if 'Upper' in label or 'Lower' in label else '-'
                    custom_handles.append(Line2D([0], [0], color=color, linestyle=linestyle, lw=1))
                
                if custom_handles:
                    price_ax.legend(handles=custom_handles, labels=legend_labels, loc='best', frameon=False, fontsize='small', labelcolor='lightgray')

            if self.include_rsi and 'RSI_14' in df_to_plot.columns:
                fig_rsi = Figure(figsize=(10, 2))
                ax_rsi = fig_rsi.add_subplot(111)

                fig_rsi.set_facecolor(fluent_dark_style["rc"]["figure.facecolor"])
                ax_rsi.set_facecolor(fluent_dark_style["facecolor"])
                
                ax_rsi.tick_params(axis='x', colors=fluent_dark_style["rc"]["xtick.color"])
                ax_rsi.tick_params(axis='y', colors=fluent_dark_style["rc"]["ytick.color"])
                
                for spine in ax_rsi.spines.values():
                    spine.set_edgecolor(fluent_dark_style["rc"]["axes.edgecolor"])
                    spine.set_linewidth(fluent_dark_style["rc"]["axes.linewidth"])

                ax_rsi.set_xlabel("Дата", color=fluent_dark_style["rc"]["axes.labelcolor"])
                ax_rsi.set_ylabel("RSI", color=fluent_dark_style["rc"]["axes.labelcolor"])
                ax_rsi.set_title(f"RSI for {self.symbol_text}", color=fluent_dark_style["rc"]["axes.titlecolor"])
                ax_rsi.grid(True, linestyle=fluent_dark_style["gridstyle"], color=fluent_dark_style["gridcolor"], linewidth=fluent_dark_style["rc"]["grid.linewidth"])

                ax_rsi.plot(df_to_plot.index, df_to_plot['RSI_14'], color='purple', label='RSI (14)')
                ax_rsi.axhline(70, color='red', linestyle='--', linewidth=0.7)
                ax_rsi.axhline(30, color='green', linestyle='--', linewidth=0.7)
                ax_rsi.fill_between(df_to_plot.index, df_to_plot['RSI_14'], 70, where=df_to_plot['RSI_14'] >= 70, color='red', alpha=0.3)
                ax_rsi.fill_between(df_to_plot.index, df_to_plot['RSI_14'], 30, where=df_to_plot['RSI_14'] <= 30, color='green', alpha=0.3)
                
                ax_rsi.legend(loc='best', frameon=False, fontsize='small', labelcolor=fluent_dark_style["rc"]["legend.labelcolor"])
                fig_rsi.tight_layout()
            
            self.message.emit("Побудова графіків завершена.")
            logging.info("ChartRenderThread: Побудова графіків завершена.")
            self.finished.emit(fig_mpf, fig_rsi)

        except Exception as e:
            error_message = f"Сталася помилка при побудові графіків: {e}"
            self.message.emit(error_message)
            logging.error(f"ChartRenderThread: {error_message}", exc_info=True)
            self.error.emit(error_message)