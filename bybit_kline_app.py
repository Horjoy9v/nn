import time
from datetime import datetime
import pandas as pd
import logging
import matplotlib.pyplot as plt

from PyQt6.QtWidgets import (
    QApplication, QWidget, QVBoxLayout, QHBoxLayout, QGridLayout,
    QFileDialog 
)
from PyQt6.QtGui import QIntValidator
from PyQt6.QtCore import Qt, pyqtSignal

from qfluentwidgets import (
    LineEdit, PrimaryPushButton, CheckBox, ComboBox,
    BodyLabel, MessageBox, ProgressBar
)
from qfluentwidgets.components.widgets.card_widget import CardWidget

from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.figure import Figure
import mplfinance as mpf

from bybit_api import parse_kline_data_to_df
from data_processing import resample_dataframe
from threads import DownloadThread, IndicatorsCalculationThread, ChartRenderThread

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [Bybit API] %(message)s')


class BybitKlineApp(QWidget):
    data_loaded_signal = pyqtSignal(pd.DataFrame)

    def __init__(self, parent=None):
        super().__init__(parent=parent)
        self.full_data_df = pd.DataFrame()
        self.price_chart_layout = QVBoxLayout()
        self.rsi_chart_layout = QVBoxLayout()
        self.setObjectName("Bybit-Kline-App-Interface")
        self.initUI()

        self.download_thread = None
        self.calc_indicators_thread = None 
        self.render_charts_thread = None
        logging.info("BybitKlineApp: Ініціалізація інтерфейсу користувача.")


    def initUI(self):
        main_layout = QHBoxLayout(self)

        control_panel_layout = QVBoxLayout()
        control_panel_layout.setAlignment(Qt.AlignmentFlag.AlignTop)
        control_panel_layout.setSpacing(20)

        request_card_container = QVBoxLayout()
        request_card_container.addWidget(BodyLabel("Параметри запиту", parent=self))
        request_card_container.setAlignment(Qt.AlignmentFlag.AlignTop)
        
        request_card = CardWidget(parent=self)
        request_layout = QGridLayout()
        request_card.setLayout(request_layout)
        
        request_layout.addWidget(BodyLabel("Символ:", parent=self), 0, 0, alignment=Qt.AlignmentFlag.AlignRight)
        self.symbol_input = LineEdit(parent=self)
        self.symbol_input.setText("BTCUSDT")
        request_layout.addWidget(self.symbol_input, 0, 1)

        request_layout.addWidget(BodyLabel("Інтервал:", parent=self), 1, 0, alignment=Qt.AlignmentFlag.AlignRight)
        self.interval_combo = ComboBox(parent=self)
        self.interval_combo.addItems(['1', '3', '5', '15', '30', '60', '120', '240', '360', '720', 'D', 'W', 'M'])
        self.interval_combo.setCurrentText('60')
        request_layout.addWidget(self.interval_combo, 1, 1)

        request_layout.addWidget(BodyLabel("Кількість днів (макс. 365):", parent=self), 2, 0, alignment=Qt.AlignmentFlag.AlignRight)
        self.days_input = LineEdit(parent=self)
        self.days_input.setText("7")
        self.days_input.setValidator(QIntValidator(1, 365))
        request_layout.addWidget(self.days_input, 2, 1)

        request_layout.addWidget(BodyLabel("Макс. свічок на графіку:", parent=self), 3, 0, alignment=Qt.AlignmentFlag.AlignRight)
        self.max_candles_input = LineEdit(parent=self)
        self.max_candles_input.setText("200") 
        self.max_candles_input.setValidator(QIntValidator(50, 1000)) 
        request_layout.addWidget(self.max_candles_input, 3, 1)

        request_card_container.addWidget(request_card)
        control_panel_layout.addLayout(request_card_container)

        indicators_card_container = QVBoxLayout()
        indicators_card_container.addWidget(BodyLabel("Обрахувати індикатори", parent=self))
        indicators_card_container.setAlignment(Qt.AlignmentFlag.AlignTop)

        indicators_card = CardWidget(parent=self)
        indicators_layout = QVBoxLayout()
        indicators_card.setLayout(indicators_layout)

        self.checkbox_ma = CheckBox("Ковзна середня (SMA/EMA)", parent=self)
        self.checkbox_bb = CheckBox("Смуги Боллінджера", parent=self)
        self.checkbox_rsi = CheckBox("RSI", parent=self)
        indicators_layout.addWidget(self.checkbox_ma)
        indicators_layout.addWidget(self.checkbox_bb)
        indicators_layout.addWidget(self.checkbox_rsi)

        indicators_card_container.addWidget(indicators_card)
        control_panel_layout.addLayout(indicators_card_container)

        self.download_button = PrimaryPushButton("Завантажити дані", parent=self)
        control_panel_layout.addWidget(self.download_button)
        self.download_button.clicked.connect(self.start_processing_pipeline)

        self.progress_bar = ProgressBar(self)
        self.progress_bar.setRange(0, 100)
        self.progress_bar.setValue(0)
        self.progress_bar.hide()
        control_panel_layout.addWidget(self.progress_bar)

        self.status_label = BodyLabel("Готовий", parent=self)
        control_panel_layout.addWidget(self.status_label)

        control_panel_layout.addStretch(1)

        main_layout.addLayout(control_panel_layout, 3)

        chart_panel_layout = QVBoxLayout()
        chart_panel_layout.setSpacing(10)

        self.price_chart_card = CardWidget(parent=self)
        self.price_chart_card.setLayout(self.price_chart_layout)
        chart_panel_layout.addWidget(self.price_chart_card, 4)

        self.rsi_chart_card = CardWidget(parent=self)
        self.rsi_chart_card.setLayout(self.rsi_chart_layout)
        chart_panel_layout.addWidget(self.rsi_chart_card, 1)

        main_layout.addLayout(chart_panel_layout, 7)

        self.update_charts_ui(None, None) 

    def start_processing_pipeline(self):
        logging.info("start_processing_pipeline: Функція була викликана.")
        symbol = self.symbol_input.text().upper()
        interval = self.interval_combo.currentText()
        try:
            days_to_download = int(self.days_input.text())
            if not (1 <= days_to_download <= 365):
                w = MessageBox(
                    "Помилка вводу",
                    "Кількість днів має бути від 1 до 365.",
                    self.window()
                )
                w.exec()
                return
            
            max_display_candles = int(self.max_candles_input.text())
            if not (50 <= max_display_candles <= 1000): 
                w = MessageBox(
                    "Помилка вводу",
                    "Максимальна кількість свічок має бути від 50 до 1000.",
                    self.window()
                )
                w.exec()
                return

        except ValueError:
            w = MessageBox(
                "Помилка вводу",
                "Кількість днів та макс. свічок повинні бути цілими числами.",
                self.window()
            )
            w.exec()
            return

        if interval in ['1', '3', '5']:
            if days_to_download > 90:
                w = MessageBox(
                    "Обмеження даних",
                    f"Для інтервалу {interval} хвилин доступна історія може бути обмежена. "
                    "Для значних обсягів даних можуть виникнути затримки або помилки. "
                    "Будь ласка, спробуйте менший діапазон днів (наприклад, до 90).",
                    self.window()
                )
                w.exec()

        end_time_ms = int(time.time() * 1000)
        start_time_ms = end_time_ms - (days_to_download * 24 * 60 * 60 * 1000)

        include_ma = self.checkbox_ma.isChecked()
        include_bb = self.checkbox_bb.isChecked()
        include_rsi = self.checkbox_rsi.isChecked()

        self.download_button.setEnabled(False)
        self.progress_bar.setValue(0)
        self.progress_bar.show()
        self.status_label.setText("Завантаження даних...")
        QApplication.instance().setOverrideCursor(Qt.CursorShape.WaitCursor)
        logging.info("start_processing_pipeline: Запускаю потік для завантаження даних.")
        
        self.download_thread = DownloadThread( 
            category="linear", 
            symbol=symbol,
            interval=interval,
            start_time_ms=start_time_ms,
            end_time_ms=end_time_ms
        )

        self.download_thread.finished.connect(
            lambda df: self.on_data_downloaded(df, include_ma, include_bb, include_rsi, symbol, max_display_candles)
        )
        self.download_thread.error.connect(self.on_processing_error)
        self.download_thread.progress.connect(self.progress_bar.setValue)
        self.download_thread.message.connect(self.status_label.setText) 

        self.download_thread.finished.connect(self.download_thread.quit)
        self.download_thread.finished.connect(self.download_thread.deleteLater)
        logging.info("start_processing_pipeline: Викликаю download_thread.start()...")
        self.download_thread.start()
        logging.info("start_processing_pipeline: download_thread.start() викликано.")

    def on_data_downloaded(self, df: pd.DataFrame, include_ma: bool, include_bb: bool, include_rsi: bool, symbol: str, max_display_candles: int):
        self.full_data_df = df.copy()
        if self.full_data_df.empty:
            self.on_processing_error("Дані не були завантажені або отримано порожній набір даних.")
            self.data_loaded_signal.emit(self.full_data_df)
            return

        self.status_label.setText("Розрахунок індикаторів...")
        self.progress_bar.setValue(0) 

        self.calc_indicators_thread = IndicatorsCalculationThread(
            df=self.full_data_df,
            include_ma=include_ma,
            include_bb=include_bb,
            include_rsi=include_rsi
        )

        self.calc_indicators_thread.finished.connect(
            lambda df_with_indicators: self.on_indicators_calculated(df_with_indicators, include_ma, include_bb, include_rsi, symbol, max_display_candles)
        )
        self.calc_indicators_thread.error.connect(self.on_processing_error)
        self.calc_indicators_thread.message.connect(self.status_label.setText) 

        self.calc_indicators_thread.finished.connect(self.calc_indicators_thread.quit)
        self.calc_indicators_thread.finished.connect(self.calc_indicators_thread.deleteLater)
        
        self.calc_indicators_thread.start()

    def on_indicators_calculated(self, df_with_indicators: pd.DataFrame, include_ma: bool, include_bb: bool, include_rsi: bool, symbol: str, max_display_candles: int):
        self.full_data_df = df_with_indicators
        self.data_loaded_signal.emit(self.full_data_df)

        self.status_label.setText("Побудова графіків...")
        self.progress_bar.setValue(0) 

        self.render_charts_thread = ChartRenderThread(
            df_with_indicators=self.full_data_df,
            include_ma=include_ma,
            include_bb=include_bb,
            include_rsi=include_rsi,
            symbol_text=symbol,
            max_display_candles=max_display_candles
        )

        self.render_charts_thread.finished.connect(self.on_charts_rendered)
        self.render_charts_thread.error.connect(self.on_processing_error)
        self.render_charts_thread.message.connect(self.status_label.setText) 

        self.render_charts_thread.finished.connect(self.render_charts_thread.quit)
        self.render_charts_thread.finished.connect(self.render_charts_thread.deleteLater)

        self.render_charts_thread.start()

    def on_charts_rendered(self, fig_mpf: Figure, fig_rsi: Figure):
        self.update_charts_ui(fig_mpf, fig_rsi)

        self.download_button.setEnabled(True)
        self.progress_bar.hide()
        self.status_label.setText("Готовий")
        QApplication.instance().restoreOverrideCursor()
        actual_displayed_candles = len(resample_dataframe(self.full_data_df, int(self.max_candles_input.text())))
        w = MessageBox(
            "Обробка завершена",
            f"Успішно завантажено {len(self.full_data_df)} свічок. "
            f"Відображено {actual_displayed_candles} свічок (після агрегації).",
            self.window()
        )
        w.exec()

    def on_processing_error(self, message: str):
        self.download_button.setEnabled(True)
        self.progress_bar.hide()
        self.status_label.setText("Помилка")
        QApplication.instance().restoreOverrideCursor()
        w = MessageBox(
            "Помилка обробки даних",
            message,
            self.window()
        )
        w.exec()
        self.full_data_df = pd.DataFrame()
        self.data_loaded_signal.emit(self.full_data_df)

    def _clear_layout(self, layout):
        if layout is not None:
            while layout.count():
                item = layout.takeAt(0)
                widget = item.widget()
                if widget is not None:
                    widget.deleteLater()
                else:
                    self._clear_layout(item.layout())
            plt.close('all')

    def update_charts_ui(self, fig_mpf, fig_rsi):
        self._clear_layout(self.price_chart_layout)
        self._clear_layout(self.rsi_chart_layout)

        if fig_mpf:
            canvas_price_new = FigureCanvas(fig_mpf)
            self.price_chart_layout.addWidget(canvas_price_new)
        else:
            empty_label_price = BodyLabel("Немає даних для відображення", parent=self)
            empty_label_price.setAlignment(Qt.AlignmentFlag.AlignCenter)
            self.price_chart_layout.addWidget(empty_label_price)

        if fig_rsi:
            canvas_rsi_new = FigureCanvas(fig_rsi)
            self.rsi_chart_layout.addWidget(canvas_rsi_new)
        else:
            if self.checkbox_rsi.isChecked() and not self.full_data_df.empty:
                 empty_label_rsi = BodyLabel("Недостатньо даних для RSI або RSI не обраховано", parent=self)
            else:
                empty_label_rsi = BodyLabel("RSI не обрано", parent=self)
            empty_label_rsi.setAlignment(Qt.AlignmentFlag.AlignCenter)
            self.rsi_chart_layout.addWidget(empty_label_rsi)