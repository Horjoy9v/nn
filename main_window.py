import pandas as pd
import logging

from qfluentwidgets import FluentWindow, FluentIcon as FIF, NavigationItemPosition
from bybit_kline_app import BybitKlineApp
from save_data_interface import SaveDataInterface

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [MainWindow] %(message)s')

class MainWindow(FluentWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle('Bybit Kline Downloader & Analyzer')
        self.setGeometry(100, 100, 1600, 900)

        self.bybit_app_interface = BybitKlineApp(parent=self)
        self.save_data_interface = SaveDataInterface(parent=self) 
        
        self.initNavigation()

        self.bybit_app_interface.data_loaded_signal.connect(self.on_data_loaded_in_bybit_app)
        logging.info("MainWindow: Підключено data_loaded_signal.")

        logging.info("MainWindow: Встановлення початкового стану SaveDataInterface (порожній DF).")
        self.save_data_interface.update_data_and_switches(pd.DataFrame())


    def initNavigation(self):
        self.addSubInterface(self.bybit_app_interface, FIF.HOME, 'Завантаження та аналіз', NavigationItemPosition.TOP)
        self.addSubInterface(self.save_data_interface, FIF.SAVE, 'Збереження даних', NavigationItemPosition.TOP)
        logging.info("MainWindow: Навігаційні інтерфейси додано.")

    def on_data_loaded_in_bybit_app(self, df: pd.DataFrame):
        """
        Слот, який викликається, коли дані завантажені в BybitKlineApp.
        Передає актуальний DataFrame до SaveDataInterface.
        """
        logging.info(f"MainWindow: Отримано сигнал data_loaded_signal. DataFrame порожній: {df.empty}.")
        self.save_data_interface.update_data_and_switches(df)