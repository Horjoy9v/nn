import pandas as pd
import logging
from typing import Dict, List, Optional
from dataclasses import dataclass
from enum import Enum

from PyQt6.QtWidgets import (
    QVBoxLayout, QWidget, QFileDialog, QHBoxLayout, QSizePolicy
)
from PyQt6.QtCore import Qt, pyqtSignal 

from qfluentwidgets import (
    BodyLabel, CardWidget, SwitchButton, PrimaryPushButton, MessageBox, 
    LineEdit, StrongBodyLabel, CaptionLabel
)

# Налаштування логування
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [SaveDataInterface] %(message)s')


class FieldType(Enum):
    """Типи полів даних"""
    BASIC = "Основні"
    VOLUME = "Об'єм"
    INDICATOR = "Індикатори"


@dataclass
class FieldConfig:
    """Конфігурація поля для збереження"""
    key: str
    label: str
    columns: List[str]
    field_type: FieldType
    default_enabled: bool = True
    always_available: bool = False


class SaveDataInterface(QWidget):
    """Оптимізований інтерфейс збереження даних"""
    
    filter_data_signal = pyqtSignal(bool)

    # Конфігурація полів
    FIELD_CONFIGS = [
        FieldConfig("ohlc", "OHLC дані (Open, High, Low, Close)", 
                   ["open", "high", "low", "close"], FieldType.BASIC, True, True),
        FieldConfig("volume", "Об'єм торгів", ["volume"], FieldType.VOLUME),
        FieldConfig("turnover", "Оборот", ["turnover"], FieldType.VOLUME),
        FieldConfig("sma_20", "SMA (20 періодів)", ["SMA_20"], FieldType.INDICATOR),
        FieldConfig("ema_20", "EMA (20 періодів)", ["EMA_20"], FieldType.INDICATOR),
        FieldConfig("bb_bands", "Смуги Боллінджера", 
                   ["BBM_20_2.0", "BBU_20_2.0", "BBL_20_2.0"], FieldType.INDICATOR),
        FieldConfig("rsi_14", "RSI (14 періодів)", ["RSI_14"], FieldType.INDICATOR),
    ]

    def __init__(self, parent: Optional[QWidget] = None):
        super().__init__(parent)
        self._current_data_df = pd.DataFrame()
        self.column_switches: Dict[str, Dict[str, QWidget]] = {}
        
        self.setObjectName("Save-Data-Interface")
        self._init_ui()
        self.set_interface_enabled(False)
        logging.info("SaveDataInterface: Ініціалізація інтерфейсу збереження даних.")

    def _init_ui(self):
        """Ініціалізація користувацького інтерфейсу"""
        layout = QVBoxLayout(self)
        layout.setContentsMargins(24, 24, 24, 24)
        layout.setSpacing(20)
        layout.setAlignment(Qt.AlignmentFlag.AlignTop)

        # Заголовок
        title = StrongBodyLabel("Збереження торгових даних")
        title.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(title)

        # Секція імені файлу
        layout.addWidget(self._create_filename_section())
        
        # Горизонтальний макет для налаштувань
        settings_layout = QHBoxLayout()
        settings_layout.setSpacing(20)
        
        # Секція полів
        settings_layout.addWidget(self._create_fields_section())
        
        # Секція опцій
        settings_layout.addWidget(self._create_options_section())
        
        layout.addLayout(settings_layout)
        
        # Кнопка збереження
        save_layout = QHBoxLayout()
        save_layout.addStretch()
        
        self.save_button = PrimaryPushButton("Експортувати дані")
        self.save_button.setFixedSize(180, 36)
        self.save_button.clicked.connect(self.save_data_to_csv)
        
        save_layout.addWidget(self.save_button)
        save_layout.addStretch()
        
        layout.addLayout(save_layout)

    def _create_filename_section(self) -> CardWidget:
        """Створення секції налаштування імені файлу"""
        card = CardWidget()
        layout = QVBoxLayout(card)
        layout.setContentsMargins(20, 16, 20, 20)
        layout.setSpacing(12)

        layout.addWidget(BodyLabel("Налаштування файлу"))
        
        self.filename_input = LineEdit()
        self.filename_input.setText("kline_data")
        self.filename_input.setPlaceholderText("Введіть ім'я файлу без розширення")
        layout.addWidget(self.filename_input)
        
        description = CaptionLabel("Файл буде збережено у форматі CSV з вказаним ім'ям")
        description.setStyleSheet("color: rgba(255, 255, 255, 0.6);")
        layout.addWidget(description)

        return card

    def _create_fields_section(self) -> CardWidget:
        """Створення секції вибору полів"""
        card = CardWidget()
        card.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.MinimumExpanding)
        layout = QVBoxLayout(card)
        layout.setContentsMargins(20, 16, 20, 20)
        layout.setSpacing(16)

        layout.addWidget(BodyLabel("Поля для експорту"))

        # Групування полів за типом
        field_groups = self._group_fields_by_type()
        
        for field_type, fields in field_groups.items():
            if fields:
                # Додаємо заголовок групи якщо потрібно
                if len(field_groups) > 1 and len(fields) > 1:
                    group_label = CaptionLabel(field_type.value)
                    group_label.setStyleSheet("color: rgba(255, 255, 255, 0.8); font-weight: 500; margin-top: 8px;")
                    layout.addWidget(group_label)
                
                # Додаємо поля групи
                for field in fields:
                    field_widget = self._create_field_switch(field)
                    layout.addWidget(field_widget)

        layout.addStretch()
        return card

    def _create_field_switch(self, field: FieldConfig) -> QWidget:
        """Створення перемикача для поля"""
        widget = QWidget()
        widget.setFixedHeight(40)  # Фіксована висота для кожного рядка
        layout = QHBoxLayout(widget)
        layout.setContentsMargins(4, 8, 16, 8)
        layout.setSpacing(16)  # Збільшений відступ між перемикачем і текстом

        switch = SwitchButton()
        # ВИПРАВЛЕННЯ: Видалено порожній setText() для правильного відображення on/off станів
        switch.setChecked(False)
        #switch.setFixedSize(44, 24)
        
        label = BodyLabel(field.label)
        label.setAlignment(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter)
        label.setWordWrap(False)  # Заборонити перенос тексту
        
        layout.addWidget(switch)
        layout.addWidget(label, 1, Qt.AlignmentFlag.AlignLeft)
        

        self.column_switches[field.key] = {'switch': switch, 'label': label}
        
        return widget

    def _create_options_section(self) -> CardWidget:
        """Створення секції опцій"""
        card = CardWidget()
        card.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.MinimumExpanding)
        layout = QVBoxLayout(card)
        layout.setContentsMargins(20, 16, 20, 20)
        layout.setSpacing(32)

        layout.addWidget(BodyLabel("Опції обробки"))

        filter_widget = QWidget()
        filter_widget.setFixedHeight(50)
        filter_layout = QVBoxLayout(filter_widget)
        filter_layout.setContentsMargins(4, 8, 16, 8)
        filter_layout.setSpacing(8)

        switch_layout = QHBoxLayout()
        switch_layout.setSpacing(32) 
        
        self.filter_incomplete_data_switch = SwitchButton()

        self.filter_incomplete_data_switch.setChecked(False)
        #self.filter_incomplete_data_switch.setFixedSize(74, 24)
        self.filter_incomplete_data_switch.checkedChanged.connect(self.on_filter_switch_changed)

        self.filter_incomplete_data_switch.setStyleSheet("""
            SwitchButton {
                font-size: 10px;
            }
        """)
        
        filter_label = BodyLabel("Очистити неповні дані з порожніми значеннями індикаторів")
        filter_label.setAlignment(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter)
        filter_label.setWordWrap(False)
        
        switch_layout.addWidget(self.filter_incomplete_data_switch)
        switch_layout.addWidget(filter_label, 1, Qt.AlignmentFlag.AlignLeft)
        filter_layout.addLayout(switch_layout)

        layout.addWidget(filter_widget)
        layout.addStretch()
        
        return card

    def _group_fields_by_type(self) -> Dict[FieldType, List[FieldConfig]]:
        """Групування полів за типом"""
        groups = {field_type: [] for field_type in FieldType}
        
        for field in self.FIELD_CONFIGS:
            groups[field.field_type].append(field)
            
        return groups

    def on_filter_switch_changed(self, checked: bool):
        """Обробка зміни перемикача фільтрації"""
        logging.info(f"SaveDataInterface: Перемикач 'Видалити неповні дані індикаторів' змінено на {checked}.")
        self.filter_data_signal.emit(checked)

    def set_interface_enabled(self, enabled: bool):
        """Включення/вимкнення інтерфейсу"""
        logging.info(f"SaveDataInterface: Інтерфейс збереження даних {'активовано' if enabled else 'деактивовано'}.")
        
        self.filename_input.setEnabled(enabled)
        self.save_button.setEnabled(enabled)
        self.filter_incomplete_data_switch.setEnabled(enabled)
        
        for switch_data in self.column_switches.values():
            switch_data['switch'].setEnabled(enabled)
            switch_data['label'].setEnabled(enabled)

    def update_data_and_switches(self, df: pd.DataFrame):
        """Оновлення даних та стану перемикачів"""
        self._current_data_df = df 
        logging.info("SaveDataInterface: Оновлення стану перемикачів колонок.")
        
        self.set_interface_enabled(False)

        if self._current_data_df.empty:
            logging.warning("SaveDataInterface: DataFrame порожній, всі перемикачі вимкнено.")
            return

        self.set_interface_enabled(True)
        available_columns = self._current_data_df.columns.tolist()

        # Оновлення стану перемикачів на основі конфігурації
        for field in self.FIELD_CONFIGS:
            switch_data = self.column_switches[field.key]
            switch = switch_data['switch']
            
            if field.always_available:
                switch.setEnabled(True)
                switch.setChecked(True)
            else:
                is_available = self._check_field_availability(field, available_columns)
                switch.setEnabled(is_available)
                switch.setChecked(is_available)
        
        logging.info("SaveDataInterface: Стан перемикачів оновлено відповідно до наявних колонок.")

    def _check_field_availability(self, field: FieldConfig, available_columns: List[str]) -> bool:
        """Перевірка доступності поля"""
        if field.key == "bb_bands":
            # Для смуг Боллінджера потрібні всі колонки
            return all(col in available_columns for col in field.columns)
        
        # Для інших полів достатньо одної колонки
        return any(col in available_columns for col in field.columns)

    def save_data_to_csv(self):
        """Збереження даних у CSV файл"""
        logging.info("SaveDataInterface: Запущено збереження даних.")
        
        if self._current_data_df.empty:
            w = MessageBox("Помилка збереження", "Немає даних для збереження. Завантажте дані спочатку.", self.window())
            w.exec()
            logging.warning("SaveDataInterface: Спроба зберегти порожні дані.")
            return

        suggested_filename = self.filename_input.text().strip() or "kline_data"
        
        file_path, _ = QFileDialog.getSaveFileName(
            self, "Зберегти дані як", f"{suggested_filename}.csv", "CSV Files (*.csv)"
        )

        if not file_path:
            return

        try:
            columns_to_export = self._get_export_columns()
            
            if not columns_to_export:
                w = MessageBox("Помилка збереження", "Не вибрано жодних колонок для збереження.", self.window())
                w.exec()
                return

            # Збереження файлу
            df_to_save = self._current_data_df[columns_to_export].copy()
            include_index = bool(self._current_data_df.index.name)
            df_to_save.to_csv(file_path, index=include_index)
            
            w = MessageBox("Збереження успішне", f"Дані успішно збережено у {file_path}", self.window())
            w.exec()
            logging.info(f"SaveDataInterface: Дані успішно збережено у {file_path}")
            
        except Exception as e:
            w = MessageBox("Помилка збереження", f"Не вдалося зберегти дані: {e}", self.window())
            w.exec()
            logging.error(f"SaveDataInterface: Помилка при збереженні даних: {e}", exc_info=True)

    def _get_export_columns(self) -> List[str]:
        """Отримання колонок для експорту"""
        export_columns = []
        available_columns = self._current_data_df.columns.tolist()
        
        for field in self.FIELD_CONFIGS:
            switch = self.column_switches[field.key]['switch']
            if switch.isChecked() and switch.isEnabled():
                field_columns = [col for col in field.columns if col in available_columns]
                export_columns.extend(field_columns)
        
        return list(dict.fromkeys(export_columns))  # Видалення дублікатів