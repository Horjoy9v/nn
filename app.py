import sys
from PyQt6.QtWidgets import QApplication
from PyQt6.QtCore import Qt
from qfluentwidgets import setTheme, Theme
from main_window import MainWindow

if __name__ == '__main__':
    QApplication.setAttribute(Qt.ApplicationAttribute.AA_DontCreateNativeWidgetSiblings) 
    app = QApplication(sys.argv)
    setTheme(Theme.DARK)
    w = MainWindow()
    w.show()
    sys.exit(app.exec())