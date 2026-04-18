import sys
import numpy as np
import pandas as pd
import pyqtgraph as pg
from PyQt6.QtWidgets import QApplication, QMainWindow, QToolBar
from PyQt6.QtGui import QAction
from PyQt6.QtWidgets import QToolButton
from PyQt6.QtCore import Qt
from config import Config
import sqlite3


# ---------- Candlestick GraphicsItem ----------
class CandlestickItem(pg.GraphicsObject):
    def __init__(self, data):
        super().__init__()
        self.data = data
        self.generatePicture()

    def generatePicture(self):
        self.picture = pg.QtGui.QPicture()
        p = pg.QtGui.QPainter(self.picture)
        w = 0.6

        for x, o, h, l, c in self.data:
            up = c >= o
            p.setPen(pg.mkPen('g' if up else 'r'))
            p.drawLine(pg.QtCore.QPointF(x, l), pg.QtCore.QPointF(x, h))
            p.setBrush(pg.mkBrush('g' if up else 'r'))
            top = max(o, c)
            bottom = min(o, c)
            p.drawRect(pg.QtCore.QRectF(x - w / 2, bottom, w, top - bottom))

        p.end()

    def paint(self, painter, *_):
        painter.drawPicture(0, 0, self.picture)

    def boundingRect(self):
        return pg.QtCore.QRectF(self.picture.boundingRect())


# ====================== MAIN VIEWER ======================
class ChartViewer(QMainWindow):
    def __init__(self, symbols, con, lookback=200):
        super().__init__()
        self.symbols = symbols
        self.con = con
        self.lookback = lookback

        self.idx = 0
        self.selected = set()
        self.hline = None

        self.setWindowTitle("ChartViewer (PyQtGraph)")
        self.resize(1200, 700)

        self._init_toolbar()
        self._init_chart()
        sym = self.symbols[self.idx]
        self.df = self.load_symbol(sym)
        self.plot()

    # ---------- Toolbar ----------
    def _init_toolbar(self):
        tb = QToolBar("Controls")
        
        self.addToolBar(tb)

        self.act_prev = QAction("◀ Prev", self)
        self.act_next = QAction("Next ▶", self)
        self.act_select = QAction("Select", self)
        self.act_select.setCheckable(True)
        self.act_hline = QAction("Horizontal Line", self)

        
        tb.addActions([
            self.act_prev,
            self.act_next,
            self.act_select,
            self.act_hline
        ])

        self.act_prev.triggered.connect(self.prev)
        self.act_next.triggered.connect(self.next)
        self.act_select.toggled.connect(self.toggle_select)
        self.act_hline.triggered.connect(self.add_hline)

        self.btn_ema10 = QToolButton()
        self.btn_ema10.setText("EMA 10")
        self.btn_ema10.setCheckable(True)
        self.btn_ema10.setChecked(True)

        self.btn_ema21 = QToolButton()
        self.btn_ema21.setText("EMA 21")
        self.btn_ema21.setCheckable(True)
        self.btn_ema21.setChecked(True)

        tb.addSeparator()
        tb.addWidget(self.btn_ema10)
        tb.addWidget(self.btn_ema21)

        self.btn_ema10.toggled.connect(self.plot)
        self.btn_ema21.toggled.connect(self.plot)


    def ema_select(self, checked):
        self.plot()
    # ---------- Chart Layout ----------
    def _init_chart(self):
        self.win = pg.GraphicsLayoutWidget()
        self.setCentralWidget(self.win)

        self.ax_price = self.win.addPlot(row=0, col=0)
        self.ax_vol = self.win.addPlot(row=1, col=0)

        #self.ax_vol.setMaximumHeight(180)
        self.ax_vol.setXLink(self.ax_price)

        self.ax_price.showGrid(x=True, y=True, alpha=0.3)
        self.ax_vol.showGrid(x=True, y=True, alpha=0.3)

        self.ax_price.setMenuEnabled(False)
        self.ax_vol.setMenuEnabled(False)

        self.ax_price.setMouseEnabled(x=True, y=True)
        self.ax_vol.setMouseEnabled(x=True, y=False)

        self.vline = pg.InfiniteLine(angle=90, movable=False, pen=pg.mkPen((180, 180, 180), width=1))
        self.hline_cursor = pg.InfiniteLine(angle=0, movable=False, pen=pg.mkPen((180, 180, 180), width=1))

        self.ax_price.addItem(self.vline, ignoreBounds=True)
        self.ax_price.addItem(self.hline_cursor, ignoreBounds=True)

        self.proxy = pg.SignalProxy(
            self.ax_price.scene().sigMouseMoved,
            rateLimit=60,
            slot=self._on_mouse_moved
        )

 
    def _on_mouse_moved(self, evt):
        pos = evt[0]
        if not self.ax_price.sceneBoundingRect().contains(pos):
            return

        mouse_point = self.ax_price.vb.mapSceneToView(pos)
        x = int(round(mouse_point.x()))
        y = mouse_point.y()

        self.vline.setPos(x)
        self.hline_cursor.setPos(y)

    # ---------- DB ----------
    def load_symbol(self, symbol):
        query = """
        SELECT
            trade_date,
            open_price  AS open,
            high_price  AS high,
            low_price   AS low,
            close_price AS close,
            ttl_trd_qnty AS volume
        FROM bhavcopy
        WHERE symbol = ?
          AND series = 'EQ'
        ORDER BY trade_date DESC
        LIMIT ?
        """
        df = pd.read_sql(query, self.con, params=(symbol, self.lookback))
        df["trade_date"] = pd.to_datetime(df["trade_date"])
        df = df.sort_values("trade_date")

        df["ema_10"] = df["close"].ewm(span=10, adjust=False).mean()
        df["ema_21"] = df["close"].ewm(span=21, adjust=False).mean()

        df.reset_index(drop=True, inplace=True)
        df["x"] = np.arange(len(df))

        return df

    # ---------- Plot ----------
    def plot(self):
        self.ax_price.clear()
        self.ax_vol.clear()
        df = self.df

        candles = CandlestickItem(
            df[["x", "open", "high", "low", "close"]].values
        )
        self.ax_price.addItem(candles)

        if self.btn_ema10.isChecked():
            self.ax_price.plot(df["x"], df["ema_10"], pen=pg.mkPen('y', width=1))

        if self.btn_ema21.isChecked():
            self.ax_price.plot(df["x"], df["ema_21"], pen=pg.mkPen('c', width=1))

        vol = pg.BarGraphItem(
            x=df["x"],
            height=df["volume"],
            width=0.6,
            brush=(120, 120, 255, 150)
        )
        self.ax_vol.addItem(vol)
        sym = self.symbols[self.idx]
        self.ax_price.setTitle(f"{sym} ({self.idx+1}/{len(self.symbols)})")

        # Toolbar sync
        self.act_prev.setEnabled(self.idx > 0)
        self.act_next.setEnabled(self.idx < len(self.symbols) - 1)

        self.act_select.blockSignals(True)
        self.act_select.setChecked(sym in self.selected)
        self.act_select.blockSignals(False)

        self.ax_price.addItem(self.vline, ignoreBounds=True)
        self.ax_price.addItem(self.hline_cursor, ignoreBounds=True)

    # ---------- Controls ----------
    def prev(self):
        if self.idx > 0:
            self.idx -= 1
            sym = self.symbols[self.idx]
            self.df = self.load_symbol(sym)
            self.plot()
            self.ax_price.vb.autoRange()
            self.ax_vol.vb.autoRange()
            
    def next(self):
        if self.idx < len(self.symbols) - 1:
            self.idx += 1
            sym = self.symbols[self.idx]
            self.df = self.load_symbol(sym)
            self.plot()
            self.ax_price.vb.autoRange()
            self.ax_vol.vb.autoRange()

    def toggle_select(self, checked):
        sym = self.symbols[self.idx]
        if checked:
            self.selected.add(sym)
        else:
            self.selected.discard(sym)

    def add_hline(self):
        if self.hline:
            self.ax_price.removeItem(self.hline)

        y = self.ax_price.viewRange()[1][0]
        self.hline = pg.InfiniteLine(
            pos=y,
            angle=0,
            movable=True,
            pen=pg.mkPen('y', width=1.5)
        )
        self.ax_price.addItem(self.hline)

def getUserSelection(data):
    app = QApplication(sys.argv)
    con = sqlite3.connect(Config.BHAV_DB_FILE_PATH)
    viewer = ChartViewer(data, con, lookback=200)
    viewer.show()
    app.exec()
    return list(viewer.selected)


# ---------- Entry ----------
def main():
    print("Selected Symbols:",  getUserSelection(["SBIN", "INFY", "ITC"]))

if __name__ == "__main__":
    main()