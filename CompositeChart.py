from abc import ABC, abstractmethod
import sys
#from matplotlib.pyplot import axis
import numpy as np
import pandas as pd
import pyqtgraph as pg
from PyQt6.QtWidgets import QApplication, QMainWindow, QToolBar
from PyQt6.QtGui import QAction, QKeySequence
from PyQt6.QtWidgets import QToolButton, QLabel
from PyQt6.QtCore import Qt
import sqlite3
from config import Config
import argparse
import pyperclip

class SymbolLoader(ABC):

    @abstractmethod
    def load_symbols(self, symbol):
        pass

class NSESymbolLoader(SymbolLoader):

    def __init__(self,  con, lookback=2000):
        self.con = con
        self.lookback = lookback

    def load_symbols(self, symbol):
        # example loading logic
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
        ORDER BY trade_date DESC
        LIMIT ?
        """
        df = pd.read_sql(query, self.con, params=(symbol, self.lookback))
        df["trade_date"] = pd.to_datetime(df["trade_date"])
        df = df.sort_values("trade_date")
        return df

class BSESymbolLoader(SymbolLoader):

    def __init__(self, filepath):
        self.filepath = filepath

    def load_symbols(self, symbol):
        with open(self.filepath) as f:
            return [line.strip() for line in f]
        
class DateAxis(pg.AxisItem):
    def __init__(self, dates, tf, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.dates = dates
        self.tf = tf  # "D", "W", "M"

    def tickStrings(self, values, scale, spacing):
        labels = []
        for v in values:
            i = int(round(v))
            if 0 <= i < len(self.dates):
                d = self.dates[i]

                if self.tf == "D":
                    labels.append(d.strftime("%d %b"))
                elif self.tf == "W":
                    labels.append(d.strftime("%b %Y"))
                else:  # "M"
                    labels.append(d.strftime("%b %Y"))
            else:
                labels.append("")
        return labels
    
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

        for x, o, h, l, c, _ in self.data:
            up = c >= o
            p.setPen(pg.mkPen('g' if up else 'r'))
            p.drawLine(pg.QtCore.QPointF(x, l), pg.QtCore.QPointF(x, h))
            p.setBrush(pg.mkBrush('g' if up else 'r'))
            p.drawRect(
                pg.QtCore.QRectF(
                    x - w / 2,
                    min(o, c),
                    w,
                    abs(c - o),
                )
            )

        p.end()

    def paint(self, painter, *_):
        painter.drawPicture(0, 0, self.picture)

    def boundingRect(self):
        return pg.QtCore.QRectF(self.picture.boundingRect())


# ====================== MAIN VIEWER ======================
class ChartViewer(QMainWindow):
    def __init__(self, symbols, loader):
        super().__init__()

        self.dfSymbols = symbols
        self.loader = loader
        
        self.idx = int(self.dfSymbols["reviewed"].sum())  # Start from first unreviewed symbol
        self.hline = None

        self.setWindowTitle("ChartViewer – Daily / Weekly / Monthly")
        self.resize(1400, 900)

        self._init_toolbar()
        self._init_chart()

        self.df = self.load_current_symbol()
        self.plot()
    @property
    def currentSymbol(self):
        return self.dfSymbols.iloc[self.idx]["symbol"]
    @property
    def currentOrigin(self):
        return self.dfSymbols.iloc[self.idx]["origin"]
    # ---------- Toolbar ----------
    def _init_toolbar(self):
        tb = QToolBar("Controls")
        self.addToolBar(tb)

        self.act_prev = QAction("◀ Prev", self)
        self.act_prev.setShortcut(QKeySequence(Qt.Key.Key_Left))
        self.act_next = QAction("Next ▶", self)
        self.act_next.setShortcut(QKeySequence(Qt.Key.Key_Right))
        self.act_select = QAction("Select", self)
        self.act_select.setCheckable(True)
        self.act_select.setShortcut(QKeySequence("S"))

        self.lbl_progress = QLabel("0 / 0")
        self.lbl_progress.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.lbl_progress.setMinimumWidth(60)  # keeps toolbar stable

        tb.addAction(self.act_prev)
        tb.addWidget(self.lbl_progress)   # 👈 between Prev & Next
        tb.addAction(self.act_next)
        tb.addAction(self.act_select)

        self.act_prev.triggered.connect(self.prev)
        self.act_next.triggered.connect(self.next)
        self.act_select.toggled.connect(self.toggle_select)

        tb.addSeparator()

        self.btn_ema10 = QToolButton()
        self.btn_ema10.setText("EMA 10")
        self.btn_ema10.setCheckable(True)
        self.btn_ema10.setChecked(True)

        self.btn_ema21 = QToolButton()
        self.btn_ema21.setText("EMA 21")
        self.btn_ema21.setCheckable(True)
        self.btn_ema21.setChecked(True)

        tb.addWidget(self.btn_ema10)
        tb.addWidget(self.btn_ema21)

        self.btn_ema10.toggled.connect(self.plot)
        self.btn_ema21.toggled.connect(self.plot)

    # ---------- Chart Layout ----------

    def create_crosshair(self, plot, bHline=False):
        v_line = pg.InfiniteLine(angle=90, movable=False, pen=pg.mkPen((180, 180, 180), width=1))
        plot.addItem(v_line, ignoreBounds=True)

        h_line = None
        if bHline:
            h_line = pg.InfiniteLine(angle=0, movable=False, pen=pg.mkPen((180, 180, 180), width=1))
            plot.addItem(h_line, ignoreBounds=True)
        return v_line, h_line

    def _init_chart(self):
        self.win = pg.GraphicsLayoutWidget()
        self.setCentralWidget(self.win)

        gl = self.win.ci

        # =====================================================
        # DAILY (top, spans both columns) → 6 rows total
        # =====================================================
        d_price = gl.addPlot(row=0, col=0, colspan=2)
        d_vol   = gl.addPlot(row=1, col=0, colspan=2,
                             axisItems={"bottom": DateAxis([], "D", orientation="bottom")})
        d_vol.setXLink(d_price)

        # =====================================================
        # WEEKLY (bottom-left) → 4 rows
        # =====================================================
        w_price = gl.addPlot(row=2, col=0)
        w_vol   = gl.addPlot(row=3, col=0,
                             axisItems={"bottom": DateAxis([], "W", orientation="bottom")})
        w_vol.setXLink(w_price)

        # =====================================================
        # MONTHLY (bottom-right) → 4 rows
        # =====================================================
        m_price = gl.addPlot(row=2, col=1)
        m_vol   = gl.addPlot(row=3, col=1,
                             axisItems={"bottom": DateAxis([], "M", orientation="bottom")})
        m_vol.setXLink(m_price)

        # =====================================================
        # STRETCH CONFIGURATION
        # =====================================================
        layout = gl.layout

        # 1. Vertical Stretch (Defining height ratios)
        layout.setRowStretchFactor(0, 5) # Daily Price tall
        layout.setRowStretchFactor(1, 1) # Daily Vol small
        layout.setRowStretchFactor(2, 3) # Weekly/Monthly Price medium
        layout.setRowStretchFactor(3, 1) # Weekly/Monthly Vol small

        # 2. Horizontal Stretch (Optional)
        # Ensures Weekly and Monthly columns are equal width
        layout.setColumnStretchFactor(0, 1)
        layout.setColumnStretchFactor(1, 1)

        # Hide X-Axis for Daily Price
        d_price.getAxis('bottom').setStyle(showValues=False)
        d_price.getAxis('bottom').setHeight(0)

        # Hide X-Axis for Weekly Price
        w_price.getAxis('bottom').setStyle(showValues=False)
        w_price.getAxis('bottom').setHeight(0)

        # Hide X-Axis for Monthly Price
        m_price.getAxis('bottom').setStyle(showValues=False)
        m_price.getAxis('bottom').setHeight(0)

        # =====================================================
        # AXIS SETTINGS
        # =====================================================
        for ax in [d_price, w_price, m_price]:
            ax.showGrid(x=True, y=True, alpha=0.3)
            ax.setMenuEnabled(False)

        for ax in [d_vol, w_vol, m_vol]:
            ax.showGrid(x=True, y=True, alpha=0.3)
            ax.setMenuEnabled(False)

        v_d_p, h_d_p = self.create_crosshair(d_price, bHline=True)
        v_d_v, h_d_v = self.create_crosshair(d_vol)

        v_w_p, h_w_p = self.create_crosshair(w_price, bHline=True)
        v_w_v, h_w_v = self.create_crosshair(w_vol)

        v_m_p, h_m_p = self.create_crosshair(m_price, bHline=True)
        v_m_v, h_m_v = self.create_crosshair(m_vol)

        self.charts = {
            "D": {
                "price": d_price , 
                "vol": d_vol,
                "label": pg.TextItem(anchor=(0,0), color='w'),
                "df":None,
                "v_lines": [v_d_p, v_d_v],
                "h_line": h_d_p,
            },
            "W": {
                "price": w_price,
                "vol": w_vol,
                "label": pg.TextItem(anchor=(0,0), color='w'),
                "df":None,
                "v_lines": [v_w_p, v_w_v],
                "h_line": h_w_p,
            },
            "M": {
                "price": m_price,
                "vol": m_vol, "label": pg.TextItem(anchor=(0,0), color='w'),
                "df":None,
                "v_lines": [v_m_p, v_m_v],
                "h_line": h_m_p,
            },
        }

        for _, cfg in self.charts.items():
            label = cfg["label"]
            plot = cfg["price"]
            label.setParentItem(plot.vb)
            label.setPos(0, 0) # Top-left corner

        

        self.proxy = proxy = pg.SignalProxy(gl.scene().sigMouseMoved, rateLimit=60, slot=self.mouseMoved)

    def get_ohlc_tooltip(self, x, df):
        if x < 0 or x >= len(df):
            return ""

        r = df.iloc[x]
        str = f"{r.trade_date.strftime('%Y-%b-%d')}: O:{r.open:.2f}  H:{r.high:.2f}  L:{r.low:.2f}  C:{r.close:.2f}  V:{int(r.volume):,}"
        return str
    
    def mouseMoved(self, evt):
        pos = evt[0]
        for _, cfg in self.charts.items():
            plot = cfg["price"]
            label = cfg["label"]
            v_lines = cfg["v_lines"]
            h_line = cfg["h_line"]  
            if plot.sceneBoundingRect().contains(pos):
                mousePoint = plot.vb.mapSceneToView(pos)
                label.setText(self.get_ohlc_tooltip(int(mousePoint.x()), cfg["df"]))
                for vl in v_lines:
                    vl.setPos(mousePoint.x())
                h_line.setPos(mousePoint.y())
                break
            
    # ---------- DB ----------
    def load_current_symbol(self):
        symbol = self.currentSymbol
        self.dfSymbols.at[self.idx, "reviewed"] = True
        self.lbl_progress.setText(f"{self.idx + 1} / {len(self.dfSymbols)}")
        
        df = self.loader.load_symbols(symbol)
        df["ema_10"] = df["close"].ewm(span=10, adjust=False).mean()
        df["ema_21"] = df["close"].ewm(span=21, adjust=False).mean()
        df["x"] = np.arange(len(df))

        return df.reset_index(drop=True)

    # ---------- Resampling ----------
    def resample_df(self, df, tf):
        if tf == "D":
            rdf = df.tail(200).copy().reset_index()
            rdf["x"] = np.arange(len(rdf))
            return rdf

        rule = {"W": "W-FRI", "M": "ME"}[tf]

        rdf = (
            df.set_index("trade_date")
              .resample(rule)
              .agg({
                  "open": "first",
                  "high": "max",
                  "low": "min",
                  "close": "last",
                  "volume": "sum",
              })
              .dropna()
              .reset_index()
        )

        rdf["ema_10"] = rdf["close"].ewm(span=10, adjust=False).mean()
        rdf["ema_21"] = rdf["close"].ewm(span=21, adjust=False).mean()
        rdf["x"] = np.arange(len(rdf))

        return rdf

    # ---------- Plot ----------
    def plot(self):
        sym = self.currentSymbol
        org = self.currentOrigin
        self.setWindowTitle(f"ChartViewer – {sym} ({org})")
        pyperclip.copy(sym)  # Copy symbol to clipboard for easy access

        for tf, cfg in self.charts.items():
            ax_price = cfg["price"]
            ax_vol   = cfg["vol"]

            ax_price.clear()
            ax_vol.clear()

            df = self.resample_df(self.df, tf)
            cfg["df"] = df
            axis = ax_vol.getAxis("bottom")
            axis.dates = df["trade_date"].tolist()

            ax_price.addItem(
                CandlestickItem(
                    df[["x", "open", "high", "low", "close", "trade_date"]].values
                )
            )

            if self.btn_ema10.isChecked():
                ax_price.plot(df["x"], df["ema_10"], pen=pg.mkPen('g', width=1))

            if self.btn_ema21.isChecked():
                ax_price.plot(df["x"], df["ema_21"], pen=pg.mkPen('r', width=1))

            ax_vol.addItem(
                pg.BarGraphItem(
                    x=df["x"],
                    height=df["volume"],
                    width=0.6,
                    brush=(120, 120, 255, 150),
                )
            )
            ax_price.setTitle(f"{sym} [{tf}]")
            # restore crosshair
            ax_price.addItem(cfg["v_lines"][0], ignoreBounds=True)
            ax_vol.addItem(cfg["v_lines"][1], ignoreBounds=True)

            if cfg["h_line"] is not None:
                ax_price.addItem(cfg["h_line"], ignoreBounds=True)

        self.act_prev.setEnabled(self.idx > 0)
        self.act_next.setEnabled(self.idx < len(self.dfSymbols) - 1)

        self.act_select.blockSignals(True)
        self.act_select.setChecked(bool(self.dfSymbols.iloc[self.idx]["selected"]))
        self.act_select.blockSignals(False)

    # ---------- Controls ----------
    def resetPlot(self):
        for tf, cfg in self.charts.items():
            ax_price = cfg["price"]
            ax_vol   = cfg["vol"]
            ax_price.vb.autoRange()
            ax_vol.vb.autoRange()
            v_lines = cfg["v_lines"]
            h_line = cfg["h_line"]  
            for vl in v_lines:
                vl.setPos(0)
            h_line.setPos(0)

    def prev(self):
        if self.idx > 0:
            self.idx -= 1
            self.df = self.load_current_symbol()
            self.plot()
            self.resetPlot()

    def next(self):
        if self.idx < len(self.dfSymbols) - 1:
            self.idx += 1
            self.df = self.load_current_symbol()
            self.plot()
            self.resetPlot()

    def toggle_select(self, checked):
        self.dfSymbols.at[self.idx, "selected"] = checked


# ---------- Entry ----------
def getUserSelection(df):
    app = QApplication(sys.argv)
    con = sqlite3.connect(Config.BHAV_DB_FILE_PATH)
    for col in ["reviewed", "selected"]:
        df[col] = False if col not in df.columns else df[col].astype(bool)

    if int(df["reviewed"].sum()) == len(df):
        print("All symbols have been reviewed...")
        return []
    nse_loader = NSESymbolLoader( con)
    viewer = ChartViewer(df, nse_loader)
    viewer.show()
    app.exec()
    viewer.dfSymbols.to_csv(Config.BHAV_WATCH_FILE, index=False)
    return list(viewer.dfSymbols.loc[viewer.dfSymbols["selected"], "symbol"])

def parse_args():
    parser = argparse.ArgumentParser(
        description="Visualize Dailly, Weekly Monthly charts for selected symbols"
    )

    parser.add_argument("--symbol-file", type=str, help=f"Name of the symbol file located in {Config.TMP_DIR} directory", default=None)
    parser.add_argument("--symbol-folder", type=str, help=f"Name of the symbol folder located in {Config.TMP_DIR} directory load all (*.csv)", default=None)

    return parser.parse_args()

def main(): 
    args = parse_args()
    df = pd.DataFrame({"symbol": ["SBIN", "INFY", "ITC"]})
    if args.symbol_file:
        filepath = Config.TMP_DIR / args.symbol_file
        if filepath.exists():
            cols = pd.read_csv(filepath, nrows=0).columns
            symbol_col = None
            for c in cols:
                if c.lower() == "symbol":
                    symbol_col = c
                    break
            df = pd.read_csv(filepath, usecols=[symbol_col])
            df.columns = df.columns.str.lower()
            df = df.drop_duplicates(subset=["symbol"]).copy()  # Ensure unique symbols
            df['origin'] = filepath.stem  # Track source file
    elif args.symbol_folder:
        folderpath = Config.TMP_DIR / args.symbol_folder
        print(f"Looking for CSV files in {folderpath}...")
        if folderpath.exists():
            csv_files = list(folderpath.glob("*.csv"))
            if csv_files:
                df = pd.concat(
                    (
                        pd.read_csv(f, usecols=lambda c: c.strip().lower() == "symbol")
                        .rename(columns=str.lower)
                        .assign(origin=f.stem)
                        for f in csv_files
                    ),
                    ignore_index=True
                )
                df = (
                    df.groupby("symbol", as_index=False)["origin"]
                    .agg(lambda x: "#".join(sorted(set(x))))
                )
    elif Config.BHAV_WATCH_FILE.exists():
        df = pd.read_csv(Config.BHAV_WATCH_FILE)
        df = df.sort_values(
            by="reviewed",
            ascending=False,
            kind="mergesort"   # 👈 keeps original order within True/False
        ).reset_index(drop=True)
    print("Selected:", getUserSelection(df))
    # py CompositeChart.py --symbol-file=weeklyfilters.csv
if __name__ == "__main__":
    main()