import sqlite3
import pandas as pd
import numpy as np
import time

from scipy import signal
from config import Config

class EMA_Calculator:
    def __init__(self, conn):
        self.conn = sqlite3.connect(Config.BHAV_DB_FILE_PATH)
        self.conn.execute("PRAGMA journal_mode = OFF;")
        self.conn.execute("PRAGMA synchronous = OFF;")
        self.conn.execute("PRAGMA temp_store = MEMORY;")

    def load_data(self):
        print(f"Querying entire DB")
        self.df = pd.read_sql(
            """
            SELECT *
            FROM bhavcopy b
            WHERE trade_date >= (
                SELECT trade_date
                FROM bhavcopy
                WHERE symbol = b.symbol
                ORDER BY trade_date DESC
                LIMIT 1 OFFSET 39
            )
            """,
            self.conn
        )
        self.df['symbol'] = self.df['symbol'].astype('category')
        self.df['trade_date'] = pd.to_datetime(self.df['trade_date'], format='%Y-%m-%d')
        self.df['close_price'] = self.df['close_price'].astype('float64')
        self.df = self.df.sort_values(['symbol', 'trade_date'])
        print(f"Loaded {len(self.df):,} rows")
    
    def updateEma(self, ema_span):
        g = self.df.groupby('symbol', observed=True)['close_price']
        for ema in ema_span:
            self.df[f'ema_{ema}'] = g.transform(
                lambda s: s.ewm(span=ema, adjust=False).mean()
            )
        print(f"EMA{ema_span} calculation done")

    def computeSignal(self):
        slope10 = (
            self.df.groupby('symbol', observed=True)['ema_10']
            .diff()
            .pipe(np.sign)
        )

        slope21 = (
            self.df.groupby('symbol', observed=True)['ema_21']
            .diff()
            .pipe(np.sign)
        )

        self.df['signal'] = 0
        self.df.loc[(slope10 == 1) & (slope21 == 1), 'signal'] = 1
        self.df.loc[(slope10 == -1) & (slope21 == -1), 'signal'] = -1
        self.df['signal'] = self.df['signal'].astype('int8')
    
    def symbols_with_latest_switch_to(self, target):
        """
        target: 1 or -1
        returns: list of symbols
        """
        if target not in (1, -1):
            raise ValueError("target must be 1 or -1")

        # per-symbol signal change
        d = self.df.groupby('symbol', observed=True)['signal'].diff()

        # switch INTO target on that row
        is_switch_to_target = (d != 0) & (self.df['signal'] == target)

        # last row per symbol
        last_rows = self.df.groupby('symbol', observed=True).tail(1)

        # symbols where last row is a switch into target
        symbols = last_rows.loc[
            is_switch_to_target.loc[last_rows.index],
            'symbol'
        ]

        return symbols.tolist()

def main():
    t0 = time.time()
    emaFilter = EMA_Calculator(None)
    emaFilter.load_data()
    t1 = time.time()
    emaFilter.updateEma([10, 21])
    emaFilter.computeSignal()
    listOnt = emaFilter.symbols_with_latest_switch_to(1)
    listOntn = emaFilter.symbols_with_latest_switch_to(-1)
    print(f"EMA calculation done in {time.time() - t1:.2f}s")
    print(f"Total runtime: {time.time() - t0:.2f}s")
    print(f"Symbols switched ON to 1: {listOnt}")
    print(f"Symbols switched ON to -1: {listOntn}")

    emaFilter.conn.close()

    # df now contains:
    # symbol | trade_date | close_price | ema_10 | ema_21
    print(emaFilter.df.tail())

if __name__ == "__main__":
    main()
