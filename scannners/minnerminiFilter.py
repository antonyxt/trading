import sys
import os

# Get the absolute path of the root directory
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..')) # Adjust '..' based on nesting
# Insert the root directory path to the beginning of sys.path
sys.path.insert(0, project_root)

import sqlite3
import pandas as pd
from config import Config
import ChartViewer

def load_data(db_path):

    conn = sqlite3.connect(db_path)

    query = """
    WITH active_symbols AS (
        SELECT
            symbol,
            MAX(trade_date) AS last_trade_date
        FROM bhavcopy
        WHERE series = 'EQ'
        AND symbol NOT LIKE '%etf%'
        AND symbol NOT LIKE '%bees'
        AND symbol NOT LIKE '%liquid%'
        AND symbol NOT LIKE '%gold%'
        AND symbol NOT LIKE '%silver%'
        AND symbol NOT LIKE '%gsec%'
        GROUP BY symbol
        HAVING last_trade_date >= strftime('%Y-%m-%d', 'now', '-30 day')
    ),
    ranked AS (
        SELECT
            b.trade_date,
            b.symbol,
            b.open_price  AS open,
            b.high_price  AS high,
            b.low_price   AS low,
            b.close_price AS close,
            b.ttl_trd_qnty AS volume,
            ROW_NUMBER() OVER (
                PARTITION BY b.symbol
                ORDER BY b.trade_date DESC
            ) AS rn
        FROM bhavcopy b
        JOIN active_symbols a
        ON b.symbol = a.symbol
        WHERE b.series = 'EQ'
    )
    SELECT
        trade_date,
        symbol,
        open,
        high,
        low,
        close,
        volume
    FROM ranked
    WHERE rn <= 120
    ORDER BY symbol, trade_date;
    """

    df = pd.read_sql(query, conn)
    conn.close()

    df['trade_date'] = pd.to_datetime(df['trade_date'], format='%Y-%m-%d')
    df['symbol'] = df['symbol'].astype('category')

    return df.sort_values(['symbol', 'trade_date']).copy()

def add_indicators(df, sma: int = 50):
    # 50 SMA (Minervini core)
    df[f'sma_{sma}'] = (
        df.groupby('symbol', observed=True)['close']
          .transform(lambda s: s.rolling(sma).mean())
    )
    return df

def tight_base(df, lookback=5, max_range_pct=0.04):
    hh = df[['open', 'close']].max(axis=1).rolling(lookback, min_periods=1).max()
    ll = df[['open', 'close']].min(axis=1).rolling(lookback, min_periods=1).min()
    # ToDo instead of % betwee Max, Min find the slope of the line connecting the mx an

    return ((hh - ll) / ll) <= max_range_pct

def pivot_high(df, lookback=10):
    return df['high'].rolling(lookback).max()

def identify_minervini_pivot(df, window=20):
    """
    Identifies the Pivot point based on Volatility Contraction Pattern (VCP) logic.
    Requires OHLCV data.
    """
    # 1. Calculate Daily Range and Volatility (ATR-like)
    df['range'] = (df['high'] - df['low']) / df['close']
    df['rolling_vol'] = df['range'].rolling(window=10).mean()
    
    # 2. Define the Trend Template (Minervini's pre-requisite)
    # Price must be above 150 & 200 Day MAs, and 50MA above 150MA
    df['ma50'] = df['close'].rolling(window=50).mean()
    df['ma150'] = df['close'].rolling(window=150).mean()
    df['ma200'] = df['close'].rolling(window=200).mean()
    
    # 3. Locating the Pivot
    # A pivot is often the high of a 'tight' period (low volatility) 
    # after a series of contractions.
    df['is_tight'] = df['rolling_vol'] < df['rolling_vol'].shift(window)
    df['volume_dry_up'] = df['volume'] < df['volume'].rolling(window=20).mean() * 0.6
    
    # The Pivot Level is the recent local high during this tight period
    df['pivot_level'] = df['high'].rolling(window=5).max()
    
    # 4. Filter for 'Cheat' or 'Pivot' setups
    # Condition: Tight price + Volume dry up + In a Stage 2 Uptrend
    df['vcp_setup'] = (
        (df['close'] > df['ma200']) & 
        (df['ma50'] > df['ma150']) & 
        (df['is_tight']) & 
        (df['volume_dry_up'])
    )
    
    return df['vcp_setup']

def compute_signals(df, lookback=10, sma: int = 50):

    df['symbol'] = df.name
    df['pivot'] = pivot_high(df, lookback)

    df['radar'] = (
        tight_base(df, lookback) 
        #& (df['close'] >= df['pivot'].shift(1) * 0.95) 
        #& (df['close'] <  df['pivot'].shift(1)) 
        #& (df['close'] > df[f'sma_{sma}']) 
        #& (df[f'sma_{sma}'] > df[f'sma_{sma}'].shift(5))
    )

    df['breakout'] = (
        (df['close'] > df['pivot'].shift(1)) &
        (df['close'] <= df['pivot'].shift(1) * 1.05)
    )
    df['vcp_radar'] = identify_minervini_pivot(df)
    return df

def run_minervini_scan(db_path):
    print(f"✅ Loading Data...")
    df = load_data(db_path)
    sma = 50
    print(f"✅ Adding Indicators...")
    df = add_indicators(df, sma=sma)
    

    df = (
        df.groupby('symbol', observed=True, group_keys=False)
          .apply(lambda x: compute_signals(x, lookback=10, sma=sma))
          .reset_index(drop=False)
    )
    print(df.columns)
    # Only last candle per symbol
    last = df.groupby('symbol', observed=True).tail(1)

    radar = last.loc[last['vcp_radar'],
                     ['symbol', 'trade_date', 'pivot', 'close']]

    breakouts = last.loc[last['breakout'],
                         ['symbol', 'trade_date', 'pivot', 'close']]

    return radar, breakouts



   
def reviewSymbols( symbols):
    finalized = ChartViewer.getUserSelection(symbols)
    return finalized

def main():
    radar, breakouts = run_minervini_scan(Config.BHAV_DB_FILE_PATH)

    print("Radar signals:")
    print(radar)
    print("\nBreakout signals:")
    print(breakouts)
    breakouts.to_csv(Config.TMP_DIR/'minervini_breakouts.csv', index=False)
    conn = sqlite3.connect(Config.BHAV_DB_FILE_PATH)
    #finalized = reviewSymbols(radar['symbol'].tolist())
    #print("\nFinalized symbols:")
    #print(finalized)



if __name__ == "__main__":
    main()
