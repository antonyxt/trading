
import sys
import os

# Get the absolute path of the root directory
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..')) # Adjust '..' based on nesting

# Insert the root directory path to the beginning of sys.path
sys.path.insert(0, project_root)
import sqlite3
import pandas as pd
from config import Config


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

def add_macd(df):

    df = df.copy()

    df['ema12'] = (
        df.groupby('symbol')['close']
        .transform(lambda x: x.ewm(span=12, adjust=False).mean())
    )

    df['ema26'] = (
        df.groupby('symbol')['close']
        .transform(lambda x: x.ewm(span=26, adjust=False).mean())
    )

    df['macd'] = df['ema12'] - df['ema26']

    df['signal'] = (
        df.groupby('symbol')['macd']
        .transform(lambda x: x.ewm(span=9, adjust=False).mean())
    )

    df['hist'] = df['macd'] - df['signal']

    return df

def detect_local_pivots(df):

    g = df.groupby('symbol')

    df['local_high'] = (
        (df['high'] > g['high'].shift(1)) &
        (df['high'] > g['high'].shift(-1))
    )

    df['local_low'] = (
        (df['low'] < g['low'].shift(1)) &
        (df['low'] < g['low'].shift(-1))
    )

    return df

def detect_swing_highs(df):

    lh = df[df['local_high']].copy()

    lh['prev_high'] = lh.groupby('symbol')['high'].shift(1)
    lh['next_high'] = lh.groupby('symbol')['high'].shift(-1)

    lh['swing_high'] = (
        (lh['high'] > lh['prev_high']) &
        (lh['high'] > lh['next_high'])
    )

    # merge back to main df
    df['swing_high'] = False
    df.loc[lh.index, 'swing_high'] = lh['swing_high']

    return df

def detect_swing_lows(df):

    ll = df[df['local_low']].copy()

    ll['prev_low'] = ll.groupby('symbol')['low'].shift(1)
    ll['next_low'] = ll.groupby('symbol')['low'].shift(-1)

    ll['swing_low'] = (
        (ll['low'] < ll['prev_low']) &
        (ll['low'] < ll['next_low'])
    )

    df['swing_low'] = False
    df.loc[ll.index, 'swing_low'] = ll['swing_low']

    return df

def macd_at_swings(df, window=2):

    g = df.groupby('symbol')

    df['macd_high_val'] = (
        g['macd']
        .transform(lambda x:
            x.rolling(window*2+1, center=True).max()
        )
    )

    df['macd_low_val'] = (
        g['macd']
        .transform(lambda x:
            x.rolling(window*2+1, center=True).min()
        )
    )

    return df

def filter_recent_signals(df, n=5):
    return (
        df.groupby('symbol')
        .tail(n)
        .copy()
    )

def bearish_divergence(df):

    sh = df[df['swing_high']].copy()

    sh['prev_price'] = (
        sh.groupby('symbol')['high'].shift(1)
    )

    sh['prev_macd'] = (
        sh.groupby('symbol')['macd_high_val'].shift(1)
    )

    # Regular bearish
    sh['regular_bearish'] = (
        (sh['high'] > sh['prev_price']) &
        (sh['macd_high_val'] < sh['prev_macd'])
    )

    # Hidden bearish
    sh['hidden_bearish'] = (
        (sh['high'] < sh['prev_price']) &
        (sh['macd_high_val'] > sh['prev_macd'])
    )

    return sh

def bullish_divergence(df):

    sl = df[df['swing_low']].copy()

    sl['prev_price'] = (
        sl.groupby('symbol')['low'].shift(1)
    )

    sl['prev_macd'] = (
        sl.groupby('symbol')['macd_low_val'].shift(1)
    )

    sl['regular_bullish'] = (
        (sl['low'] < sl['prev_price']) &
        (sl['macd_low_val'] > sl['prev_macd'])
    )

    sl['hidden_bullish'] = (
        (sl['low'] > sl['prev_price']) &
        (sl['macd_low_val'] < sl['prev_macd'])
    )

    return sl
def main():
    print(f"✅ Loading Data...")
    df = load_data(Config.BHAV_DB_FILE_PATH)
    print(f"✅ Adding MACD...")
    df = add_macd(df)
    print(f"✅ Detecting Local Pivots...")
    df = detect_local_pivots(df)
    print(f"✅ Detecting Swing Highs...")
    df = detect_swing_highs(df)
    print(f"✅ Detecting Swing Lows...")
    df = detect_swing_lows(df)
    print(f"✅ Calculating MACD at Swings...")
    df = macd_at_swings(df)
    print(f"✅ Filtering Recent Signals...")

    bearish = bearish_divergence(df)
    bullish = bullish_divergence(df)
    max_date = df['trade_date'].max()
    cutoff = max_date - pd.Timedelta(days=5)
    bearish_recent = bearish[bearish['trade_date'] >= cutoff]
    bullish_recent = bullish[bullish['trade_date'] >= cutoff]
    print("Bearish Divergence Regular:")
    print(bearish_recent[bearish_recent['regular_bearish'] | bearish_recent['hidden_bearish']])
    bearishDf = bearish_recent[bearish_recent['regular_bearish'] | bearish_recent['hidden_bearish']]
    bearishDf.to_csv(Config.TMP_DIR/'bearish_divergence.csv', index=False)
    print("Bullish Divergence:")
    print(bullish_recent[bullish_recent['regular_bullish'] | bullish_recent['hidden_bullish']])
    bullishDf = bullish_recent[bullish_recent['regular_bullish'] | bullish_recent['hidden_bullish']]
    bullishDf.to_csv(Config.TMP_DIR/'bullish_divergence.csv', index=False)

if __name__ == "__main__":
    main()