import sqlite3
import pandas as pd
import numpy as np
from tqdm import tqdm
from config import Config


EMA_MAX_SPAN = 21  # max EMA span for EMA21 continuity
MIN_RUN_LENGTH = 5  # filter runs shorter than this


# =================================================
# 1. Init output DB
# ALTER TABLE bhavcopy ADD COLUMN rank INTEGER;

"""
UPDATE bhavcopy
SET rank = (
    SELECT CASE
        WHEN signal = -1 AND count > 25 THEN 1
        WHEN signal = -1 AND count > 15 THEN 2
        WHEN signal = -1 AND count > 5  THEN 3
        WHEN signal = 1  AND count > 25 THEN 7
        WHEN signal = 1  AND count > 15 THEN 6
        WHEN signal = 1  AND count > 5  THEN 5
        ELSE 4
    END
    FROM signal_runs r
    WHERE r.symbol = bhavcopy.symbol
      AND r.trade_date = bhavcopy.trade_date
)



"""
# =================================================
def init_output_db(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS signal_runs (
            trade_date TEXT,
            symbol TEXT,
            signal INTEGER,
            count INTEGER,
            PRIMARY KEY (trade_date, symbol)
        )
    """)
    conn.commit()


# =================================================
# 2. Get last run per symbol
# =================================================
def get_last_run(conn, symbol):
    last_run = pd.read_sql(
        """
        SELECT trade_date, signal, count
        FROM signal_runs
        WHERE symbol = ?
        ORDER BY trade_date DESC
        LIMIT 1
        """,
        conn,
        params=[symbol]
    )
    if last_run.empty:
        return None
    return last_run.iloc[0]  # returns a Series


# =================================================
# 3. Process one symbol
# =================================================
def process_symbol(symbol, conn):
    # Get last run for this symbol
    last_run = get_last_run(conn, symbol)
    overlap_date = None
    if last_run is not None:
        overlap_date = pd.to_datetime(last_run['trade_date']) - pd.Timedelta(days=EMA_MAX_SPAN)

    # Read bhavcopy with overlap for EMA continuity
    query = "SELECT trade_date, close_price FROM bhavcopy WHERE symbol = ?"
    params = [symbol]
    if overlap_date is not None:
        query += " AND trade_date >= ?"
        params.append(overlap_date.strftime('%Y-%m-%d'))
    query += " ORDER BY trade_date"

    df = pd.read_sql(query, conn, params=params)
    if df.empty or len(df) < EMA_MAX_SPAN + 3:
        return  # not enough data

    df['trade_date'] = pd.to_datetime(df['trade_date'], format='%Y-%m-%d')

    # --- EMA ---
    df['ema10'] = df['close_price'].ewm(span=10, adjust=False).mean()
    df['ema21'] = df['close_price'].ewm(span=21, adjust=False).mean()

    # --- slopes ---
    slope10 = np.sign(df['ema10'].diff()).fillna(0).astype('int8')
    slope21 = np.sign(df['ema21'].diff()).fillna(0).astype('int8')

    # --- signal boolean mask ---
    signal = np.zeros(len(df), dtype='int8')
    signal[(slope10 == 1) & (slope21 == 1)] = 1
    signal[(slope10 == -1) & (slope21 == -1)] = -1

    # --- detect runs ---
    signal_change = np.diff(signal, prepend=signal[0]) != 0
    group_id = np.cumsum(signal_change)

    df['signal'] = signal
    df['group_id'] = group_id

    # --- aggregate runs ---
    runs = df.groupby('group_id').agg(
        trade_date=('trade_date', 'first'),
        trade_date_p1=('trade_date', lambda x: x.iloc[1] if len(x) > 1 else pd.NaT),
        signal=('signal', 'first'),
        count=('signal', 'size')
    ).reset_index(drop=True)

    # --- trade_date_p2 = previous row in df list ---
    df_index_map = pd.Series(df.index.values, index=df['trade_date'])
    run_indices = df_index_map[runs['trade_date']].values
    runs['trade_date_p2'] = df['trade_date'].iloc[np.maximum(run_indices - 1, 0)].values

    # --- extend last run if needed ---
    if last_run is not None:
        first_run_signal = runs['signal'].iloc[0]
        if first_run_signal == last_run['signal']:
            # Extend the last run count
            new_count = last_run['count'] + runs['count'].iloc[0]
            conn.execute(
                "UPDATE signal_runs SET count = ? WHERE trade_date = ? AND symbol = ?",
                (new_count, last_run['trade_date'], symbol)
            )
            # Drop first run from current runs since it is merged
            runs = runs.iloc[1:].reset_index(drop=True)

    # filter runs: signal != 0 and count >= MIN_RUN_LENGTH
    runs = runs[(runs['signal'] != 0) & (runs['count'] >= MIN_RUN_LENGTH)]
    if runs.empty:
        return

    runs['symbol'] = symbol

    # --- melt 3 dates per run into rows ---
    final_rows = pd.melt(
        runs,
        id_vars=['symbol', 'signal', 'count'],
        value_vars=['trade_date_p2', 'trade_date', 'trade_date_p1'],
        value_name='run_date'
    ).drop(columns=['variable'])
    final_rows.rename(columns={'run_date': 'trade_date'}, inplace=True)
    # drop NaT
    final_rows = final_rows.dropna(subset=['trade_date'])
    final_rows['trade_date'] = pd.to_datetime(final_rows['trade_date']).dt.strftime('%Y-%m-%d')

    # --- write to SQLite ---
    final_rows.to_sql('signal_runs', conn, if_exists='append', index=False)

    # free memory
    del df, slope10, slope21, signal, runs, final_rows


# =================================================
# 4. Main driver
# =================================================
def main():
    conn = sqlite3.connect(Config.BHAV_DB_FILE_PATH)
    init_output_db(conn)

    # get all symbols
    symbols = pd.read_sql("SELECT DISTINCT symbol FROM bhavcopy ORDER BY symbol", conn)['symbol']

    for symbol in tqdm(symbols, desc="Processing symbols", unit="symbol"):
        process_symbol(symbol, conn)

    conn.commit()
    conn.close()


if __name__ == "__main__":
    main()
