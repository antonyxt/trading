
import sys
import os

# Get the absolute path of the root directory
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..')) # Adjust '..' based on nesting

# Insert the root directory path to the beginning of sys.path
sys.path.insert(0, project_root)
import sqlite3
import pandas as pd
from config import Config


def main():
    conn = sqlite3.connect(Config.BHAV_DB_FILE_PATH)
    query = """
        SELECT * FROM bhavcopy
        WHERE trade_date >= date('now', '-60 days')
        AND symbol IN (
            SELECT symbol
            FROM bhavcopy
            WHERE trade_date >= date('now', '-60 days')
            GROUP BY symbol
            HAVING COUNT(*) >= 30
        )
        ORDER BY symbol, trade_date;
    """
    df = pd.read_sql(query, conn)
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    df['deliv_qty'] = pd.to_numeric(df['deliv_qty'], errors='coerce')
    df = df.sort_values(["symbol", "trade_date"]).copy()
    g = df.groupby("symbol")

    # Rolling Statistics (Vectorized per Symbol)
    df['del_ma20'] = g['deliv_qty'].transform(lambda x: x.rolling(20).mean())
    df['delivery_spike'] = df['deliv_qty'] > 5 * df['del_ma20']

    latest = df.groupby('symbol').tail(1)
    candidates = latest[latest['delivery_spike']][["trade_date", "symbol","close_price", "deliv_qty", "del_ma20"]]
    
    candidates.to_csv(Config.TMP_DIR/'deliverSpike.csv', index=False)

if __name__ == "__main__":
    main()