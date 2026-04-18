import sqlite3
from pathlib import Path
from datetime import datetime
import pandas as pd
import argparse

from config import Config


# =========================
# Utilities
# =========================

def yyyymmdd_to_nse_date(date_str: str) -> str:
    """Convert '20260116' -> '16-Jan-2026'"""
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    return dt.strftime("%d-%b-%Y")


def get_connection() -> sqlite3.Connection:
    return sqlite3.connect(Config.BHAV_DB_FILE_PATH)


def fetch_trade_dates(conn: sqlite3.Connection, first_n_days: int = None) -> list[str]:
    """Fetch distinct trade dates, sorted ascending, optionally limited"""
    query = "SELECT DISTINCT DATE1 FROM main_table ORDER BY DATE1 ASC"
    df = pd.read_sql_query(query, conn)
    dates = df["DATE1"].tolist()
    if first_n_days:
        dates = dates[:first_n_days]
    return dates


# =========================
# Export Logic
# =========================

def export_bhavcopy_for_date(trade_date: str):
    """Export a single date to NSE bhavcopy CSV"""
    conn = get_connection()

    df = pd.read_sql_query(
        "SELECT * FROM main_table WHERE DATE1 = ? ORDER BY symbol, series",
        conn,
        params=(trade_date,)
    )

    if df.empty:
        print(f"⚠ No data found for {trade_date}")
        return

    # Convert trade_date -> DATE1
    df["DATE1"] = df["DATE1"].apply(yyyymmdd_to_nse_date)

    # Column order
    nse_cols = [
        "SYMBOL", "SERIES", "DATE1", "PREV_CLOSE", "OPEN_PRICE",
        "HIGH_PRICE", "LOW_PRICE", "LAST_PRICE", "CLOSE_PRICE",
        "AVG_PRICE", "TTL_TRD_QNTY", "TURNOVER_LACS", "NO_OF_TRADES",
        "DELIV_QTY", "DELIV_PER"
    ]
    df = df[nse_cols]

    # Create folder if needed
    Config.BHAVCOPY_DIR.mkdir(parents=True, exist_ok=True)

    # Output filename
    out_file = Config.BHAVCOPY_DIR / f"{trade_date}.csv"
    df.to_csv(out_file, index=False)
    print(f"✅ Exported bhavcopy: {out_file}")


def export_bhavcopies(first_n_days: int = None):
    conn = get_connection()
    trade_dates = fetch_trade_dates(conn, first_n_days)
    print(f"📂 Exporting {len(trade_dates)} trade dates")
    for td in trade_dates:
        export_bhavcopy_for_date(td)
    conn.close()


# =========================
# CLI Entry
# =========================

def main():
    parser = argparse.ArgumentParser(
        description="Export NSE bhavcopy CSVs from SQLite DB"
    )
    parser.add_argument(
        "--first_n_days",
        type=int,
        default=None,
        help="Export only first N trade dates (default: all)"
    )

    args = parser.parse_args()
    export_bhavcopies(args.first_n_days)


if __name__ == "__main__":
    main()
