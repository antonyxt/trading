import sqlite3
import shutil
from pathlib import Path
from datetime import datetime

import pandas as pd

from config import Config


# =========================
# Date Utilities
# =========================

def nse_date_to_yyyymmdd(date_str: str) -> str:
    """
    Convert '19-Jan-2026' -> '2026-01-19'
    Handles extra spaces from NSE data
    """
    date_str = date_str.strip()
    dt = datetime.strptime(date_str, "%d-%b-%Y")
    return dt.strftime("%Y-%m-%d")


# =========================
# File Discovery
# =========================

def list_bhavcopy_files(folder: Path) -> list[Path]:
    """
    List files matching YYYY-MM-DD.csv and sort by date
    """
    files = []
    for f in folder.glob("*.csv"):
        try:
            datetime.strptime(f.stem, "%Y-%m-%d")
            files.append(f)
        except ValueError:
            continue

    return sorted(files, key=lambda f: f.stem)


def move_to_backup(src: Path, backup_dir: Path):
    backup_dir.mkdir(parents=True, exist_ok=True)
    dest = backup_dir / src.name
    shutil.move(str(src), str(dest))


# =========================
# Database
# =========================

def get_connection() -> sqlite3.Connection:
    Config.BHAV_DB_DIR.mkdir(parents=True, exist_ok=True)
    return sqlite3.connect(Config.BHAV_DB_FILE_PATH)


def create_table(conn: sqlite3.Connection):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS bhavcopy (
            trade_date TEXT NOT NULL,     -- YYYYMMDD
            symbol TEXT NOT NULL,
            series TEXT,
            prev_close REAL,
            open_price REAL,
            high_price REAL,
            low_price REAL,
            last_price REAL,
            close_price REAL,
            avg_price REAL,
            ttl_trd_qnty INTEGER,
            turnover_lacs REAL,
            no_of_trades INTEGER,
            deliv_qty INTEGER,
            deliv_per REAL,
            PRIMARY KEY (trade_date, symbol)
        );
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_symbol_date_desc
        ON bhavcopy (symbol, trade_date ASC);
    """)
    conn.commit()


# =========================
# Insert Logic
# =========================

def insert_csv_into_db(csv_file: Path, conn: sqlite3.Connection):
    df = pd.read_csv(csv_file, skipinitialspace=True)
    df.columns = df.columns.str.strip()
    df = df.apply(lambda col: col.str.strip() if col.dtype == "object" else col)
    

    # Convert NSE DATE1 -> YYYY-MM-DD
    df.columns = df.columns.str.upper()
    df["trade_date"] = df["DATE1"].apply(nse_date_to_yyyymmdd)

    # Validate filename date vs data date
    if not (df["trade_date"] == csv_file.stem).all():
        raise ValueError(f"Date mismatch in {csv_file.name}")
    
    
    # Rename columns
    df = df.rename(columns={
        "SYMBOL": "symbol",
        "SERIES": "series",
        "PREV_CLOSE": "prev_close",
        "OPEN_PRICE": "open_price",
        "HIGH_PRICE": "high_price",
        "LOW_PRICE": "low_price",
        "LAST_PRICE": "last_price",
        "CLOSE_PRICE": "close_price",
        "AVG_PRICE": "avg_price",
        "TTL_TRD_QNTY": "ttl_trd_qnty",
        "TURNOVER_LACS": "turnover_lacs",
        "NO_OF_TRADES": "no_of_trades",
        "DELIV_QTY": "deliv_qty",
        "DELIV_PER": "deliv_per",
    })
    

    df = df[df["series"].isin(Config.ALLOWED_SERIES)]

    df = df[
        [
            "trade_date",
            "symbol",
            "series",
            "prev_close",
            "open_price",
            "high_price",
            "low_price",
            "last_price",
            "close_price",
            "avg_price",
            "ttl_trd_qnty",
            "turnover_lacs",
            "no_of_trades",
            "deliv_qty",
            "deliv_per",
        ]
    ]

    df.to_sql(
        "bhavcopy",
        conn,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=50
    )


# =========================
# Main Loader
# =========================

def load_all_bhavcopies():
    Config.BHAVCOPY_DIR.mkdir(parents=True, exist_ok=True)

    conn = get_connection()
    create_table(conn)

    files = list_bhavcopy_files(Config.BHAVCOPY_DIR)
    print(f"📂 Found {len(files)} bhavcopy files")

    for csv_file in files:
        try:
            insert_csv_into_db(csv_file, conn)
            conn.commit()
            move_to_backup(csv_file, Config.BHAV_BACKUP_DIR)
            print(f"✅ Loaded & backed up {csv_file.name}")

        except sqlite3.IntegrityError:
            # Duplicate (already loaded)
            print(f"⚠ Duplicate data: {csv_file.name} → moving to backup")
            move_to_backup(csv_file, Config.BHAV_BACKUP_DIR)

        except Exception as e:
            conn.rollback()
            print(f"❌ Failed {csv_file.name}: {e}")

    conn.close()


# =========================
# Entry Point
# =========================

def main():
    load_all_bhavcopies()

# python db_loader.py
if __name__ == "__main__":
    main()
