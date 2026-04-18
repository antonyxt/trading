import argparse
import os
from datetime import datetime, timedelta
from typing import Iterator, Set

import pandas as pd
from nsepython import get_bhavcopy, nse_holidays
from config import Config


# =========================
# Utility Functions
# =========================

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = df.columns.str.strip()
    return df

def normalize_nse_date(date_str: str, fallback_year: int) -> str:
    """
    Fix malformed NSE date strings like ' 16-Jan-202'
    """
    date_str = date_str.strip()

    # If year is truncated (YYYY -> YYY)
    if len(date_str) == 10:
        date_str = f"{date_str}{str(fallback_year)[-1]}"

    return date_str


def nse_date_str(date_obj: datetime) -> str:
    return date_obj.strftime("%d-%m-%Y")


def output_filename(date_obj: datetime) -> str:
    return date_obj.strftime("%Y-%m-%d") + ".csv"

def extract_bhavcopy_date(df, requested_date):
    """
    Extract and normalize bhavcopy date
    """
    raw_date = df["DATE1"].iloc[0]
    fixed_date = normalize_nse_date(raw_date, requested_date.year)

    return datetime.strptime(fixed_date, "%d-%b-%Y").date()


# =========================
# Holiday Logic
# =========================

def get_nse_holiday_dates() -> Set[datetime.date]:
    """
    Fetch NSE holidays and return a set of dates
    """
    holidays = nse_holidays()
    holiday_dates = set()

    for h in holidays.get("CM", []):
        holiday_dates.add(
            datetime.strptime(h["tradingDate"], "%d-%b-%Y").date()
        )

    return holiday_dates


def is_trading_day(date_obj: datetime, holiday_set: Set[datetime.date]) -> bool:
    if date_obj.weekday() >= 5:  # Saturday / Sunday
        return False
    if date_obj.date() in holiday_set:
        return False
    return True


# =========================
# Core Download Logic
# =========================

def download_and_save(date_obj: datetime, output_dir: str) -> bool:
    try:
        dateString=nse_date_str(date_obj)
        df = get_bhavcopy(dateString)
        if df is None or df.empty:
            print(f"❌ No data for {date_obj.date()}")
            return False

        df = normalize_columns(df)
        bhav_date = extract_bhavcopy_date(df, date_obj)

        if bhav_date != date_obj.date():
            print(f"⚠ Date mismatch for {date_obj.date()}")
            return False

        filepath = os.path.join(output_dir, output_filename(date_obj))
        df.to_csv(filepath, index=False)

        print(f"✅ Saved {filepath}")
        return True

    except Exception as e:
        print(f"❌ Error on {date_obj.date()}: {e}")
        return False


# =========================
# Iterators
# =========================

def backward_date_iterator(start_dt: datetime, end_dt: datetime) -> Iterator[datetime]:
    current = start_dt
    while current >= end_dt:
        yield current
        current -= timedelta(days=1)


def iterate_by_days(start_dt: datetime, days: int, output_dir: str, holiday_set: Set[datetime.date]):
    end_dt = start_dt - timedelta(days=days - 1)
    for dt in backward_date_iterator(start_dt, end_dt):
        if is_trading_day(dt, holiday_set):
            download_and_save(dt, output_dir)


def iterate_by_range(start_dt: datetime, end_dt: datetime, output_dir: str, holiday_set: Set[datetime.date]):
    if end_dt > start_dt:
        raise ValueError("end-date must be earlier than or equal to start-date")

    for dt in backward_date_iterator(start_dt, end_dt):
        if is_trading_day(dt, holiday_set):
            download_and_save(dt, output_dir)


# =========================
# CLI
# =========================

def parse_args():
    parser = argparse.ArgumentParser(
        description="Download NSE Bhavcopies using nsepython"
    )

    parser.add_argument("--start-date", type=str, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", type=str, help="End date (YYYY-MM-DD)")
    parser.add_argument("--days", type=int, help="Number of backward days")

    return parser.parse_args()


# =========================
# Main
# =========================

def main():
    args = parse_args()
    holiday_set = get_nse_holiday_dates()
    output_dir = Config.BHAVCOPY_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    # ---- DEFAULT: Today only ----
    if not args.start_date and not args.days and not args.end_date:
        today = datetime.today()
        if is_trading_day(today, holiday_set):
            download_and_save(today, output_dir)
        else:
            print("⚠ Today is not a trading day")
        return

    # ---- Parse start date ----
    if not args.start_date:
        raise ValueError("--start-date is required when using --days or --end-date")

    start_dt = datetime.strptime(args.start_date, "%Y-%m-%d")

    # ---- Mode 1: start + days ----
    if args.days:
        iterate_by_days(start_dt, args.days, output_dir, holiday_set)
        return

    # ---- Mode 2: start + end ----
    if args.end_date:
        end_dt = datetime.strptime(args.end_date, "%Y-%m-%d")
        iterate_by_range(start_dt, end_dt, output_dir, holiday_set)
        return

    raise ValueError("Invalid argument combination")

# py bhavcopy_downloader.py --start-date="2026-03-21" --end-date="2026-03-19"
if __name__ == "__main__":
    main()
