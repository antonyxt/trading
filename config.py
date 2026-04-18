from pathlib import Path


from pathlib import Path


class Config:
    """
    Central configuration for Bhavcopy Downloader
    """

    # D:\Antony\Trading
    PROJECT_DIR = Path(__file__).resolve().parent

    # D:\Antony
    ROOT_DIR = PROJECT_DIR.parent

    # D:\Antony\out
    BASE_OUTPUT_DIR = ROOT_DIR / "out"

    # D:\Antony\out\bhavcopies
    BHAVCOPY_DIR = BASE_OUTPUT_DIR / "bhavcopies"
    BHAV_BACKUP_DIR = BASE_OUTPUT_DIR / "backup"
    BHAV_WATCH_FILE = BASE_OUTPUT_DIR / "watch/watch.csv"
    TMP_DIR =  BASE_OUTPUT_DIR / "tmp"

    BHAV_DB_DIR = ROOT_DIR / "db"
    BHAV_DB_FILE_PATH = BHAV_DB_DIR / "main.db"
    ALLOWED_SERIES = {"EQ", "BE", "BZ", "SM", "ST", "MF"}



