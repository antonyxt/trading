import json
import sqlite3
import numpy as np
import torch
from torch.utils.data import Dataset
from tqdm import tqdm
import os
import pandas as pd
import numpy as np

FEATURE_LIST = ['prev_close', 'open_price','high_price','low_price',
                'last_price','close_price','avg_price', 'ttl_trd_qnty',
                'no_of_trades', 'deliv_qty']
# TODO, load mean & stddev and apply natural  logs to all items except delivery %
# TODO save only trained symbols, no need to store skipped symbols
class StreamingDoveDataset(Dataset):
    def __init__(self, db_path, norm_stat_path, date_threshold, symbol_map, seq_len=150, label_col="rank", stride=5, features=FEATURE_LIST,
                 unk_id=None, num_workers=8, mmap_dir=None, preload=True):
        """
        TPU/GPU-optimized streaming dataset.
        Supports in-memory (GPU) or memory-mapped (TPU-safe) loading.
        """
        self.DATE_THRESHOLD = date_threshold
        self.seq_len = seq_len
        self.stride = stride
        self.features = features
        self.label_col = label_col
        self.unk_id = unk_id
        self.num_workers = num_workers
        self.mmap_dir = mmap_dir
        self.preload = preload

        conn = sqlite3.connect(db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        self.normalization_stats = np.load(
            norm_stat_path,
            allow_pickle=True
        ).item()
        norm_mean = self.normalization_stats["mean"].astype(np.float32)
        norm_std  = np.maximum(
            self.normalization_stats["std"].astype(np.float32),
            1e-8
        )
        log_mask = np.array(
            [f != "deliv_qty" for f in self.features],
            dtype=bool
        )

        # Cache
        self.cache = {}
        self.seq_counts = []
        self.skipped_symbols = []
        self.all_labels = []
        self.symbolDF = pd.read_csv(symbol_map)
        self.symbol_ids = self.symbolDF["index"].tolist()
        print(f"Loading {len(self.symbol_ids)} symbols into cache...")
        tqdmBar = tqdm(
            zip(self.symbolDF["index"], self.symbolDF["symbol"]),
            total=len(self.symbolDF),
            desc="Loading symbols"
        )
        for sym_id, symbol in tqdmBar:
            cur = conn.cursor()
            cur.execute(f"""
                SELECT {','.join(self.features)}, {self.label_col}
                FROM bhavcopy
                WHERE symbol=? AND {self.label_col} IS NOT NULL AND trade_date <= ?
                ORDER BY trade_date ASC
            """, (symbol, self.DATE_THRESHOLD))
            rows = cur.fetchall()
            cur.close()

            if not rows:
                self.skipped_symbols.append(sym_id)
                continue

            x_all = np.array([[float(r[f]) if r[f] is not None else np.nan for f in self.features]
                              for r in rows], dtype=np.float32)
            # log1p (only non-delivery columns)
            x_all[:, self.log_mask] = np.log1p(x_all[:, self.log_mask])
            # normalize
            x_all = (x_all - norm_mean) / norm_std
            y_all = np.array([float(r[self.label_col]) if r[self.label_col] is not None else np.nan
                              for r in rows], dtype=np.float32)

            seq_count = max(1, (len(rows) - seq_len) // stride + 1)
            seq_starts = [i * stride for i in range(seq_count)]
            for start in seq_starts:
                # We look at the 'y' value at the end of this specific sequence
                # because that is what __getitem__ returns as the label.
                target_idx = start + self.seq_len - 1                
                # Boundary check for the end of the array
                actual_idx = min(target_idx, len(y_all) - 1)
                label = int(y_all[actual_idx])                
                self.all_labels.append(label)

            if self.preload:
                self.cache[sym_id] = {"x": x_all, "y": y_all, "seq_starts": seq_starts}
            elif self.mmap_dir:
                # Save once per symbol to memmap
                os.makedirs(self.mmap_dir, exist_ok=True)
                x_path = os.path.join(self.mmap_dir, f"x_{sym_id}.npy")
                y_path = os.path.join(self.mmap_dir, f"y_{sym_id}.npy")
                np.save(x_path, x_all)
                np.save(y_path, y_all)
                self.cache[sym_id] = {"x_path": x_path, "y_path": y_path, "seq_starts": seq_starts}

            self.seq_counts.append(seq_count)

        conn.close()

        # Convert to a NumPy array for extremely fast indexing later
        self.all_labels = np.array(self.all_labels, dtype=np.int32)

        self.symbol_ids = [s for s in self.symbol_ids if s not in self.skipped_symbols]
        self.cumulative_counts = np.cumsum(self.seq_counts)

    def __len__(self):
        return int(self.cumulative_counts[-1])

    def __getitem__(self, idx):
        sym_idx = np.searchsorted(self.cumulative_counts, idx, side='right')
        symbol_id = self.symbol_ids[sym_idx]
        local_idx = idx if sym_idx == 0 else idx - self.cumulative_counts[sym_idx - 1]
        entry = self.cache[symbol_id]
        start_idx = entry['seq_starts'][local_idx]
        end_idx = start_idx + self.seq_len

        if self.preload:
            x_all, y_all = entry['x'], entry['y']
        else:
            x_all = np.load(entry['x_path'], mmap_mode='r')
            y_all = np.load(entry['y_path'], mmap_mode='r')

        x_seq = x_all[start_idx:end_idx]
        y_seq = y_all[start_idx:end_idx]

        # Pad if sequence shorter than seq_len
        if len(x_seq) < self.seq_len:
            pad_len = self.seq_len - len(x_seq)
            x_pad = np.full((pad_len, len(self.features)), np.nan, dtype=np.float32)
            y_pad = np.zeros((pad_len,), dtype=np.float32)
            x_seq = np.vstack([x_pad, x_seq])
            y_seq = np.hstack([y_pad, y_seq])

        return np.int64(symbol_id), x_seq, np.array([y_seq[-1]], dtype=np.float32)

    # -----------------------------
    # Skipped symbols save/load
    # -----------------------------
    def save_symbols_details(self, path):
        exclude_indices = set(self.skipped_symbols)
        filtered_df = self.symbolDF[~self.symbolDF["index"].isin(exclude_indices)]
        filtered_df.to_csv(path / "trainedSymbols.csv", index=False)
        np.save(path / "norm_stats.npy", self.normalization_stats)

