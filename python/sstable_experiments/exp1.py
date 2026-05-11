"""Exp 1: block_restart_interval sweep — port of run_exp1_restart_interval.sh."""
from __future__ import annotations

import pandas as pd

from . import plots
from .config import ExperimentConfig
from .paths import CSV_DIR, ensure_dirs
from .runner import run_experiment

DB_DIR = "/tmp/rocksdb_pyexp1"
NUM_KEYS = 500_000
VALUE_SIZE = 128
CACHE_SIZE = 64 * 1024 * 1024
INTERVALS = [4, 8, 16, 32, 64]


def main() -> pd.DataFrame:
    ensure_dirs()
    rows = []
    for interval in INTERVALS:
        write_cfg = ExperimentConfig(
            benchmarks="fillrandom", db=DB_DIR,
            num_keys=NUM_KEYS, value_size=VALUE_SIZE,
            block_restart_interval=interval, cache_size=CACHE_SIZE,
        )
        w = run_experiment(write_cfg, reset_db=True)
 
        read_cfg = ExperimentConfig(
            benchmarks="readrandom", db=DB_DIR,
            num_keys=NUM_KEYS, reads=100_000, value_size=VALUE_SIZE,
            block_restart_interval=interval, cache_size=CACHE_SIZE,
            use_existing_db=True,
        )
        r = run_experiment(read_cfg, reset_db=False)

        file_mb = round((w.sst_bytes_on_disk or 0) / 1024 / 1024, 2)
        rows.append({
            "interval": interval,
            "write_ops_sec": w.ops_per_sec or 0,
            "read_ops_sec": r.ops_per_sec or 0,
            "file_size_mb": file_mb,
        })
        print(f"  interval={interval} write={w.ops_per_sec} read={r.ops_per_sec} size={file_mb}MB")

    df = pd.DataFrame(rows)
    csv = CSV_DIR / "exp1_restart_interval.csv"
    df.to_csv(csv, index=False)
    plots.restart_interval(df)
    print(f"-> {csv}")
    return df


if __name__ == "__main__":
    main()
