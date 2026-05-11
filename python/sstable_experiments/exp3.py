"""Exp 3: block_size sweep — port of run_exp3_block_size.sh."""
from __future__ import annotations

import pandas as pd

from . import plots
from .config import ExperimentConfig
from .paths import CSV_DIR, ensure_dirs
from .runner import run_experiment

DB_DIR = "/tmp/rocksdb_pyexp3"
NUM_KEYS = 500_000
READS = 100_000
VALUE_SIZE = 256
CACHE_SIZE = 256 * 1024 * 1024
BLOCK_SIZES = [512, 1024, 4096, 16384, 65536]


def main() -> pd.DataFrame:
    ensure_dirs()
    rows = []
    for bsize in BLOCK_SIZES:
        w = run_experiment(ExperimentConfig(
            benchmarks="fillrandom", db=DB_DIR, num_keys=NUM_KEYS,
            value_size=VALUE_SIZE, block_size=bsize, bloom_bits=10,
            cache_size=CACHE_SIZE,
        ), reset_db=True)
        r = run_experiment(ExperimentConfig(
            benchmarks="readrandom", db=DB_DIR, num_keys=NUM_KEYS, reads=READS,
            value_size=VALUE_SIZE, block_size=bsize, bloom_bits=10,
            cache_size=CACHE_SIZE, use_existing_db=True,
        ), reset_db=False)
        file_mb = round((w.sst_bytes_on_disk or 0) / 1024 / 1024, 2)
        rows.append({
            "block_size_bytes": bsize,
            "write_ops_sec": w.ops_per_sec or 0,
            "read_ops_sec": r.ops_per_sec or 0,
            "file_size_mb": file_mb,
        })
        print(f"  bs={bsize} write={w.ops_per_sec} read={r.ops_per_sec} size={file_mb}MB")

    df = pd.DataFrame(rows)
    csv = CSV_DIR / "exp3_block_size.csv"
    df.to_csv(csv, index=False)
    plots.block_size_amp(df)
    print(f"-> {csv}")
    return df


if __name__ == "__main__":
    main()
