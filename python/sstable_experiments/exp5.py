"""Exp 5: write amplification vs dataset size — port of run_exp5_write_amp.sh."""
from __future__ import annotations

import pandas as pd

from . import plots
from .config import ExperimentConfig
from .metrics import total_bytes_written
from .paths import CSV_DIR, ensure_dirs
from .runner import run_experiment

DB_DIR = "/tmp/rocksdb_pyexp5"
VALUE_SIZE = 256
NUM_KEYS_SWEEP = [100_000, 500_000, 1_000_000, 2_000_000]
KEY_OVERHEAD_BYTES = 24  # match shell script accounting


def main() -> pd.DataFrame:
    ensure_dirs()
    rows = []
    for n in NUM_KEYS_SWEEP:
        res = run_experiment(ExperimentConfig(
            benchmarks="fillrandom", db=DB_DIR, num_keys=n,
            value_size=VALUE_SIZE, bloom_bits=10, block_size=4096,
            cache_size=64 * 1024 * 1024,
            max_bytes_for_level_base=10 * 1024 * 1024,
            level_compaction_dynamic_level_bytes=0,
            statistics=1,
        ), reset_db=True)

        bytes_written = total_bytes_written(res.stats)
        user_bytes = n * (VALUE_SIZE + KEY_OVERHEAD_BYTES)
        wa = round(bytes_written / user_bytes, 2) if user_bytes else 0.0
        rows.append({
            "num_keys": n,
            "bytes_written_mb": round(bytes_written / 1048576, 2),
            "user_data_mb": round(user_bytes / 1048576, 2),
            "write_amp": wa,
        })
        print(f"  n={n} user={user_bytes/1048576:.1f}MB written={bytes_written/1048576:.1f}MB WA={wa}")

    df = pd.DataFrame(rows)
    csv = CSV_DIR / "exp5_write_amp.csv"
    df.to_csv(csv, index=False)
    plots.write_amplification(df)
    print(f"-> {csv}")
    return df


if __name__ == "__main__":
    main()
