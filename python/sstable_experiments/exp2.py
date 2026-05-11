"""Exp 2: bloom bits/key sweep — port of run_exp2_bloom_filter.sh."""
from __future__ import annotations

import pandas as pd

from . import plots
from .config import ExperimentConfig
from .paths import CSV_DIR, ensure_dirs
from .runner import run_experiment

DB_DIR = "/tmp/rocksdb_pyexp2"
NUM_KEYS = 500_000
READS = 100_000
VALUE_SIZE = 128
CACHE_SIZE = 256 * 1024 * 1024
BLOOM_BITS = [0, 5, 10, 15, 20]


def _shared(bits: int, *, bench: str, use_existing: bool, reads: int | None):
    return ExperimentConfig(
        benchmarks=bench, db=DB_DIR,
        num_keys=NUM_KEYS, reads=reads, value_size=VALUE_SIZE,
        bloom_bits=bits, cache_size=CACHE_SIZE,
        use_existing_db=use_existing,
    )


def main() -> pd.DataFrame:
    ensure_dirs()
    rows = []
    for bits in BLOOM_BITS:
        w = run_experiment(_shared(bits, bench="fillrandom",
                                   use_existing=False, reads=None),
                           reset_db=True)
        r = run_experiment(_shared(bits, bench="readrandom",
                                   use_existing=True, reads=READS),
                           reset_db=False)
        m = run_experiment(_shared(bits, bench="readmissing",
                                   use_existing=True, reads=READS),
                           reset_db=False)
        rows.append({
            "bloom_bits": bits,
            "write_ops_sec": w.ops_per_sec or 0,
            "readrandom_ops_sec": r.ops_per_sec or 0,
            "readmissing_ops_sec": m.ops_per_sec or 0,
        })
        print(f"  bits={bits} write={w.ops_per_sec} read={r.ops_per_sec} miss={m.ops_per_sec}")

    df = pd.DataFrame(rows)
    csv = CSV_DIR / "exp2_bloom_filter.csv"
    df.to_csv(csv, index=False)
    plots.bloom_fpr(df)
    print(f"-> {csv}")
    return df


if __name__ == "__main__":
    main()
