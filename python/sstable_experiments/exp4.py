"""Exp 4: cache_size × {uniform, zipfian} — port of run_exp4_cache_skew.sh."""
from __future__ import annotations

import pandas as pd

from . import plots
from .config import ExperimentConfig
from .metrics import cache_hit_ratio
from .paths import CSV_DIR, ensure_dirs
from .runner import run_experiment

DB_DIR = "/tmp/rocksdb_pyexp4"
NUM_KEYS = 500_000
READS = 200_000
VALUE_SIZE = 256
SEED_CACHE = 512 * 1024 * 1024
CACHES_MB = [1, 16, 64, 256]


def _seed_db() -> None:
    run_experiment(ExperimentConfig(
        benchmarks="fillrandom", db=DB_DIR, num_keys=NUM_KEYS,
        value_size=VALUE_SIZE, bloom_bits=10, cache_size=SEED_CACHE,
    ), reset_db=True)


def main() -> pd.DataFrame:
    ensure_dirs()
    _seed_db()
    rows = []
    for cache_mb in CACHES_MB:
        for dist in ("uniform", "zipfian"):
            cfg = ExperimentConfig(
                benchmarks="readrandom", db=DB_DIR,
                num_keys=NUM_KEYS, reads=READS, value_size=VALUE_SIZE,
                bloom_bits=10, cache_size=cache_mb * 1024 * 1024,
                statistics=1, use_existing_db=True,
            )
            if dist == "zipfian":
                cfg.keyrange_dist_a = 0.9
                cfg.keyrange_dist_b = 0.0
                cfg.keyrange_dist_c = 0.0
                cfg.keyrange_dist_d = 0.0
            res = run_experiment(cfg, reset_db=False)
            ratio = cache_hit_ratio(res.stats) or 0.0
            rows.append({
                "cache_mb": cache_mb,
                "distribution": dist,
                "ops_sec": res.ops_per_sec or 0,
                "cache_hit_ratio": round(ratio, 4),
            })
            print(f"  cache={cache_mb}MB dist={dist} ops={res.ops_per_sec} hit={ratio:.4f}")

    df = pd.DataFrame(rows)
    csv = CSV_DIR / "exp4_cache_skew.csv"
    df.to_csv(csv, index=False)
    plots.cache_zipfian(df)
    print(f"-> {csv}")
    return df


if __name__ == "__main__":
    main()
