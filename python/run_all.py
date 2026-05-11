"""Run all five experiments end-to-end and print a summary table."""
from __future__ import annotations

import time

import pandas as pd

from sstable_experiments import exp1, exp2, exp3, exp4, exp5


def _timed(name, fn):
    t0 = time.time()
    df = fn()
    return {"experiment": name, "rows": len(df), "secs": round(time.time() - t0, 1)}


def main() -> None:
    summary = [
        _timed("exp1_restart_interval", exp1.main),
        _timed("exp2_bloom_filter",     exp2.main),
        _timed("exp3_block_size",       exp3.main),
        _timed("exp4_cache_skew",       exp4.main),
        _timed("exp5_write_amp",        exp5.main),
    ]
    print()
    print(pd.DataFrame(summary).to_string(index=False))


if __name__ == "__main__":
    main()
