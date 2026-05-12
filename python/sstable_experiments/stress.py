"""Stress / failure experiments for Requirement §E.

§E.1 size       — data-size scaling, plot write-amp vs dataset size.
§E.2 skew       — sweep zipfian a-parameter, plot cache hit + throughput.
§E.3 crash      — SIGKILL mid-write, verify WAL replay on reopen.
§E.4 assumption — disable group commit (sync=1) and constrain bg threads.

CLI:
    python -m sstable_experiments.stress --scenario {size,skew,assumption,crash}
"""
from __future__ import annotations

import argparse
from typing import Dict

import pandas as pd

from . import plots
from .config import ExperimentConfig
from .metrics import cache_hit_ratio, total_bytes_written
from .paths import CSV_DIR, ensure_dirs
from .runner import run_experiment

DB_BASE = "/tmp/rocksdb_pystress"
VALUE_SIZE = 256
KEY_OVERHEAD_BYTES = 24


# ---------------- §E.1 -------------------------------------------------------
def scenario_size() -> pd.DataFrame:
    rows = []
    for n in [100_000, 500_000, 1_000_000, 5_000_000, 10_000_000]:
        res = run_experiment(ExperimentConfig(
            benchmarks="fillrandom", db=f"{DB_BASE}_size",
            num_keys=n, value_size=VALUE_SIZE, bloom_bits=10,
            block_size=4096, cache_size=64 * 1024 * 1024,
            max_bytes_for_level_base=10 * 1024 * 1024,
            level_compaction_dynamic_level_bytes=0,
            statistics=1,
        ), reset_db=True)
        bytes_written = total_bytes_written(res.stats)
        user_bytes = n * (VALUE_SIZE + KEY_OVERHEAD_BYTES)
        rows.append({
            "num_keys": n,
            "bytes_written_mb": round(bytes_written / 1048576, 2),
            "user_data_mb": round(user_bytes / 1048576, 2),
            "write_amp": round(bytes_written / user_bytes, 2) if user_bytes else 0,
        })
        print(f"  n={n} WA={rows[-1]['write_amp']}")
    df = pd.DataFrame(rows)
    df.to_csv(CSV_DIR / "stress_e1_data_size.csv", index=False)
    plots.stress_data_size(df)
    return df


# ---------------- §E.2 -------------------------------------------------------
def scenario_skew() -> pd.DataFrame:
    DB = f"{DB_BASE}_skew"
    # Seed once
    run_experiment(ExperimentConfig(
        benchmarks="fillrandom", db=DB, num_keys=500_000,
        value_size=VALUE_SIZE, bloom_bits=10,
        cache_size=512 * 1024 * 1024,
    ), reset_db=True)

    rows = []
    for theta in [0.0, 0.5, 0.8, 0.99, 1.2]:
        cfg = ExperimentConfig(
            benchmarks="readrandom", db=DB, num_keys=500_000, reads=200_000,
            value_size=VALUE_SIZE, bloom_bits=10,
            cache_size=64 * 1024 * 1024, statistics=1,
            use_existing_db=True,
        )
        if theta > 0:
            cfg.keyrange_dist_a = theta
            cfg.keyrange_dist_b = 0.0
            cfg.keyrange_dist_c = 0.0
            cfg.keyrange_dist_d = 0.0
        res = run_experiment(cfg, reset_db=False)
        rows.append({
            "theta": theta,
            "ops_sec": res.ops_per_sec or 0,
            "cache_hit_ratio": round(cache_hit_ratio(res.stats) or 0.0, 4),
        })
        print(f"  theta={theta} ops={res.ops_per_sec} hit={rows[-1]['cache_hit_ratio']}")
    df = pd.DataFrame(rows)
    df.to_csv(CSV_DIR / "stress_e2_skew.csv", index=False)
    plots.stress_skew(df)
    return df


# ---------------- §E.4 -------------------------------------------------------
def scenario_assumption() -> pd.DataFrame:
    """Probe two foundational assumptions: group-commit and bg-thread headroom."""
    NUM = 20_000  # small — sync_per_write does one fsync per key, very slow
    scenarios: Dict[str, ExperimentConfig] = {
        "baseline": ExperimentConfig(
            benchmarks="fillrandom", db=f"{DB_BASE}_assume_baseline",
            num_keys=NUM, value_size=VALUE_SIZE, bloom_bits=10,
            cache_size=64 * 1024 * 1024, statistics=1,
        ),
        "sync_per_write": ExperimentConfig(
            benchmarks="fillrandom", db=f"{DB_BASE}_assume_sync",
            num_keys=NUM, value_size=VALUE_SIZE, bloom_bits=10,
            cache_size=64 * 1024 * 1024, statistics=1, sync=1,
        ),
        "bg_threads_1": ExperimentConfig(
            benchmarks="fillrandom", db=f"{DB_BASE}_assume_bg1",
            num_keys=NUM, value_size=VALUE_SIZE, bloom_bits=10,
            cache_size=64 * 1024 * 1024, statistics=1, max_background_jobs=1,
        ),
    }
    rows = []
    for name, cfg in scenarios.items():
        res = run_experiment(cfg, reset_db=True)
        rows.append({"scenario": name, "ops_sec": res.ops_per_sec or 0})
        print(f"  {name}: {res.ops_per_sec} ops/sec")
    df = pd.DataFrame(rows)
    df.to_csv(CSV_DIR / "stress_e4_assumptions.csv", index=False)
    plots.stress_assumptions(df)
    return df


# ---------------- §E.3 -------------------------------------------------------
def scenario_crash() -> pd.DataFrame:
    """WAL-truncation crash recovery test.

    Sends SIGKILL to db_bench at {2,4,8,15}s, then reopens the DB and
    verifies successful WAL replay at every kill point.
    """
    import os
    import pathlib
    import shutil
    import signal
    import subprocess
    import time

    from .runner import _db_bench_path

    DB = pathlib.Path(f"{DB_BASE}_crash_wal")
    NUM = 2_000_000

    if DB.exists():
        shutil.rmtree(DB)
    DB.mkdir(parents=True)

    rows = []
    for kill_after_s in [2, 4, 8, 15]:
        if DB.exists():
            shutil.rmtree(DB)
        DB.mkdir(parents=True)

        proc = subprocess.Popen([
            str(_db_bench_path()),
            "--benchmarks=fillrandom",
            f"--db={DB}",
            f"--num={NUM}",
            "--value_size=256",
            "--bloom_bits=10",
            "--compression_type=none",
        ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

        time.sleep(kill_after_s)
        proc.send_signal(signal.SIGKILL)
        proc.wait()

        # Measure BEFORE reopen — WAL replay will flush/clear these files.
        sst_bytes = sum(p.stat().st_size for p in DB.glob("*.sst"))
        wal_bytes = sum(p.stat().st_size for p in DB.glob("*.log"))

        # Reopen: num=1 forces DB::Open which replays the WAL.
        recover = subprocess.run([
            str(_db_bench_path()),
            "--benchmarks=readseq",
            f"--db={DB}",
            "--use_existing_db=1",
            "--num=1",
            "--compression_type=none",
        ], capture_output=True, text=True)
        reopen_ok = recover.returncode == 0

        rows.append({
            "kill_after_s": kill_after_s,
            "reopen_ok": reopen_ok,
            "sst_mb": round(sst_bytes / 1048576, 2),
            "wal_mb": round(wal_bytes / 1048576, 2),
        })
        print(f"  kill@{kill_after_s}s reopen_ok={reopen_ok} "
              f"sst={sst_bytes//1048576}MB wal={wal_bytes//1048576}MB")

    df = pd.DataFrame(rows)
    df.to_csv(CSV_DIR / "stress_e3_crash_wal.csv", index=False)
    plots.stress_crash_wal(df)
    return df


SCENARIOS = {
    "size": scenario_size,
    "skew": scenario_skew,
    "assumption": scenario_assumption,
    "crash": scenario_crash,
}


def main() -> None:
    ensure_dirs()
    parser = argparse.ArgumentParser()
    parser.add_argument("--scenario", required=True, choices=sorted(SCENARIOS))
    args = parser.parse_args()
    SCENARIOS[args.scenario]()


if __name__ == "__main__":
    main()
