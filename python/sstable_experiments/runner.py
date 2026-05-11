"""run_experiment — subprocess wrapper around the db_bench binary.

Subprocess pass (no pybind11 yet). Locates db_bench at REPO_ROOT/db_bench by
default; override with the SSTABLE_DB_BENCH environment variable.
"""
from __future__ import annotations

import os
import pathlib
import shutil
import subprocess
from dataclasses import dataclass
from typing import Dict, Optional

from .config import ExperimentConfig
from .metrics import parse_bench_ops, parse_statistics

REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
DEFAULT_DB_BENCH = REPO_ROOT / "db_bench"


def _db_bench_path() -> pathlib.Path:
    override = os.environ.get("SSTABLE_DB_BENCH")
    if override:
        return pathlib.Path(override)
    return DEFAULT_DB_BENCH


@dataclass
class BenchResult:
    benchmark: str
    ops_per_sec: Optional[int]
    stats: Dict[str, int]
    stdout: str
    sst_bytes_on_disk: Optional[int]
    returncode: int


def _sst_bytes(db_dir: pathlib.Path) -> int:
    total = 0
    if not db_dir.exists():
        return 0
    for p in db_dir.glob("*.sst"):
        try:
            total += p.stat().st_size
        except OSError:
            pass
    return total


def run_experiment(cfg: ExperimentConfig,
                   *, reset_db: bool = True,
                   timeout_s: Optional[float] = None) -> BenchResult:
    """Run db_bench once with `cfg` and return parsed metrics.

    `reset_db=True` wipes cfg.db before the run (use False for read-after-write
    phases that need an existing DB).
    """
    db_dir = pathlib.Path(cfg.db)
    if reset_db:
        if db_dir.exists():
            shutil.rmtree(db_dir)
        db_dir.mkdir(parents=True, exist_ok=True)

    args = [str(_db_bench_path()), *cfg.to_db_bench_args()]
    proc = subprocess.run(
        args, capture_output=True, text=True, timeout=timeout_s,
    )
    combined = (proc.stdout or "") + "\n" + (proc.stderr or "")

    # First benchmark name in the comma-separated list — the one whose ops/sec
    # we report (later phases re-call run_experiment).
    primary_bench = cfg.benchmarks.split(",")[0]

    return BenchResult(
        benchmark=primary_bench,
        ops_per_sec=parse_bench_ops(combined, primary_bench),
        stats=parse_statistics(combined),
        stdout=combined,
        sst_bytes_on_disk=_sst_bytes(db_dir),
        returncode=proc.returncode,
    )
