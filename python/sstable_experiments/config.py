"""ExperimentConfig — typed mirror of the db_bench flags this project uses."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional


@dataclass
class ExperimentConfig:
    # Workload
    benchmarks: str = "fillrandom"            # fillrandom, readrandom, readmissing, ...
    num_keys: int = 500_000
    reads: Optional[int] = None               # defaults to num_keys when None
    value_size: int = 128
    use_existing_db: bool = False

    # SST shape
    block_size: Optional[int] = None          # bytes; None → db_bench default
    block_restart_interval: Optional[int] = None
    bloom_bits: Optional[int] = None
    compression_type: str = "none"

    # Cache + level shape
    cache_size: int = 64 * 1024 * 1024
    max_bytes_for_level_base: Optional[int] = None
    level_compaction_dynamic_level_bytes: Optional[int] = None

    # Stats / instrumentation
    statistics: int = 0

    # Distribution flags (exp4 zipfian)
    keyrange_dist_a: Optional[float] = None
    keyrange_dist_b: Optional[float] = None
    keyrange_dist_c: Optional[float] = None
    keyrange_dist_d: Optional[float] = None

    # Crash / failure controls (used by stress.py)
    sync: Optional[int] = None
    max_background_jobs: Optional[int] = None

    # Filesystem
    db: str = "/tmp/rocksdb_pyexp"

    # Free-form extras (`["--foo=bar", ...]`) for one-off flags
    extra_args: List[str] = field(default_factory=list)

    def to_db_bench_args(self) -> List[str]:
        out: List[str] = [
            f"--benchmarks={self.benchmarks}",
            f"--db={self.db}",
            f"--num={self.num_keys}",
            f"--value_size={self.value_size}",
            f"--cache_size={self.cache_size}",
            f"--statistics={self.statistics}",
            f"--compression_type={self.compression_type}",
        ]
        if self.reads is not None:
            out.append(f"--reads={self.reads}")
        if self.use_existing_db:
            out.append("--use_existing_db=1")
        for flag, val in [
            ("block_size", self.block_size),
            ("block_restart_interval", self.block_restart_interval),
            ("bloom_bits", self.bloom_bits),
            ("max_bytes_for_level_base", self.max_bytes_for_level_base),
            ("level_compaction_dynamic_level_bytes",
             self.level_compaction_dynamic_level_bytes),
            ("sync", self.sync),
            ("max_background_jobs", self.max_background_jobs),
            ("keyrange_dist_a", self.keyrange_dist_a),
            ("keyrange_dist_b", self.keyrange_dist_b),
            ("keyrange_dist_c", self.keyrange_dist_c),
            ("keyrange_dist_d", self.keyrange_dist_d),
        ]:
            if val is not None:
                out.append(f"--{flag}={val}")
        out.extend(self.extra_args)
        return out
