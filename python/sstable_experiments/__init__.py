"""sstable_experiments — Python-driven RocksDB SSTable experiments.

Subprocess-based pass (current): wraps the existing `db_bench` binary.
A pybind11-backed pass will land later for crash-injection scenarios.
"""

from .config import ExperimentConfig
from .runner import run_experiment, BenchResult

__all__ = ["ExperimentConfig", "run_experiment", "BenchResult"]
