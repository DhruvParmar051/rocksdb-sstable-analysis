# sstable_experiments — Python-driven RocksDB SSTable experiments

This package replaces the shell-script workflow under `experiments/run_exp*.sh`
with a Python-controlled framework. Pass 1 (this commit) wraps the existing
`db_bench` binary via subprocess. Pass 2 will add a pybind11 module
(`sstable_experiments._native`) for crash-injection scenarios.

## Prerequisites

1. Build `db_bench` once at the repo root:

   ```bash
   cd ../rocksdb
   make static_lib db_bench -j
   cp db_bench ../db_bench        # the runner expects REPO_ROOT/db_bench
   ```

2. Install Python deps:

   ```bash
   cd python
   pip install -e .
   ```

## Running

Single experiment:

```bash
python -m sstable_experiments.exp1
```

All five (writes CSVs to `experiments/out/csv/` and PNGs to
`experiments/out/figs/`, same paths as the shell era):

```bash
python run_all.py
```

Stress / failure scenarios (Requirement §E):

```bash
python -m sstable_experiments.stress --scenario size        # §E.1
python -m sstable_experiments.stress --scenario skew        # §E.2
python -m sstable_experiments.stress --scenario assumption  # §E.4
# --scenario crash  → §E.3, blocked on pybind11 pass
```

## Library use

```python
from sstable_experiments import ExperimentConfig, run_experiment

cfg = ExperimentConfig(num_keys=500_000, block_restart_interval=16,
                       cache_size=64 << 20)
result = run_experiment(cfg)
print(result.ops_per_sec, result.stats["rocksdb.block.cache.hit"])
```

`SSTABLE_DB_BENCH=/path/to/db_bench` overrides the binary location.
