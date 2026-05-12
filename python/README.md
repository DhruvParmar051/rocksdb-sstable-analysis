# sstable_experiments — Python-driven RocksDB SSTable experiments

This package wraps the `db_bench` binary via subprocess and provides
a clean Python API for running RocksDB SSTable experiments, parsing results,
and generating figures.

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
python -m sstable_experiments.stress --scenario crash       # §E.3
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
