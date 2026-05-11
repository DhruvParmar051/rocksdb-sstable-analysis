"""Parse db_bench stdout: per-benchmark ops/sec and rocksdb.* counters."""
from __future__ import annotations

import re
from typing import Dict, Optional

# Example line: "fillrandom   :       3.421 micros/op 292268 ops/sec ..."
_BENCH_LINE = re.compile(
    r"^(?P<bench>\w+)\s*:\s*"
    r"(?P<micros>[\d.]+)\s*micros/op\s+"
    r"(?P<ops>\d+)\s*ops/sec"
)

# Example: "rocksdb.block.cache.hit COUNT : 12345"
_STAT_LINE = re.compile(r"^(?P<key>rocksdb\.\S+)\s+COUNT\s*:\s*(?P<val>\d+)")


def parse_bench_ops(stdout: str, bench_name: str) -> Optional[int]:
    """Return ops/sec for the first matching benchmark line, or None."""
    for line in stdout.splitlines():
        m = _BENCH_LINE.match(line.strip())
        if m and m.group("bench") == bench_name:
            return int(m.group("ops"))
    return None


def parse_statistics(stdout: str) -> Dict[str, int]:
    """Return {counter_name: value} for every rocksdb.* COUNT line present."""
    stats: Dict[str, int] = {}
    for line in stdout.splitlines():
        m = _STAT_LINE.match(line.strip())
        if m:
            stats[m.group("key")] = int(m.group("val"))
    return stats


def cache_hit_ratio(stats: Dict[str, int]) -> Optional[float]:
    hits = stats.get("rocksdb.block.cache.hit", 0)
    misses = stats.get("rocksdb.block.cache.miss", 0)
    total = hits + misses
    return hits / total if total > 0 else None


def total_bytes_written(stats: Dict[str, int]) -> int:
    return (
        stats.get("rocksdb.compact.write.bytes", 0)
        + stats.get("rocksdb.flush.write.bytes", 0)
    )
