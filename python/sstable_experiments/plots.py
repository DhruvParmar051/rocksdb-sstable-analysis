"""matplotlib helpers — one function per experiment, writes a PNG to FIG_DIR.

Plots are deliberately minimal (axes + line, no styling), so they reproduce
deterministically and look the same on every machine.
"""
from __future__ import annotations

import pathlib
from typing import Optional

import matplotlib
matplotlib.use("Agg")  # headless
import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd  # noqa: E402

from .paths import FIG_DIR, ensure_dirs


def _save(fig, name: str) -> pathlib.Path:
    ensure_dirs()
    out = FIG_DIR / name
    fig.savefig(out, dpi=150, bbox_inches="tight")
    plt.close(fig)
    return out


def _outside_legend(ax, **kwargs):
    """Place legend below the axes, outside the plot area."""
    ax.legend(loc="upper center", bbox_to_anchor=(0.5, -0.18),
              ncol=3, frameon=True, **kwargs)


def restart_interval(df: pd.DataFrame, name: str = "exp1_restart_interval.png"):
    fig, ax1 = plt.subplots(figsize=(7, 5))
    ax2 = ax1.twinx()
    l1, = ax1.plot(df["interval"], df["read_ops_sec"], "o-", label="read ops/s", color="C0")
    l2, = ax1.plot(df["interval"], df["write_ops_sec"], "s-", label="write ops/s", color="C1")
    l3, = ax2.plot(df["interval"], df["file_size_mb"], "^--", label="SST MB", color="C2")
    ax1.set_xlabel("block_restart_interval")
    ax1.set_ylabel("ops/sec")
    ax2.set_ylabel("SST size (MB)")
    ax1.set_title("Exp 1: Restart Interval — throughput vs file size")
    ax1.legend(handles=[l1, l2, l3], loc="upper center",
               bbox_to_anchor=(0.5, -0.18), ncol=3, frameon=True)
    fig.subplots_adjust(bottom=0.25)
    return _save(fig, name)


def bloom_fpr(df: pd.DataFrame, name: str = "exp2_bloom_fpr.png"):
    fig, ax = plt.subplots(figsize=(7, 5))
    ax.plot(df["bloom_bits"], df["readrandom_ops_sec"], "o-", label="readrandom (hits)")
    ax.plot(df["bloom_bits"], df["readmissing_ops_sec"], "s-", label="readmissing (absent)")
    ax.set_xlabel("bloom bits/key")
    ax.set_ylabel("ops/sec")
    ax.set_title("Exp 2: Bloom Filter FPR vs bits/key")
    _outside_legend(ax)
    fig.subplots_adjust(bottom=0.25)
    return _save(fig, name)


def block_size_amp(df: pd.DataFrame, name: str = "exp3_block_size_amp.png"):
    fig, ax1 = plt.subplots(figsize=(7, 5))
    ax2 = ax1.twinx()
    l1, = ax1.plot(df["block_size_bytes"], df["read_ops_sec"], "o-", label="read ops/s", color="C0")
    l2, = ax1.plot(df["block_size_bytes"], df["write_ops_sec"], "s-", label="write ops/s", color="C1")
    l3, = ax2.plot(df["block_size_bytes"], df["file_size_mb"], "^--", label="SST MB", color="C2")
    ax1.set_xscale("log", base=2)
    ax1.set_xlabel("block_size (bytes, log2)")
    ax1.set_ylabel("ops/sec"); ax2.set_ylabel("SST size (MB)")
    ax1.set_title("Exp 3: Block Size — read vs write amplification")
    ax1.legend(handles=[l1, l2, l3], loc="upper center",
               bbox_to_anchor=(0.5, -0.18), ncol=3, frameon=True)
    fig.subplots_adjust(bottom=0.25)
    return _save(fig, name)


def cache_zipfian(df: pd.DataFrame, name: str = "exp4_cache_zipfian.png"):
    fig, (axL, axR) = plt.subplots(1, 2, figsize=(11, 5))
    for dist, marker in (("uniform", "o"), ("zipfian", "s")):
        sub = df[df["distribution"] == dist].sort_values("cache_mb")
        axL.plot(sub["cache_mb"], sub["ops_sec"], marker=marker, label=dist)
        axR.plot(sub["cache_mb"], sub["cache_hit_ratio"], marker=marker, label=dist)
    for ax in (axL, axR):
        ax.set_xscale("log", base=2)
        ax.set_xlabel("cache size (MB, log2)")
        ax.legend(loc="upper center", bbox_to_anchor=(0.5, -0.18),
                  ncol=2, frameon=True)
    axL.set_ylabel("ops/sec"); axL.set_title("Throughput")
    axR.set_ylabel("cache hit ratio"); axR.set_title("Cache hit ratio")
    fig.subplots_adjust(bottom=0.25)
    return _save(fig, name)


def write_amplification(df: pd.DataFrame, name: str = "exp5_write_amplification.png"):
    fig, ax1 = plt.subplots(figsize=(7, 5))
    ax2 = ax1.twinx()
    l1, = ax1.plot(df["num_keys"], df["bytes_written_mb"], "o-", label="bytes written (MB)", color="C0")
    l2, = ax1.plot(df["num_keys"], df["user_data_mb"], "s--", label="user data (MB)", color="C1")
    l3, = ax2.plot(df["num_keys"], df["write_amp"], "^-", label="write amp ×", color="C3")
    ax1.set_xscale("log"); ax1.set_xlabel("num_keys (log)")
    ax1.set_ylabel("MB"); ax2.set_ylabel("write amplification (×)")
    ax1.set_title("Exp 5: LSM write amplification vs dataset size")
    ax1.legend(handles=[l1, l2, l3], loc="upper center",
               bbox_to_anchor=(0.5, -0.18), ncol=3, frameon=True)
    fig.subplots_adjust(bottom=0.25)
    return _save(fig, name)



