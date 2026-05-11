"""Shared output paths so every script writes alongside the shell-era CSVs."""
from __future__ import annotations

import pathlib

REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
OUT_DIR = REPO_ROOT / "experiments" / "out"
CSV_DIR = OUT_DIR / "csv"
FIG_DIR = OUT_DIR / "figs"


def ensure_dirs() -> None:
    CSV_DIR.mkdir(parents=True, exist_ok=True)
    FIG_DIR.mkdir(parents=True, exist_ok=True)
