from __future__ import annotations
from pathlib import Path

def extract_raw(raw_dir: Path) -> list[Path]:
    """
    Extract step: place *unchanged* source data into data/raw.
    For now: return whatever CSVs already exist there.
    Laster: copy from source location, download from API, etc.
    """
    raw_dir.mkdir(parents=True, exist_ok=True)
    return sorted(raw_dir.glob("*.csv"))