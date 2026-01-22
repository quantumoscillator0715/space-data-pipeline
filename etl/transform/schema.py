from __future__ import annotations
from pathlib import Path
import pandas as pd

EXPECTED_COLUMNS = [
    "planet_id",
    "planet_name",
    "star_name",
    "disc_year",
    "disc_method",
    "distance_ly",
    "radius_earth",
    "mass_earth",
    "orbital_period_days",
    "eq_temp_k",
    "notes",
]


DTYPE_MAP = {
    "planet_id": "Int64",          # nullable integer
    "planet_name": "string",
    "star_name": "string",
    "disc_year": "Int64",          # nullable integer
    "disc_method": "string",
    "distance_ly": "float64",
    "radius_earth": "float64",
    "mass_earth": "float64",
    "orbital_period_days": "float64",
    "eq_temp_k": "float64",
    "notes": "string",
}

def _standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    #Normalize, trip spaces, lower, replace spaces with underscores
    df = df.copy()
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
    )
    return df

def to_staging(raw_paths: List[Path], staging_dir: Path) -> list[Path]:
    staging_dir.mkdir(parents=True, exist_ok=True)
    
    out_paths: list[Path] = []

    for raw_path in raw_paths:
        df = pd.read_csv(raw_path)

        df = _standardize_columns(df)

        #ensure all expected columns exist (create missing as NA)
        for col in EXPECTED_COLUMNS:
            if col not in df.columns:
                df[col] = pd.NA

        #drop unexpected columns (keeps staging schemes stable)
        df = df[EXPECTED_COLUMNS]

        #coerce dtypes carefully:
        # - to_numeric with errors='coerce' turns bad strings into NaN
        numeric_cols = ["distance_ly", "radius_earth", "mass_earth", "orbital_period_days", "eq_temp_k"]
        for c in numeric_cols:
            df[c] = pd.to_numeric(df[c], errors="coerce")

        df["planet_id"] = pd.to_numeric(df["planet_id"], errors="coerce").astype("Int64")
        df["disc_year"] = pd.to_numeric(df["disc_year"], errors="coerce").astype("Int64")

        #strings - use pandas string dtype
        for c in ["planet_name", "star_name", "disc_method", "notes"]:
            df[c] = df[c].astype("string")

        #optional: basic "structural corruption" filter
        #drop rows where planet_name or star_name is missing AND everything numeric is missing
        key_missing = df["planet_name"].isna() | (df["planet_name"].str.len() == 0)
        all_numeric_missing = df[numeric_cols].isna().all(axis=1)
        df = df[~(key_missing & all_numeric_missing)].copy()

        out_path = staging_dir / "exoplanets_staging.csv"
        df.to_csv(out_path, index=False)

        out_paths.append(out_path)
    return out_paths