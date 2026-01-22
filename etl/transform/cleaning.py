from __future__ import annotations
from pathlib import Path
import numpy as np
import pandas as pd

EARTH_DENSITY_G_CM3 = 5.51
#domain-informed but not 'physics perfect' boundaries for sanity checks
MAX_DENSITY_G_CM3 = 30.0 #way too high for a normal planet
MIN_RADIUS_EARTH = 0.1 #dwarf planet territory
MAX_RADIUS_EARTH = 25.0 #current maximum planetery radius is 2 times the Jupiter

METHOD_MAP = {
    "RV": "Radial Velocity",
    "Radial": "Radial Velocity",
    "Radial Velocity": "Radial Velocity",
    "Transit": "Transit",
}


def _normalize_method(s: pd.Series) -> pd.Series:
    s = s.astype("string").str.strip()
    return s.replace(METHOD_MAP)


def _size_class(radius: pd.Series) -> pd.Series:
    """
    Simple bins in Earth radii.
    Adjust as needed, but keep deterministic.
    """
    bins = [0, 0.8, 1.25, 2.0, np.inf]
    labels = ["Sub-Earth", "Earth-Like", "Super-Earth", "Giant"]
    return pd.cut(radius, bins=bins, labels=labels, include_lowest=True)


def to_curated(staging_paths: list[Path], curated_dir: Path) -> list[Path]:
    curated_dir.mkdir(parents=True, exist_ok=True)
    out_paths: list[Path] = []

    for staging_path in staging_paths:
        df = pd.read_csv(staging_path)

        # 1) normalize categorical text
        df["disc_method"] = _normalize_method(df["disc_method"])

        # 2) enforce numeric types again (just in case)
        numeric_cols = ["distance_ly", "radius_earth", "mass_earth", "orbital_period_days", "eq_temp_k"]
        for c in numeric_cols:
            df[c] = pd.to_numeric(df[c], errors="coerce")

        df["disc_year"] = pd.to_numeric(df["disc_year"], errors="coerce").astype("Int64")
        df["planet_id"] = pd.to_numeric(df["planet_id"], errors="coerce").astype("Int64")

        # 3) basic validity rules
        #set negative or zero radii to NaN
        bad_radius = (df["radius_earth"] < MIN_RADIUS_EARTH) | (df["radius_earth"] > MAX_RADIUS_EARTH)
        df.loc[bad_radius, "radius_earth"] = np.nan

        #masses should be > 0 if available
        bad_mass = (df["mass_earth"] <= 0)
        df.loc[bad_mass, "mass_earth"] = np.nan

        #discovery year cannot exceed today
        this_year = pd.Timestamp.today().year
        df.loc[df["disc_year"] > this_year, "disc_year"] = pd.NA

        # 4) derived features
        #density relative to Earth
        df["density_rel_earth"] = df["mass_earth"] / (df["radius_earth"]**3)
        df["density_g_cm3"] = df["density_rel_earth"] * EARTH_DENSITY_G_CM3

        #density sanity cap: mark absurd results as missing
        df.loc[df["density_g_cm3"] > MAX_DENSITY_G_CM3, ["density_rel_earth", "density_g_cm3"]] = np.nan

        #size class via pd.cut
        df["class_size"] = _size_class(df["radius_earth"])

        # 5) write curated output
        out_path = curated_dir / "exoplanets_curated.csv"
        df.to_csv(out_path, index=False)

        out_paths.append(out_path)
        
    return out_paths