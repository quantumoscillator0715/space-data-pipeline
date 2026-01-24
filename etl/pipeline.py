import logging
from __future__ import annotations

from pathlib import Path
from datetime import datetime

from etl.extract import extract_raw
from etl.transform.schema import to_staging
from etl.transform.cleaning import to_curated
from etl.load.sqlite_loader import load_csv_to_sqlite


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
STAGING_DIR = DATA_DIR / "staging"
CURATED_DIR = DATA_DIR / "curated"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)

def run_pipeline() -> None:
    """
    End-to-end ETL pipeline (idempotent):
    - Extract: write raw files (unchanged)
    - Transform (schema): produce staging dataset(s)
    - Transform (semantic): produce curated dataset(s)
    - Load: write final deliverables (CSV/DB)
    """
    # Ensure directories exist (safe to re-run)
    for d in (RAW_DIR, STAGING_DIR, CURATED_DIR):
        d.mkdir(parents=True, exist_ok=True)

    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    logger.info(f"[ETL] Starting run_id={run_id}")
    logger.info(f"[ETL] raw={RAW_DIR}")
    logger.info(f"[ETL] staging={STAGING_DIR}")
    logger.info(f"[ETL] curated={CURATED_DIR}")

    # 1) EXTRACT
    raw_paths = extract_raw(raw_dir=RAW_DIR)
    logger.info(f"[ETL] Extracted: {raw_paths}")

    # 2) TRANSFORM: schema (staging)
    staging_paths = to_staging(raw_paths=raw_paths, staging_dir=STAGING_DIR)
    logger.info(f"[ETL] Staging: {staging_paths}")

    # 3) TRANSFORM: semantic (curated)
    curated_paths = to_curated(staging_paths=staging_paths, curated_dir=CURATED_DIR)
    logger.info(f"[ETL] Curated: {curated_paths}")

    # 4) LOAD
    DB_DIR = DATA_DIR / "db"
    DB_PATH = DB_DIR / "exoplanets.sqlite"

    if curated_paths:
        n_rows = load_csv_to_sqlite(
            csv_path=curated_paths[0],
            db_path=DB_PATH,
            table_name="exoplanets"
        )
        print(f"[LOAD] Loaded {n_rows} rows into {DB_PATH}")
    else:
        print("[LOAD] No curated files to load yet.")

    logger.info(f"[ETL] Done run_id={run_id}")


if __name__ == "__main__":
    run_pipeline()