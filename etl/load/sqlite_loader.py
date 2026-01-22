from __future__ import annotations

import sqlite3
from pathlib import Path
import pandas as pd

def load_csv_to_sqlite(csv_path: Path, db_path: Path, table_name: str = "exoplanets") -> int:
    db_path.parent.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(csv_path)

    with sqlite3.connect(db_path) as conn:
        #replace for now
        df.to_sql(table_name, conn, if_exists="replace", index=False)

        #simple validation
        cur = conn.execute(f"SELECT COUNT(*) FROM {table_name}")
        (n_rows,) = cur.fetchone()

    return int(n_rows)