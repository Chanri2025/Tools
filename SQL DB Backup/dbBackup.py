#!/usr/bin/env python3
"""
backup_db_as_csv.py
Dump every table of a database to individual CSV files.

• Edit the `SOURCE_DB` string at the bottom.
• Run:  python backup_db_as_csv.py
"""

from pathlib import Path
from datetime import datetime

import pandas as pd
from sqlalchemy import create_engine, inspect
from tqdm import tqdm


def export_db_to_csv(source_uri: str, out_dir: Path) -> None:
    """Connect to `source_uri`, read all tables, write them to CSV."""
    engine = create_engine(source_uri)
    inspector = inspect(engine)
    tables = inspector.get_table_names()

    if not tables:
        print("⚠️  No tables found in the source database.")
        return

    print(f"📋  Found {len(tables)} table(s): {', '.join(tables)}")
    out_dir.mkdir(parents=True, exist_ok=True)

    for table in tqdm(tables, desc="Exporting", unit="table"):
        query = f'SELECT * FROM `{table}`'      # back-ticks guard weird names
        try:
            df = pd.read_sql(query, engine)
            csv_file = out_dir / f"{table}.csv"
            # UTF-8 with BOM → opens cleanly in Excel
            df.to_csv(csv_file, index=False, encoding="utf-8-sig")
            tqdm.write(f"   ✅  {csv_file.name}")
        except Exception as exc:
            tqdm.write(f"   ❌  Skipped {table}: {exc}")

    engine.dispose()
    print(f"🎉  Backup finished. Files are in: {out_dir.resolve()}")


if __name__ == "__main__":
    # ─── EDIT JUST THESE LINES ───────────────────────────────────────────────────
    SOURCE_DB = ("mysql+pymysql://DB-Admin:DBTest%40123@69.62.74.149:3306/swm")
    # ─────────────────────────────────────────────────────────────────────────────

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_folder = Path(f"db_backup_{timestamp}")

    export_db_to_csv(SOURCE_DB, backup_folder)