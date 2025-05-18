#!/usr/bin/env python3
"""
backup_db_as_csv.py
Dump every table/collection of a database to individual CSV files.

• Edit the `SOURCE_DB` string at the bottom.
• Run:  python backup_db_as_csv.py
"""

from pathlib import Path
from datetime import datetime
from urllib.parse import urlparse

import pandas as pd
from sqlalchemy import create_engine, inspect
from tqdm import tqdm

try:
    from pymongo import MongoClient
except ImportError:
    print("⚠️  Missing pymongo. Run: pip install pymongo")

def export_mysql_to_csv(source_uri: str, out_dir: Path) -> None:
    """Backup MySQL database tables to CSV."""
    engine = create_engine(source_uri)
    inspector = inspect(engine)
    tables = inspector.get_table_names()

    if not tables:
        print("⚠️  No tables found in the source database.")
        return

    print(f"📋  Found {len(tables)} MySQL table(s): {', '.join(tables)}")
    out_dir.mkdir(parents=True, exist_ok=True)

    for table in tqdm(tables, desc="Exporting MySQL", unit="table"):
        query = f'SELECT * FROM `{table}`'
        try:
            df = pd.read_sql(query, engine)
            csv_file = out_dir / f"{table}.csv"
            df.to_csv(csv_file, index=False, encoding="utf-8-sig")
            tqdm.write(f"   ✅  {csv_file.name}")
        except Exception as exc:
            tqdm.write(f"   ❌  Skipped {table}: {exc}")

    engine.dispose()
    print(f"🎉  MySQL Backup finished. Files are in: {out_dir.resolve()}")


def export_mongo_to_csv(source_uri: str, out_dir: Path) -> None:
    """Backup MongoDB collections to CSV."""
    parsed = urlparse(source_uri)
    db_name = parsed.path.lstrip("/")
    client = MongoClient(source_uri)
    db = client[db_name]
    collections = db.list_collection_names()

    if not collections:
        print("⚠️  No collections found in the MongoDB database.")
        return

    print(f"📋  Found {len(collections)} MongoDB collection(s): {', '.join(collections)}")
    out_dir.mkdir(parents=True, exist_ok=True)

    for collection_name in tqdm(collections, desc="Exporting MongoDB", unit="collection"):
        try:
            collection = db[collection_name]
            documents = list(collection.find())
            if documents:
                df = pd.DataFrame(documents)
                # Drop MongoDB's internal ID if not needed
                if '_id' in df.columns:
                    df.drop(columns=['_id'], inplace=True)
                csv_file = out_dir / f"{collection_name}.csv"
                df.to_csv(csv_file, index=False, encoding="utf-8-sig")
                tqdm.write(f"   ✅  {csv_file.name}")
            else:
                tqdm.write(f"   ⚠️  {collection_name} is empty. Skipped.")
        except Exception as exc:
            tqdm.write(f"   ❌  Skipped {collection_name}: {exc}")

    client.close()
    print(f"🎉  MongoDB Backup finished. Files are in: {out_dir.resolve()}")


if __name__ == "__main__":
    # ─── EDIT JUST THESE LINES ───────────────────────────────────────────────────
    # MySQL Example: "mysql+pymysql://user:password@host:port/database"
    # MongoDB Example: "mongodb://user:password@host:port/database"
    SOURCE_DB = "mongodb://swm_user:Swm123@69.62.74.149:27017/swm?authSource=swm"
    # ─────────────────────────────────────────────────────────────────────────────

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_folder = Path(f"db_backup_{timestamp}")

    if SOURCE_DB.startswith("mysql"):
        export_mysql_to_csv(SOURCE_DB, backup_folder)
    elif SOURCE_DB.startswith("mongodb"):
        export_mongo_to_csv(SOURCE_DB, backup_folder)
    else:
        print("❌ Unsupported database type. Please provide a valid MySQL or MongoDB URI.")