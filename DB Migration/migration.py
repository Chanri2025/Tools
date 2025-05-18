from sqlalchemy import create_engine, inspect
from pymongo import MongoClient
import pandas as pd
from tqdm import tqdm
from urllib.parse import urlparse


def migrate_mysql(source_uri, destination_uri):
    """Migrate all tables from one MySQL DB to another."""
    source_engine = create_engine(source_uri)
    dest_engine = create_engine(destination_uri)

    inspector = inspect(source_engine)
    tables = inspector.get_table_names()

    if not tables:
        print("⚠️  No tables found in the source MySQL database.")
        return

    print(f"📋  Found {len(tables)} MySQL table(s): {', '.join(tables)}")

    for table in tqdm(tables, desc="Migrating MySQL Tables", unit="table"):
        print(f"\nMigrating table: {table}")
        query = f"SELECT * FROM `{table}`"

        try:
            df = pd.read_sql(query, source_engine)
            with dest_engine.begin() as conn:
                df.to_sql(table, conn, if_exists='replace', index=False)
            print(f"   ✅ Table {table} migrated successfully.")
        except Exception as e:
            print(f"   ❌ Error migrating table {table}: {e}")
            continue

    print("🎉 MySQL Migration completed successfully!")


def migrate_mongodb(source_uri, destination_uri):
    """Migrate all collections from one MongoDB DB to another."""
    src_parsed = urlparse(source_uri)
    dst_parsed = urlparse(destination_uri)

    src_db_name = src_parsed.path.lstrip("/")
    dst_db_name = dst_parsed.path.lstrip("/")

    src_client = MongoClient(source_uri)
    dst_client = MongoClient(destination_uri)

    src_db = src_client[src_db_name]
    dst_db = dst_client[dst_db_name]

    collections = src_db.list_collection_names()

    if not collections:
        print("⚠️  No collections found in the source MongoDB database.")
        return

    print(f"📋  Found {len(collections)} MongoDB collection(s): {', '.join(collections)}")

    for collection_name in tqdm(collections, desc="Migrating MongoDB Collections", unit="collection"):
        try:
            documents = list(src_db[collection_name].find())
            if documents:
                dst_db[collection_name].delete_many({})  # Clear target collection before inserting
                dst_db[collection_name].insert_many(documents)
                tqdm.write(f"   ✅ Collection {collection_name} migrated successfully.")
            else:
                tqdm.write(f"   ⚠️  Collection {collection_name} is empty. Skipped.")
        except Exception as e:
            tqdm.write(f"   ❌ Error migrating collection {collection_name}: {e}")
            continue

    print("🎉 MongoDB Migration completed successfully!")


if __name__ == "__main__":
    # ─── EDIT YOUR CONNECTION STRINGS BELOW ─────────────────────────────────────
    # For MySQL:
    source_db = "mysql+pymysql://DB-Admin:DBTest%40123@69.62.74.149:3306/guvnldev"
    destination_db = "mysql+pymysql://root@localhost/guvnl_dev"
    
    # For MongoDB (Example):
    # source_db = "mongodb://localhost:27017/source_db"
    # destination_db = "mongodb://localhost:27017/destination_db"
    # ────────────────────────────────────────────────────────────────────────────

    if source_db.startswith("mysql") and destination_db.startswith("mysql"):
        migrate_mysql(source_db, destination_db)
    elif source_db.startswith("mongodb") and destination_db.startswith("mongodb"):
        migrate_mongodb(source_db, destination_db)
    else:
        print("❌ Unsupported or mismatched database types. Both URIs must be either MySQL or MongoDB.")