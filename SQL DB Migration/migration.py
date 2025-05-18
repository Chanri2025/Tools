from sqlalchemy import create_engine, inspect
import pandas as pd
from tqdm import tqdm

def migrate_db(source_uri, destination_uri):
    # Create database connections
    source_engine = create_engine(source_uri)
    dest_engine = create_engine(destination_uri)

    # Inspect source database
    inspector = inspect(source_engine)
    tables = inspector.get_table_names()

    if not tables:
        print("No tables found in the source database.")
        return

    print(f"Found tables: {tables}")

    # Migrate each table with a progress bar
    for table in tqdm(tables, desc="Migrating tables", unit="table"):
        print(f"\nMigrating table: {table}")
        query = f"SELECT * FROM {table}"

        try:
            df = pd.read_sql(query, source_engine)

            with dest_engine.begin() as conn:  # Explicitly handle transactions
                df.to_sql(table, conn, if_exists='replace', index=False)
                print(f"Table {table} migrated successfully.")

        except Exception as e:
            print(f"Error migrating table {table}: {e}")
            continue  # Skip problematic tables and proceed with migration

    print("Migration completed successfully!")

if __name__ == "__main__":
    # Example database URIs (modify as needed)
    # source_db = "mysql+pymysql://u449273699_ProjectTeam:Smsc%402024@193.203.184.150:3306/u449273699_project_status"
    
    source_db = "mysql+pymysql://DB-Admin:DBTest%40123@69.62.74.149:3306/guvnldev"
    destination_db = "mysql+pymysql://root@localhost/guvnl_dev"
    # destination_db = "mysql+pymysql://DB-Admin:DBTest%40123@69.62.74.149:3306/guvnldev"

    migrate_db(source_db, destination_db)