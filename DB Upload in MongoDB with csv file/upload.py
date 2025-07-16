from pymongo import MongoClient
import pandas as pd
from tqdm import tqdm
import os
import requests
from datetime import datetime
import argparse


def migrate_csv_to_mongodb(csv_path, mongo_uri, db_name, collection_name, delimiter=';'):
    """
    Migrate banking data from CSV file to MongoDB collection.
    
    Args:
        csv_path: Path to the CSV file or URL
        mongo_uri: MongoDB connection URI
        db_name: MongoDB database name
        collection_name: MongoDB collection name
        delimiter: CSV delimiter character
    """
    # Connect to MongoDB
    client = MongoClient(mongo_uri)
    db = client[db_name]
    collection = db[collection_name]
    
    # Load CSV data
    if csv_path.lower().startswith(('http://', 'https://')):
        print(f"🌐 Downloading CSV from {csv_path}...")
        resp = requests.get(csv_path)
        resp.raise_for_status()
        df = pd.read_csv(pd.io.common.StringIO(resp.text), delimiter=delimiter, quotechar='"')
    else:
        print(f"📂 Reading CSV file from {csv_path}...")
        df = pd.read_csv(csv_path, delimiter=delimiter, quotechar='"')
    
    # Clean column names (remove quotes if present)
    df.columns = [col.strip('"') for col in df.columns]
    
    # Convert TimeStamp to datetime
    print("🕒 Converting timestamps...")
    df['TimeStamp'] = pd.to_datetime(df['TimeStamp'])
    
    # Convert numeric columns to appropriate types
    numeric_columns = [
        'Injection_Electricity', 'Total_Consumption', 'Net_Injection',
        'Banking_Unit', 'Banking_Cumulative', 'Adjusted_Unit',
        'MOD_Price', 'Banking_Charges', 'Adjustment_Charges'
    ]
    
    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Convert DataFrame to list of dictionaries
    print("🔄 Preparing data for MongoDB...")
    records = df.to_dict('records')
    
    # Check if collection exists and has data
    existing_count = collection.count_documents({})
    if existing_count > 0:
        print(f"⚠️  Collection '{collection_name}' already contains {existing_count} documents.")
        choice = input("Do you want to (a)ppend, (r)eplace, or (c)ancel? [a/r/c]: ").lower()
        
        if choice == 'c':
            print("❌ Operation cancelled.")
            return
        elif choice == 'r':
            print(f"🗑️  Dropping existing collection '{collection_name}'...")
            collection.drop()
    
    # Insert data into MongoDB
    print(f"📥 Inserting {len(records)} records into {db_name}.{collection_name}...")
    
    # Process in batches for better performance
    batch_size = 1000
    for i in tqdm(range(0, len(records), batch_size), desc="Inserting batches", unit="batch"):
        batch = records[i:i+batch_size]
        
        # Upsert records (update if exists, insert if not)
        for record in batch:
            collection.update_one(
                {"TimeStamp": record["TimeStamp"]},
                {"$set": record},
                upsert=True
            )
    
    # Create index on TimeStamp for better query performance
    print("🔍 Creating index on TimeStamp field...")
    collection.create_index("TimeStamp")
    
    # Verify the import
    final_count = collection.count_documents({})
    print(f"✅ Import completed! Collection now contains {final_count} documents.")
    
    # Close MongoDB connection
    client.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Import banking data from CSV to MongoDB')
    parser.add_argument('--source', required=True, help='Path to CSV file or URL')
    parser.add_argument('--mongo-uri', required=True, help='MongoDB connection URI')
    parser.add_argument('--db', default='powercasting', help='MongoDB database name')
    parser.add_argument('--collection', default='Banking_Data', help='MongoDB collection name')
    parser.add_argument('--delimiter', default=';', help='CSV delimiter character')
    
    args = parser.parse_args()
    
    migrate_csv_to_mongodb(
        csv_path=args.source,
        mongo_uri=args.mongo_uri,
        db_name=args.db,
        collection_name=args.collection,
        delimiter=args.delimiter
    )