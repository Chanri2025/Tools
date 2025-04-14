import os
import pandas as pd
import mysql.connector
from tqdm import tqdm

# MySQL Database Configuration
db_config = {
    "host": "localhost",      # Change if using a remote server
    "user": "root",           # Change according to your MySQL credentials
    "password": "",           # Your MySQL password
    "database": "guvnl_dev"  # Replace with your database name
}

# Directory containing CSV files
csv_folder = "Backup"  # Make sure all your CSVs are inside this folder

# Connect to MySQL
try:
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()
    print("✅ Connected to MySQL Database")
except mysql.connector.Error as err:
    print(f"❌ Error: {err}")
    exit()

# Function to create table dynamically
def create_table_from_csv(file_path, table_name):
    df = pd.read_csv(file_path)  # Load CSV using Pandas
    columns = df.columns.tolist()

    # Generate SQL query for table creation
    create_query = f"CREATE TABLE IF NOT EXISTS `{table_name}` (id INT AUTO_INCREMENT PRIMARY KEY, "
    create_query += ", ".join([f"`{col}` TEXT" for col in columns])
    create_query += ");"

    cursor.execute(create_query)
    print(f"📌 Table `{table_name}` created successfully!")

# Function to insert CSV data into MySQL with tqdm progress
def insert_csv_data(file_path, table_name):
    df = pd.read_csv(file_path)

    # Convert NaN values to NULL
    df = df.where(pd.notna(df), None)

    total_rows = len(df)
    with tqdm(total=total_rows, desc=f"⏳ Inserting into `{table_name}`", unit=" rows") as pbar:
        for _, row in df.iterrows():
            placeholders = ", ".join(["%s"] * len(row))
            insert_query = f"INSERT INTO `{table_name}` ({', '.join(df.columns)}) VALUES ({placeholders})"
            cursor.execute(insert_query, tuple(row))
            pbar.update(1)

    conn.commit()
    print(f"✅ Data inserted into `{table_name}` successfully!")

# Loop through all CSV files in the folder with tqdm progress bar
csv_files = [f for f in os.listdir(csv_folder) if f.endswith(".csv")]

if not csv_files:
    print("⚠️ No CSV files found in the folder!")

for file in tqdm(csv_files, desc="🚀 Processing CSV Files", unit=" files"):
    file_path = os.path.join(csv_folder, file)
    table_name = os.path.splitext(file)[0]  # Table name = File name without extension

    print(f"\n📂 Processing file: {file}")
    create_table_from_csv(file_path, table_name)  # Create table if not exists
    insert_csv_data(file_path, table_name)  # Insert data

# Close MySQL connection
cursor.close()
conn.close()
print("\n🎉 Migration Completed Successfully!")