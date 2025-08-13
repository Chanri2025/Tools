# mustrunconsumption.py
import sys
import pandas as pd
import mysql.connector
from pymongo import MongoClient, ASCENDING
from tqdm import tqdm

# ─── CONFIG ───────────────────────────────────────────────────────────
MYSQL = {
    "host": "localhost",
    "user": "root",
    "password": "",
    "database": "guvnl",
    "port": 3306,
}
PLANTS_META_TABLE = "plant_details"  # meta table containing Code, Type, etc.
MONGO_URI = "mongodb://localhost:27017/"
MONGO_DB = "powercasting"
MONGO_COL = "mustrunplantconsumption"

UPSERT = True  # upsert by (TimeStamp, Plant_Name)
BATCH_INSERT = 1000  # not used for upserts; used if UPSERT=False
VERBOSE_SKIPS = True  # print reason when a plant is skipped

# Plant codes to skip
SKIP_CODES = ["WI"]  # add more codes here if needed
# ──────────────────────────────────────────────────────────────────────

# Candidate names (first match wins). All checks are case-insensitive.
TS_CANDIDATES = ["TimeStamp", "Timestamp", "time_stamp", "ts", "time"]
ACT_CANDIDATES = ["Actual", "Actual_Value", "actual", "value", "act"]
PRED_CANDIDATES = ["Pred", "Predicted", "Prediction", "Pred_Value", "pred", "forecast"]


def _get_columns(conn, table: str) -> list[str]:
    cur = conn.cursor()
    cur.execute(f"SHOW COLUMNS FROM `{table}`;")
    cols = [r[0] for r in cur.fetchall()]
    cur.close()
    return cols


def _pick(colnames: list[str], candidates: list[str]) -> str | None:
    lower = {c.lower(): c for c in colnames}
    for cand in candidates:
        if cand in colnames:
            return cand
        if cand.lower() in lower:
            return lower[cand.lower()]
    return None


def fetch_must_run_codes(conn) -> list[str]:
    q = f"SELECT `Code` FROM `{PLANTS_META_TABLE}` WHERE `Type`='Must run';"
    cur = conn.cursor()
    cur.execute(q)
    codes = [r[0] for r in cur.fetchall()]
    cur.close()
    return codes


def fetch_plant_df(conn, table: str) -> pd.DataFrame:
    cols = _get_columns(conn, table)
    ts = _pick(cols, TS_CANDIDATES)
    act = _pick(cols, ACT_CANDIDATES)
    pred = _pick(cols, PRED_CANDIDATES)

    missing = []
    if ts is None:
        missing.append("TimeStamp")
    if act is None:
        missing.append("Actual")
    if pred is None:
        missing.append("Pred/Predicted")

    if missing:
        raise RuntimeError(f"missing columns: {', '.join(missing)} | available={cols}")

    # Build a select that aliases to canonical names
    q = f"SELECT `{ts}` AS TimeStamp, `{act}` AS Actual, `{pred}` AS Pred FROM `{table}`;"
    df = pd.read_sql(q, conn)

    # Normalize dtypes
    if not pd.api.types.is_datetime64_any_dtype(df["TimeStamp"]):
        df["TimeStamp"] = pd.to_datetime(df["TimeStamp"], errors="coerce")
    df["Actual"] = pd.to_numeric(df["Actual"], errors="coerce")
    df["Pred"] = pd.to_numeric(df["Pred"], errors="coerce")

    df = df.dropna(subset=["TimeStamp"])
    return df


def main():
    # Connect MySQL
    try:
        sql_conn = mysql.connector.connect(**MYSQL)
    except mysql.connector.Error as e:
        print(f"❌ MySQL connection failed: {e}")
        sys.exit(1)

    # Connect Mongo
    try:
        mongo = MongoClient(MONGO_URI)
        mdb = mongo[MONGO_DB]
        col = mdb[MONGO_COL]
    except Exception as e:
        print(f"❌ MongoDB connection failed: {e}")
        sys.exit(1)

    # Ensure unique index for upserts
    if UPSERT:
        col.create_index(
            [("TimeStamp", ASCENDING), ("Plant_Name", ASCENDING)], unique=True
        )

    print("🔍 Fetching must run plant codes...")
    codes = fetch_must_run_codes(sql_conn)

    # Skip unwanted codes
    codes = [c for c in codes if c not in SKIP_CODES]
    print(f"✅ Found {len(codes)} must run plants (excluding {SKIP_CODES}).")

    total_written = 0
    skipped = 0
    for code in tqdm(codes, desc="Processing plants"):
        try:
            df = fetch_plant_df(sql_conn, code)
            if df.empty:
                skipped += 1
                if VERBOSE_SKIPS:
                    print(f"\n⚠️  '{code}' has no rows after filtering; skipping.")
                continue

            df["Plant_Name"] = code
            records = df.to_dict("records")

            if UPSERT:
                for r in records:
                    col.update_one(
                        {"TimeStamp": r["TimeStamp"], "Plant_Name": r["Plant_Name"]},
                        {"$set": r},
                        upsert=True,
                    )
                total_written += len(records)
            else:
                for i in range(0, len(records), BATCH_INSERT):
                    col.insert_many(records[i : i + BATCH_INSERT])
                total_written += len(records)

        except Exception as e:
            skipped += 1
            if VERBOSE_SKIPS:
                print(f"\n⚠️  Skipping '{code}': {e}")

    print(f"\n✅ Done. Upserted/Inserted: {total_written} | Skipped plants: {skipped}")

    sql_conn.close()
    mongo.close()


if __name__ == "__main__":
    main()
