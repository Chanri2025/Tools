# wi_excel_to_mongo.py
import sys
import pandas as pd
from pymongo import MongoClient, ASCENDING, UpdateOne

# ==== CONFIG ====
EXCEL_PATH = r"C:\Users\hp\Desktop\Code\Tools\DB Upload in MongoDB with csv file\WI_with_pred.xlsx"  # <-- change to your file path
SHEET_NAME = 0  # or "Sheet1"

MONGO_URI = "mongodb://localhost:27017/"
MONGO_DB = "powercasting"
MONGO_COL = "mustrunplantconsumption"

PLANT_NAME = "WI"
BULK_BATCH = 2000
# =================

TS_CANDIDATES = ["TimeStamp", "Timestamp", "Date", "date"]
ACT_CANDIDATES = ["Actual", "actual", "Actual_Value"]
PRED_CANDIDATES = ["Pred", "Predicted", "Prediction", "pred", "forecast", "Pred_Value"]


def pick(cols, candidates):
    lower = {c.lower(): c for c in cols}
    for cand in candidates:
        if cand in cols:
            return cand
        if cand.lower() in lower:
            return lower[cand.lower()]
    return None


def main():
    # Read Excel
    try:
        df = pd.read_excel(EXCEL_PATH, sheet_name=SHEET_NAME)
    except Exception as e:
        print(f"❌ Failed to read Excel: {e}")
        sys.exit(1)

    if df.empty:
        print("⚠️ Excel has no rows.")
        return

    # Pick columns
    cols = list(df.columns)
    ts_col = pick(cols, TS_CANDIDATES)
    act_col = pick(cols, ACT_CANDIDATES)
    pred_col = pick(cols, PRED_CANDIDATES)

    if not all([ts_col, act_col, pred_col]):
        print(f"❌ Missing required columns. Found: {cols}")
        return

    df = df[[ts_col, act_col, pred_col]].copy()
    df.columns = ["TimeStamp", "Actual", "Pred"]

    # Parse timestamps and keep as full datetime
    df["TimeStamp"] = pd.to_datetime(df["TimeStamp"], errors="coerce", dayfirst=False)
    df = df.dropna(subset=["TimeStamp"])  # remove invalid timestamps

    # Convert to Python datetime (so Mongo stores it as ISODate, not string)
    df["TimeStamp"] = df["TimeStamp"].dt.to_pydatetime()

    # Ensure numeric values
    df["Actual"] = pd.to_numeric(df["Actual"], errors="coerce")
    df["Pred"] = pd.to_numeric(df["Pred"], errors="coerce")

    # Add Plant_Name
    df["Plant_Name"] = PLANT_NAME

    # Connect to MongoDB
    client = MongoClient(MONGO_URI)
    col = client[MONGO_DB][MONGO_COL]
    col.create_index([("TimeStamp", ASCENDING), ("Plant_Name", ASCENDING)], unique=True)

    # Bulk upsert
    records = df.to_dict("records")
    ops = []
    for r in records:
        ops.append(
            UpdateOne(
                {"TimeStamp": r["TimeStamp"], "Plant_Name": r["Plant_Name"]},
                {"$set": r},
                upsert=True,
            )
        )
        if len(ops) >= BULK_BATCH:
            col.bulk_write(ops, ordered=False)
            ops = []
    if ops:
        col.bulk_write(ops, ordered=False)

    print(
        f"✅ Done. Upserted {len(records)} docs into {MONGO_DB}.{MONGO_COL} for Plant_Name='{PLANT_NAME}'."
    )


if __name__ == "__main__":
    main()
