# make_pred_from_actual_and_yoy.py
# Usage:
#   python3 make_pred_from_actual_and_yoy.py  WI.csv  --date TimeStamp --actual Actual
# If column names are unknown, omit the flags and the script will auto-detect.

import argparse
import pandas as pd
import numpy as np
from pandas.tseries.offsets import DateOffset
from pathlib import Path


def detect_cols(df):
    date_try = [
        "Timestamp",
        "TimeStamp",
        "Date",
        "date",
        "DATE",
        "datetime",
        "Datetime",
    ]
    actual_try = [
        "Actual",
        "actual",
        "Value",
        "Load",
        "Consumption",
        "kWh",
        "MW",
        "actual_value",
    ]
    date_col = next((c for c in date_try if c in df.columns), df.columns[0])
    actual_col = next(
        (c for c in actual_try if c in df.columns and c != date_col),
        next(
            (
                c
                for c in df.columns
                if c != date_col and pd.api.types.is_numeric_dtype(df[c])
            ),
            df.columns[1],
        ),
    )
    return date_col, actual_col


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("input_csv", help="Path to input CSV")
    ap.add_argument("--date", default=None, help="Date/time column name")
    ap.add_argument("--actual", default=None, help="Actual value column name")
    ap.add_argument(
        "--seed", type=int, default=2025, help="Random seed for reproducibility"
    )
    args = ap.parse_args()

    rng = np.random.default_rng(args.seed)

    # Load
    df = pd.read_csv(args.input_csv)
    date_col, actual_col = (args.date, args.actual)
    if date_col is None or actual_col is None:
        auto_date, auto_actual = detect_cols(df)
        date_col = date_col or auto_date
        actual_col = actual_col or auto_actual

    # Parse time and sort
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    df = df.sort_values(by=date_col).reset_index(drop=True)

    # Prepare 'pred'
    if "pred" not in df.columns:
        df["pred"] = np.nan

    # Build fast index: exact timestamp -> row index
    ts_to_idx = {ts: i for i, ts in enumerate(df[date_col])}

    pivot = pd.Timestamp("2024-04-01 00:00:00")

    # Walk forward chronologically so YoY anchors are already computed
    for i in range(len(df)):
        ts = df.at[i, date_col]
        if pd.isna(ts):
            continue

        # draw one epsilon in [-5%, +5%] per row
        eps = float(rng.uniform(-0.05, 0.05))

        if ts < pivot:
            # Use actual (non-zero historically) ±5%
            a = df.at[i, actual_col]
            if pd.isna(a):
                df.at[i, "pred"] = np.nan
            else:
                df.at[i, "pred"] = float(a) * (1.0 + eps)
        else:
            # On/after 2024-04-01: build from previous year's SAME timestamp pred
            anchor = ts - DateOffset(years=1)
            j = ts_to_idx.get(anchor)

            if j is not None and not pd.isna(df.at[j, "pred"]):
                base = df.at[j, "pred"]
                df.at[i, "pred"] = float(base) * (1.0 + eps)
            else:
                # Fallbacks (rare): if no anchor pred exists yet
                # 1) If actual is non-zero, use it; else 2) use previous row pred; else NaN
                a = df.at[i, actual_col]
                if pd.notna(a) and float(a) != 0.0:
                    df.at[i, "pred"] = float(a) * (1.0 + eps)
                elif i > 0 and pd.notna(df.at[i - 1, "pred"]):
                    df.at[i, "pred"] = float(df.at[i - 1, "pred"]) * (1.0 + eps)
                else:
                    df.at[i, "pred"] = np.nan

    # Save next to input
    inp = Path(args.input_csv)
    out_csv = inp.with_name(inp.stem + "_with_pred.csv")
    out_xlsx = inp.with_name(inp.stem + "_with_pred.xlsx")
    df.to_csv(out_csv, index=False)
    try:
        df.to_excel(out_xlsx, index=False)
    except Exception:
        pass  # Excel optional

    print(f"Detected columns -> date: {date_col}  | actual: {actual_col}")
    print(f"Written: {out_csv}")
    if out_xlsx.exists():
        print(f"Written: {out_xlsx}")


if __name__ == "__main__":
    main()
