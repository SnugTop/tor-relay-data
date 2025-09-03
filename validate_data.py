#!/usr/bin/env python3
import argparse
import sys
import hashlib
import datetime as dt
import pandas as pd

def die(msg, code=2):
    print(f"VALIDATION ERROR: {msg}", file=sys.stderr)
    sys.exit(code)

def parse_args():
    p = argparse.ArgumentParser(description="Validate tor daily bandwidth panel CSV (inferred dates).")
    p.add_argument("csv", help="Path to daily_bw.csv")
    return p.parse_args()

def daterange(d0, d1):
    days, d = [], d0
    while d <= d1:
        days.append(d)
        d += dt.timedelta(days=1)
    return days

def main():
    args = parse_args()

    # Hash (for reproducibility in logs)
    with open(args.csv, "rb") as fh:
        digest = hashlib.sha256(fh.read()).hexdigest()

    # Load + schema
    try:
        df = pd.read_csv(args.csv, dtype={"fingerprint": "string"})
    except Exception as e:
        die(f"Failed to read CSV: {e}")

    required = ["date", "fingerprint", "relay_bandwidth", "timestamp"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        die(f"Missing required columns: {missing}")

    # Types & basic sanity
    try:
        df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d", errors="raise").dt.date
    except Exception as e:
        die(f"Bad 'date' values: {e}")

    try:
        pd.to_datetime(df["timestamp"], errors="raise")
    except Exception as e:
        die(f"Bad 'timestamp' values: {e}")

    df["relay_bandwidth"] = pd.to_numeric(df["relay_bandwidth"], errors="coerce")
    if df["relay_bandwidth"].isna().any():
        die(f"'relay_bandwidth' has {int(df['relay_bandwidth'].isna().sum())} non-numeric cells")
    if (df["relay_bandwidth"] < 0).any():
        die(f"'relay_bandwidth' has {int((df['relay_bandwidth'] < 0).sum())} negative values")

    if df.isna().any().any():
        die("Found NA/null values in the dataset")

    # No duplicate (date, fingerprint)
    dup = int(df.duplicated(subset=["date", "fingerprint"]).sum())
    if dup:
        die(f"Found {dup} duplicate (date,fingerprint) rows")

    # Date coverage: no gaps
    days_present = sorted(set(df["date"].tolist()))
    d0, d1 = min(days_present), max(days_present)
    expected_days = daterange(d0, d1)
    if set(days_present) != set(expected_days):
        missing_days = [d for d in expected_days if d not in days_present]
        die(f"Date coverage has gaps: missing {len(missing_days)} day(s); first few: {', '.join(map(str, missing_days[:10]))}")

    # Fingerprint set must be identical every day
    by_day = {d: set(g["fingerprint"]) for d, g in df.groupby("date")}
    sizes = {d: len(s) for d, s in by_day.items()}
    if len(set(sizes.values())) != 1:
        preview = ", ".join([f"{d}:{sizes[d]}" for d in sorted(sizes)[:10]])
        die(f"Per-day row counts vary across days (first 10 shown: {preview})")

    common = set.intersection(*by_day.values()) if by_day else set()
    for d, s in by_day.items():
        if s != common:
            die(f"Common-set mismatch on {d}: missing {len(common - s)}, extra {len(s - common)}")

    # Light bandwidth outlier notes (non-fatal)
    zeros = int((df["relay_bandwidth"] == 0).sum())
    if zeros > 0:
        print(f"NOTE: {zeros} rows have relay_bandwidth == 0", file=sys.stderr)
    huge = int((df["relay_bandwidth"] > 10**9).sum())
    if huge > 0:
        print(f"NOTE: {huge} rows have very large relay_bandwidth (>1e9)", file=sys.stderr)

    # Row count matches days × common
    n_days = len(expected_days)
    n_common = len(common)
    n_rows = len(df)
    if n_rows != n_days * n_common:
        die(f"Row count mismatch: rows={n_rows} but days×common={n_days*n_common}")

    # Hour diagnostics (if present)
    if "hour" in df.columns:
        df["hour"] = pd.to_numeric(df["hour"], errors="coerce").astype("Int64")
        if df["hour"].isna().any():
            die(f"'hour' has {int(df['hour'].isna().sum())} non-numeric cells")

        per_day_hour = df.groupby("date")["hour"].nunique()
        days_multi_hours = int((per_day_hour > 1).sum())
        hour_counts = df.drop_duplicates(["date","hour"])["hour"].value_counts().sort_index()

        print("\nHour usage across days:")
        for h, c in hour_counts.items():
            print(f"  - {int(h):02d}:00 → {c} day(s)")

        if days_multi_hours:
            print(f"WARNING: {days_multi_hours} day(s) have multiple different hours in the CSV")

        off_zero = df.groupby("date")["hour"].first().loc[lambda s: s != 0]
        if len(off_zero):
            print(f"Days not at 00:00 ({len(off_zero)}):")
            for d, h in off_zero.items():
                print(f"  {d} @ {int(h):02d}:00")
    else:
        print("\nNOTE: No 'hour' column found; cannot report hour variation.")

    # OK summary
    print("VALIDATION Summary")
    print("------------------")
    print("Passed all checks: No duplicate fingerprints, missing days, all days aligned.")
    print(f"- File: {args.csv}")
    print(f"- SHA256: {digest}")
    print(f"- Dates: {d0} → {d1} (inclusive) = {n_days} days")
    print(f"- Common relays: {n_common:,}")
    print(f"- Total rows: {n_rows:,} (days × common = {n_days} × {n_common})")

if __name__ == "__main__":
    sys.exit(main())
