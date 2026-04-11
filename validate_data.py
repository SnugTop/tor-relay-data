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
    p = argparse.ArgumentParser(description="Validate tor daily bandwidth panel CSV.")
    p.add_argument("csv", help="Path to daily_bw.csv")
    p.add_argument("--max-missing-days", type=int, default=0,
                   help="Allow up to N missing days in the date range. Default: 0 (strict, no gaps).")
    p.add_argument("--strict-panel", action="store_true",
                   help="Require identical relay set on every day (only valid for pull_relay_data.py output).")
    return p.parse_args()

def daterange(d0, d1):
    days, d = [], d0
    while d <= d1:
        days.append(d)
        d += dt.timedelta(days=1)
    return days

def main():
    args = parse_args()

    # --- SHA256 (streaming, memory-safe for large files) ---
    sha256 = hashlib.sha256()
    with open(args.csv, "rb") as fh:
        for chunk in iter(lambda: fh.read(65536), b""):
            sha256.update(chunk)
    digest = sha256.hexdigest()

    # --- Load ---
    try:
        df = pd.read_csv(args.csv, dtype={"fingerprint": "string"})
    except Exception as e:
        die(f"Failed to read CSV: {e}")

    # --- Column name normalisation: accept both collector output formats ---
    if "relay_bandwidth" not in df.columns and "advertised_bw" in df.columns:
        df = df.rename(columns={"advertised_bw": "relay_bandwidth"})

    required = ["date", "fingerprint", "relay_bandwidth", "timestamp"]
    missing_cols = [c for c in required if c not in df.columns]
    if missing_cols:
        die(f"Missing required columns: {missing_cols}. Present: {list(df.columns)}")

    # --- Types & basic sanity ---
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

    if df[["date", "fingerprint", "relay_bandwidth", "timestamp"]].isna().any().any():
        die("Found NA/null values in required columns")

    # --- No duplicate (date, fingerprint) ---
    dup = int(df.duplicated(subset=["date", "fingerprint"]).sum())
    if dup:
        die(f"Found {dup} duplicate (date, fingerprint) rows")

    # --- Date coverage ---
    days_present = sorted(set(df["date"].tolist()))
    d0, d1 = min(days_present), max(days_present)
    expected_days = daterange(d0, d1)
    missing_days = [d for d in expected_days if d not in days_present]

    if len(missing_days) > args.max_missing_days:
        die(
            f"Date coverage has {len(missing_days)} gap(s) "
            f"(allowed: {args.max_missing_days} via --max-missing-days). "
            f"First few missing: {', '.join(map(str, missing_days[:10]))}"
        )
    elif missing_days:
        print(f"NOTE: {len(missing_days)} missing day(s) within allowed tolerance: "
              f"{', '.join(map(str, missing_days[:10]))}")

    # --- Per-day relay consistency ---
    by_day = {d: set(g["fingerprint"]) for d, g in df.groupby("date")}
    sizes = {d: len(s) for d, s in by_day.items()}

    if len(set(sizes.values())) != 1:
        if args.strict_panel:
            preview = ", ".join([f"{d}:{sizes[d]}" for d in sorted(sizes)[:10]])
            die(f"Per-day relay counts vary (--strict-panel enabled). First 10: {preview}")
        else:
            min_c, max_c = min(sizes.values()), max(sizes.values())
            print(f"NOTE: per-day relay count varies ({min_c:,}–{max_c:,}). "
                  "Use --strict-panel to treat this as an error.")

    # --- Bandwidth outlier notes (non-fatal) ---
    zeros = int((df["relay_bandwidth"] == 0).sum())
    if zeros:
        print(f"NOTE: {zeros} rows have relay_bandwidth == 0")
    huge = int((df["relay_bandwidth"] > 10**9).sum())
    if huge:
        print(f"NOTE: {huge} rows have very large relay_bandwidth (>1e9)")

    # --- Hour diagnostics ---
    if "hour" in df.columns:
        df["hour"] = pd.to_numeric(df["hour"], errors="coerce").astype("Int64")
        if df["hour"].isna().any():
            die(f"'hour' has {int(df['hour'].isna().sum())} non-numeric cells")

        per_day_hour = df.groupby("date")["hour"].nunique()
        days_multi_hours = int((per_day_hour > 1).sum())
        hour_counts = df.drop_duplicates(["date", "hour"])["hour"].value_counts().sort_index()

        print("\nHour usage across days:")
        for h, c in hour_counts.items():
            print(f"  {int(h):02d}:00 → {c} day(s)")

        if days_multi_hours:
            print(f"WARNING: {days_multi_hours} day(s) have multiple different hours in the CSV")

        off_hour = df.groupby("date")["hour"].first()
        off_hour = off_hour[off_hour != 0]
        if len(off_hour):
            print(f"\nDays not at 00:00 ({len(off_hour)}):")
            for d, h in off_hour.items():
                print(f"  {d} @ {int(h):02d}:00")
    else:
        print("\nNOTE: No 'hour' column — cannot report hour variation.")

    # --- Summary ---
    n_days = len(days_present)
    n_relays = df["fingerprint"].nunique()
    n_rows = len(df)

    print("\nVALIDATION PASSED")
    print("-----------------")
    print(f"  File          : {args.csv}")
    print(f"  SHA256        : {digest}")
    print(f"  Date range    : {d0} → {d1} ({n_days} days present, {len(missing_days)} missing)")
    print(f"  Unique relays : {n_relays:,}")
    print(f"  Total rows    : {n_rows:,}")

if __name__ == "__main__":
    sys.exit(main())
