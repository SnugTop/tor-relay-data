#!/usr/bin/env python3
"""
data-check.py

Quick health-check for Tor relay CSVs produced by onionoo_to_csv.py.

Usage:
  python3 data-check.py path/to/daily_bw.csv [--baseline {midnight,time,auto}] [--baseline-time HH:MM]

What it reports:
- total rows & unique relays
- date range
- duplicate (fingerprint, date) keys
- null / negative values
- zero readings (count, # relays, top offenders)
- time-of-day offset stats relative to a baseline:
    baseline=midnight  → 00:00 UTC
    baseline=time      → HH:MM you pass
    baseline=auto      → uses the dataset’s most common HH:MM (UTC)
"""

import sys
import argparse
from pathlib import Path
import datetime as dt

import pandas as pd
import numpy as np


def load_csv(path: Path) -> pd.DataFrame:
    # Parse date-like columns if present
    preview = pd.read_csv(path, nrows=1)
    parse_cols = [c for c in ["date", "timestamp"] if c in preview.columns]
    df = pd.read_csv(path, parse_dates=parse_cols)
    # If date missing but timestamp present, derive date from timestamp (UTC day)
    if "date" not in df.columns and "timestamp" in df.columns:
        ts = pd.to_datetime(df["timestamp"], errors="coerce", utc=True)
        df["date"] = ts.dt.tz_convert("UTC").dt.date
    return df


def fmt_int(x): return f"{int(x):,}"


def parse_hhmm(s: str) -> tuple[int, int]:
    hh, mm = s.split(":")
    hh, mm = int(hh), int(mm)
    if not (0 <= hh < 24 and 0 <= mm < 60):
        raise ValueError("HH:MM out of range")
    return hh, mm


def infer_baseline_hhmm(ts_utc: pd.Series) -> tuple[int, int]:
    # Use most common minute-of-day as the dataset's "center"
    hhmm = ts_utc.dt.strftime("%H:%M")
    mode = hhmm.mode().iloc[0]
    return parse_hhmm(mode)


def seconds_since_midnight(t: pd.Timestamp) -> int:
    return t.hour * 3600 + t.minute * 60 + t.second


def time_offset_stats(df: pd.DataFrame, baseline: str, baseline_time: str | None):
    if "timestamp" not in df.columns:
        print("\n[Time-of-day] No 'timestamp' column present — skipping time offset stats.")
        return

    ts_utc = pd.to_datetime(df["timestamp"], errors="coerce", utc=True)
    if ts_utc.isna().all():
        print("\n[Time-of-day] All 'timestamp' values are NaT — skipping.")
        return

    # Decide baseline HH:MM
    if baseline == "midnight":
        base_h, base_m = 0, 0
        baseline_label = "00:00 (UTC)"
    elif baseline == "time":
        if not baseline_time:
            print("[Time-of-day] --baseline time requires --baseline-time HH:MM", file=sys.stderr)
            return
        base_h, base_m = parse_hhmm(baseline_time)
        baseline_label = f"{baseline_time} (UTC)"
    elif baseline == "auto":
        base_h, base_m = infer_baseline_hhmm(ts_utc)
        baseline_label = f"{base_h:02d}:{base_m:02d} (UTC, auto)"
    else:
        print(f"[Time-of-day] Unknown baseline: {baseline}", file=sys.stderr)
        return

    # Ensure a 'date' column as UTC calendar day
    if "date" in df.columns:
        d = pd.to_datetime(df["date"], errors="coerce").dt.tz_localize(None)
    else:
        d = ts_utc.dt.tz_convert("UTC").dt.normalize().dt.tz_localize(None)

    # Build anchors per row: date + baseline HH:MM (UTC)
    base_seconds = base_h * 3600 + base_m * 60
    midnight = d.dt.normalize()
    anchor_today = midnight + pd.to_timedelta(base_seconds, unit="s")
    # To handle near-midnight wrap properly, consider prev/next day anchors and take the min abs delta
    anchor_prev = anchor_today - pd.Timedelta(days=1)
    anchor_next = anchor_today + pd.Timedelta(days=1)

    ts_naive = ts_utc.dt.tz_convert("UTC").dt.tz_localize(None)
    delta_today = (ts_naive - anchor_today).abs().dt.total_seconds()
    delta_prev = (ts_naive - anchor_prev).abs().dt.total_seconds()
    delta_next = (ts_naive - anchor_next).abs().dt.total_seconds()
    delta_sec = np.minimum.reduce([delta_today, delta_prev, delta_next])

    # Stats
    med = float(np.nanmedian(delta_sec))
    p95 = float(np.nanpercentile(delta_sec, 95))
    mx = float(np.nanmax(delta_sec))

    print("\n[Time-of-day]")
    print(f"  baseline: {baseline_label}")
    print(f"  offset from baseline: median={med:.0f}s ({med/3600:.2f}h), p95={p95:.0f}s ({p95/3600:.2f}h), max={mx:.0f}s ({mx/3600:.2f}h)")

    # Bucketize by hours
    bins = [-0.1, 3600, 3*3600, 6*3600, 12*3600, 24*3600, np.inf]
    labels = ["0–1h", "1–3h", "3–6h", "6–12h", "12–24h", ">24h"]
    bucket = pd.cut(delta_sec, bins=bins, labels=labels)
    dist = bucket.value_counts().reindex(labels, fill_value=0)
    print("  buckets:")
    for lbl in labels:
        print(f"    {lbl:<6} : {fmt_int(dist[lbl])}")

    # Top HH:MM in dataset (for reference)
    top_hhmm = ts_utc.dt.strftime("%H:%M").value_counts().head(5)
    print("  most common sample times (UTC):")
    for k, v in top_hhmm.items():
        print(f"    {k} → {fmt_int(v)} rows")


def main():
    ap = argparse.ArgumentParser(description="Quick validator for Tor relay CSV.")
    ap.add_argument("csv_path", type=str, help="Path to CSV (e.g., daily_bw_1year.csv)")
    ap.add_argument("--baseline", choices=["midnight", "time", "auto"], default="midnight",
                    help="Offset baseline: midnight (00:00 UTC), a specific time (--baseline-time), or inferred from data (auto).")
    ap.add_argument("--baseline-time", type=str, default=None,
                    help="HH:MM in UTC. Used only when --baseline time.")
    args = ap.parse_args()

    path = Path(args.csv_path)
    if not path.exists():
        print(f"File not found: {path}")
        sys.exit(2)

    df = load_csv(path)

    # Required columns check
    required = {"date", "fingerprint", "advertised_bw"}
    missing = required - set(df.columns)
    if missing:
        print(f"ERROR: Missing required columns: {sorted(missing)}")
        print(f"Columns present: {list(df.columns)}")
        sys.exit(2)

    # Shape & range
    n_rows = len(df)
    n_relays = df["fingerprint"].nunique()
    date_min = pd.to_datetime(df["date"], errors="coerce").min()
    date_max = pd.to_datetime(df["date"], errors="coerce").max()

    print("[Shape]")
    print(f"  rows           : {fmt_int(n_rows)}")
    print(f"  unique relays  : {fmt_int(n_relays)}")
    print(f"  date range     : {date_min} → {date_max}")

    # Data quality
    n_null_fp = df["fingerprint"].isna().sum()
    n_null_bw = df["advertised_bw"].isna().sum()
    n_neg_bw = (df["advertised_bw"] < 0).sum() if np.issubdtype(df["advertised_bw"].dtype, np.number) else "n/a"
    print("\n[Data quality]")
    print(f"  null fingerprints : {fmt_int(n_null_fp)}")
    print(f"  null advertised_bw: {fmt_int(n_null_bw)}")
    print(f"  negative advertised_bw: {n_neg_bw}")

    # Duplicates
    dup = df.duplicated(subset=["fingerprint", "date"]).sum()
    print(f"  duplicate (fingerprint,date) rows: {fmt_int(dup)}")

    # Zero readings
    zero_rows = df[df["advertised_bw"] == 0]
    n_zero_rows = len(zero_rows)
    n_relays_with_zero = zero_rows["fingerprint"].nunique()
    print("\n[Zero readings]")
    print(f"  rows with advertised_bw == 0   : {fmt_int(n_zero_rows)}")
    print(f"  relays that reported any zero  : {fmt_int(n_relays_with_zero)}")
    if n_zero_rows > 0:
        top_zero = zero_rows.groupby("fingerprint").size().sort_values(ascending=False).head(10)
        print("  top relays by zero-day count (fingerprint → days):")
        for fp, cnt in top_zero.items():
            print(f"    {fp} → {fmt_int(cnt)}")

    # Time-of-day offsets (configurable baseline)
    time_offset_stats(df, baseline=args.baseline, baseline_time=args.baseline_time)

    # Conclusion
    print("\n[Conclusion]")
    problems = []
    if n_null_fp > 0: problems.append("null fingerprints")
    if n_null_bw > 0: problems.append("null advertised_bw")
    if isinstance(n_neg_bw, (int, np.integer)) and n_neg_bw > 0: problems.append("negative advertised_bw")
    if dup > 0: problems.append("duplicate (fingerprint,date) keys")

    if problems:
        print("  ⚠ Issues detected:", "; ".join(problems))
    else:
        print("  ✅ Dataset looks structurally sound.")


if __name__ == "__main__":
    main()
