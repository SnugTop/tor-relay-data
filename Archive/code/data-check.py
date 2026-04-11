#!/usr/bin/env python3
"""
data-check.py

Health-check + optional cleaning for Tor relay CSVs produced by onionoo_to_csv.py.

It can:
  1) Print health metrics (shape, range, dupes, zero rows, time-of-day offsets),
  2) Filter to a date window (last N days or start/end),
  3) Keep only rows within a time offset of a baseline time (midnight | fixed HH:MM | auto),
  4) Deduplicate (fingerprint,date) by picking the row closest to the baseline,
  5) Keep only relays present on *every* day in the window (strict intersection),
  6) Write a clean CSV ready for graphing.

Columns expected:
  - date (YYYY-MM-DD or datetime)
  - fingerprint
  - advertised_bw      (Onionoo consensus_weight; ~kB/s)
  - timestamp (UTC datetime)  # recommended for offset filtering

Examples:
  python3 data-check.py daily_bw_1year.csv --baseline auto
  python3 data-check.py daily_bw_1year.csv --baseline time --baseline-time 00:00 --last-days 365 \
      --max-offset-hours 1 --require-all-days --out-clean verified_clean.csv
"""
import sys
import argparse
from pathlib import Path
import datetime as dt
from typing import Optional, Tuple, List

import pandas as pd
import numpy as np


# ---------- basic utils ----------
def fmt_int(x): return f"{int(x):,}"

def parse_hhmm(s: str) -> Tuple[int, int]:
    hh, mm = s.split(":")
    hh, mm = int(hh), int(mm)
    if not (0 <= hh < 24 and 0 <= mm < 60):
        raise ValueError("HH:MM out of range")
    return hh, mm

def infer_baseline_hhmm(ts_utc: pd.Series) -> Tuple[int, int]:
    hhmm = ts_utc.dt.strftime("%H:%M")
    mode = hhmm.mode().iloc[0]
    return parse_hhmm(mode)

def load_csv(path: Path) -> pd.DataFrame:
    preview = pd.read_csv(path, nrows=1)
    parse_cols = [c for c in ["date", "timestamp"] if c in preview.columns]
    df = pd.read_csv(path, parse_dates=parse_cols)
    # derive date from timestamp if missing
    if "date" not in df.columns and "timestamp" in df.columns:
        ts = pd.to_datetime(df["timestamp"], errors="coerce", utc=True)
        df["date"] = ts.dt.tz_convert("UTC").dt.date
    return df

# ---------- offset + baseline ----------
def compute_offset_seconds(df: pd.DataFrame, base_h: int, base_m: int) -> pd.Series:
    """
    Minimal absolute seconds between each row's timestamp and the baseline time
    for that calendar day, considering prev/next day anchors.
    Always returns a pandas Series aligned to df.index.
    """
    if "timestamp" not in df.columns:
        return pd.Series(np.nan, index=df.index)

    ts_utc = pd.to_datetime(df["timestamp"], errors="coerce", utc=True)
    if "date" in df.columns:
        d = pd.to_datetime(df["date"], errors="coerce").dt.tz_localize(None)
    else:
        d = ts_utc.dt.tz_convert("UTC").dt.normalize().dt.tz_localize(None)

    base_seconds = base_h * 3600 + base_m * 60
    midnight = d.dt.normalize()
    anchor_today = midnight + pd.to_timedelta(base_seconds, unit="s")
    anchor_prev  = anchor_today - pd.Timedelta(days=1)
    anchor_next  = anchor_today + pd.Timedelta(days=1)

    ts_naive = ts_utc.dt.tz_convert("UTC").dt.tz_localize(None)
    delta_today = (ts_naive - anchor_today).abs().dt.total_seconds()
    delta_prev  = (ts_naive - anchor_prev ).abs().dt.total_seconds()
    delta_next  = (ts_naive - anchor_next ).abs().dt.total_seconds()

    # IMPORTANT: return a Series aligned to df.index
    delta_sec = pd.concat([delta_today, delta_prev, delta_next], axis=1).min(axis=1)
    return delta_sec


def summarize_offsets(delta_sec: pd.Series, label: str):
    med = float(np.nanmedian(delta_sec))
    p95 = float(np.nanpercentile(delta_sec, 95))
    mx  = float(np.nanmax(delta_sec))
    print("\n[Time-of-day]")
    print(f"  baseline: {label}")
    print(f"  offset from baseline: median={med:.0f}s ({med/3600:.2f}h), p95={p95:.0f}s ({p95/3600:.2f}h), max={mx:.0f}s ({mx/3600:.2f}h)")
    bins = [-0.1, 3600, 3*3600, 6*3600, 12*3600, 24*3600, np.inf]
    labels = ["0–1h", "1–3h", "3–6h", "6–12h", "12–24h", ">24h"]
    bucket = pd.cut(delta_sec, bins=bins, labels=labels)
    dist = bucket.value_counts().reindex(labels, fill_value=0)
    print("  buckets:")
    for lbl in labels:
        print(f"    {lbl:<6}: {fmt_int(dist[lbl])}")

# ---------- dedupe to nearest baseline ----------
def dedupe_nearest(df: pd.DataFrame, delta_sec: pd.Series) -> pd.DataFrame:
    """
    Ensure one row per (fingerprint, date). If duplicates exist,
    keep the one with minimal offset seconds; else keep first.
    """
    df = df.copy()
    df["_delta"] = delta_sec
    if not {"fingerprint", "date"}.issubset(df.columns):
        return df.drop(columns=["_delta"], errors="ignore")

    def pick(g: pd.DataFrame) -> pd.DataFrame:
        if g.shape[0] == 1: return g
        # prefer lowest _delta (if NaN, push to end)
        g = g.assign(_delta2=g["_delta"].fillna(np.inf))
        return g.nsmallest(1, "_delta2").drop(columns=["_delta2"])

    out = (
        df.groupby(["fingerprint", pd.to_datetime(df["date"]).dt.normalize()], as_index=False, group_keys=False)
          .apply(pick)
          .reset_index(drop=True)
    )
    return out.drop(columns=["_delta"], errors="ignore")

# ---------- strict presence ----------
def strict_intersection(df: pd.DataFrame, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
    """
    Keep only relays present on *every* calendar day in [start, end].
    Assumes one row per (fingerprint, date).
    """
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"]).dt.normalize()
    all_days = pd.date_range(start, end, freq="D")
    n_days = len(all_days)
    counts = df.groupby("fingerprint")["date"].nunique()
    keep_relays = counts[counts == n_days].index
    kept = df[df["fingerprint"].isin(keep_relays)].copy()
    return kept

# ---------- main ----------
def main():
    ap = argparse.ArgumentParser(description="Health-check + optional cleaning for Tor relay CSV.")
    ap.add_argument("csv_path", type=str, help="Path to CSV (e.g., daily_bw_1year.csv)")

    # baseline / offsets
    ap.add_argument("--baseline", choices=["midnight", "time", "auto"], default="midnight",
                    help="Offset baseline: midnight (00:00 UTC), a specific time (--baseline-time), or auto-infer.")
    ap.add_argument("--baseline-time", type=str, default=None,
                    help="HH:MM in UTC. Used only when --baseline time.")
    ap.add_argument("--max-offset-hours", type=float, default=None,
                    help="If set, drop rows whose timestamp is farther than this many hours from baseline.")

    # window selection
    ap.add_argument("--start", type=str, default=None, help="Start date YYYY-MM-DD (inclusive).")
    ap.add_argument("--end",   type=str, default=None, help="End date YYYY-MM-DD (inclusive).")
    ap.add_argument("--last-days", type=int, default=None,
                    help="If --start/--end omitted, use the last N days ending at max(date). (e.g., 365)")

    # strict presence
    ap.add_argument("--require-all-days", action="store_true",
                    help="Keep only relays that appear on *every* day in the selected window.")

    # output
    ap.add_argument("--out-clean", type=str, default=None,
                    help="If provided, write a cleaned CSV ready to graph.")

    args = ap.parse_args()
    path = Path(args.csv_path)
    if not path.exists():
        print(f"File not found: {path}")
        sys.exit(2)

    df = load_csv(path)

    # required columns
    required = {"date", "fingerprint", "advertised_bw"}
    missing = required - set(df.columns)
    if missing:
        print(f"ERROR: Missing required columns: {sorted(missing)}")
        print(f"Columns present: {list(df.columns)}")
        sys.exit(2)

    # --- health metrics ---
    n_rows = len(df)
    n_relays = df["fingerprint"].nunique()
    dmin = pd.to_datetime(df["date"], errors="coerce").min()
    dmax = pd.to_datetime(df["date"], errors="coerce").max()

    print("[Shape]")
    print(f"  rows          : {fmt_int(n_rows)}")
    print(f"  unique relays : {fmt_int(n_relays)}")
    print(f"  date range    : {dmin} → {dmax}")

    n_null_fp = df["fingerprint"].isna().sum()
    n_null_bw = df["advertised_bw"].isna().sum()
    n_neg_bw  = (df["advertised_bw"] < 0).sum() if np.issubdtype(df["advertised_bw"].dtype, np.number) else "n/a"
    dup = df.duplicated(subset=["fingerprint", "date"]).sum()
    zeros = (df["advertised_bw"] == 0).sum()

    print("\n[Data quality]")
    print(f"  null fingerprints           : {fmt_int(n_null_fp)}")
    print(f"  null advertised_bw          : {fmt_int(n_null_bw)}")
    print(f"  negative advertised_bw      : {n_neg_bw}")
    print(f"  duplicate (fingerprint,date): {fmt_int(dup)}")
    print(f"  rows with advertised_bw==0  : {fmt_int(zeros)}")

    # baseline selection
    label = "00:00 (UTC)"
    if args.baseline == "midnight":
        base_h, base_m = 0, 0
        label = "00:00 (UTC)"
    elif args.baseline == "time":
        if not args.baseline_time:
            print("--baseline time requires --baseline-time HH:MM", file=sys.stderr); sys.exit(2)
        base_h, base_m = parse_hhmm(args.baseline_time)
        label = f"{args.baseline_time} (UTC)"
    else:  # auto
        if "timestamp" not in df.columns:
            print("--baseline auto needs a 'timestamp' column; falling back to midnight.", file=sys.stderr)
            base_h, base_m = 0, 0
            label = "00:00 (UTC, fallback)"
        else:
            ts_utc = pd.to_datetime(df["timestamp"], errors="coerce", utc=True)
            base_h, base_m = infer_baseline_hhmm(ts_utc)
            label = f"{base_h:02d}:{base_m:02d} (UTC, auto)"

    # offsets + summary
    delta_sec = compute_offset_seconds(df, base_h, base_m)
    summarize_offsets(delta_sec, label)

    # --- if no cleaning requested, exit here ---
    if not args.out_clean and not (args.last_days or args.start or args.end or args.max_offset_hours or args.require_all_days):
        print("\n(No cleaning requested; pass --out-clean to write a filtered CSV.)")
        return

    # --- build window ---
    dates_norm = pd.to_datetime(df["date"], errors="coerce").dt.normalize()
    if args.start and args.end:
        start = pd.Timestamp(args.start)
        end   = pd.Timestamp(args.end)
    else:
        end = dates_norm.max()
        if args.last_days and args.last_days > 0:
            start = end - pd.Timedelta(days=args.last_days - 1)
        else:
            # default: use full span if not specified
            start = dates_norm.min()
    if start > end:
        print("ERROR: start date is after end date.", file=sys.stderr); sys.exit(2)

    # --- apply filters ---
    # 1) window slice
    in_win = (dates_norm >= start) & (dates_norm <= end)
    dfw = df.loc[in_win].copy()
    dfw["date"] = dates_norm.loc[in_win]

    # 2) (optional) time-of-day tolerance
    if args.max_offset_hours is not None and "timestamp" in dfw.columns:
        delta_w = delta_sec.loc[in_win]
        tol_sec = args.max_offset_hours * 3600.0
        keep = (delta_w <= tol_sec)
        before = dfw.shape[0]
        dfw = dfw.loc[keep].copy()
        print(f"\n[Filter] offset ≤ {args.max_offset_hours}h: kept {fmt_int(dfw.shape[0])} / {fmt_int(before)} rows")

    # 3) dedupe (fingerprint, date) to nearest baseline
    delta_w2 = compute_offset_seconds(dfw, base_h, base_m) if "timestamp" in dfw.columns else pd.Series(np.nan, index=dfw.index)
    before = dfw.shape[0]
    dfw = dedupe_nearest(dfw, delta_w2)
    print(f"[Filter] deduplicated (fingerprint,date): {fmt_int(before)} → {fmt_int(dfw.shape[0])}")

    # 4) (optional) strict presence: keep only relays on every day
    if args.require_all_days:
        before_relays = dfw["fingerprint"].nunique()
        dfw = strict_intersection(dfw, start, end)
        after_relays = dfw["fingerprint"].nunique()
        print(f"[Filter] require-all-days in {start.date()}–{end.date()} ({(end-start).days+1} days): "
              f"relays {fmt_int(before_relays)} → {fmt_int(after_relays)}; rows now {fmt_int(dfw.shape[0])}")

    # final sort + write
    dfw = dfw.sort_values(["date", "fingerprint"]).reset_index(drop=True)
    out_path = Path(args.out_clean) if args.out_clean else Path("verified_clean.csv")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    dfw.to_csv(out_path, index=False)

    print(f"\n[Done] wrote clean CSV: {out_path.resolve()}")
    print(f"Rows: {fmt_int(dfw.shape[0])} | Unique relays: {fmt_int(dfw['fingerprint'].nunique())} | Window: {start.date()} → {end.date()}")

if __name__ == "__main__":
    main()
