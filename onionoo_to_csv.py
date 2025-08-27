
#!/usr/bin/env python3
"""

INFO:
- Onionoo: The consensus_weight you get in Onionoo is the same weight value assigned to relays in the daily/hourly consensus documents.
- Onionoos consensus_weight = this final, adjusted, authoritative value — not the raw self-advertised number.
- kilobytes per second (kB/s)
- Some relays have an offset because Onionoo does not pull every relay at the same time so we use the closest sample to the requested time.
- Fetches per-relay consensus weight histories from Onionoo and writes a tidy CSV:
    date,fingerprint,advertised_bw


Key options:
  --window      one of: 1week | 1month | 3months | 6months | 1year (prefers 1year if available)
  --sample-time HH:MM in UTC (e.g., 00:00). For each relay and calendar day, picks the sample closest to this time.
  --out         output CSV path

Examples:
  python3 onionoo_to_csv.py --out daily_bw.csv --window 1year --sample-time 00:00
  python3 onionoo_to_csv.py --out daily_bw.csv --window 3months --sample-time 06:00

Dependencies:
  - requests
  - pandas
"""

import argparse
import sys
import time
from pathlib import Path
from typing import Dict, Any, Optional, List, Tuple
import datetime as dt

import requests
import pandas as pd

BASE = "https://onionoo.torproject.org/weights"
PAGE_SIZE = 5000
TIMEOUT = 20
RETRIES = 4
BACKOFF = 1.5

WINDOW_KEYS = ["1_year", "6_months", "3_months", "1_month", "1_week"]


def get_json(params: Dict[str, Any]) -> Dict[str, Any]:
    last_err = None
    for attempt in range(1, RETRIES + 1):
        try:
            r = requests.get(
                BASE,
                params=params,
                timeout=TIMEOUT,
                headers={"User-Agent": "tor-metrics-research/1.0"},
            )
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last_err = e
            if attempt < RETRIES:
                time.sleep(BACKOFF ** attempt)
            else:
                raise
    raise last_err


def choose_history(obj: Dict[str, Any], preferred: Optional[str]) -> Optional[Dict[str, Any]]:
    cw = obj.get("consensus_weight", {})
    if not cw:
        return None
    if preferred and preferred in cw:
        return cw[preferred]
    for k in WINDOW_KEYS:
        if k in cw:
            return cw[k]
    return None


def unpack_history(hist: Dict[str, Any]) -> pd.DataFrame:
    """Expand Onionoo history to absolute timestamps, preserving time-of-day."""
    first = pd.to_datetime(hist["first"], utc=True)
    interval = pd.to_timedelta(int(hist.get("interval", 86400)), unit="s")
    factor = hist.get("factor", 1)
    vals = hist.get("values", [])
    if not isinstance(vals, list):
        vals = []
    ts = [first + i * interval for i in range(len(vals))]
    return pd.DataFrame(
        {
            "timestamp": pd.to_datetime(ts, utc=True),
            "value": [None if v is None else v * factor for v in vals],
        }
    )


def fetch_all(window: Optional[str]) -> pd.DataFrame:
    rows: List[pd.DataFrame] = []
    offset = 0
    while True:
        j = get_json({"limit": PAGE_SIZE, "offset": offset})
        relays = j.get("relays", [])
        if not relays:
            break
        for obj in relays:
            fp = obj.get("fingerprint")
            if not fp:
                continue
            hist = choose_history(obj, preferred=window)
            if not hist:
                continue
            df = unpack_history(hist).dropna(subset=["value"])
            if df.empty:
                continue
            df["fingerprint"] = fp
            rows.append(df)
        if len(relays) < PAGE_SIZE:
            break
        offset += PAGE_SIZE

    if not rows:
        return pd.DataFrame(columns=["timestamp", "fingerprint", "value"])

    out = pd.concat(rows, ignore_index=True)
    # Ensure ordering/types
    out["timestamp"] = pd.to_datetime(out["timestamp"], utc=True)
    return out.sort_values(["fingerprint", "timestamp"])


def normalize_window(w: Optional[str]) -> Optional[str]:
    if w is None:
        return "1_year"
    mapping = {
        "1week": "1_week",
        "1month": "1_month",
        "3months": "3_months",
        "6months": "6_months",
        "1year": "1_year",
    }
    return mapping.get(w, None)


def nearest_daily_sample(df: pd.DataFrame, hh: int, mm: int) -> pd.DataFrame:
    """
    For each relay and calendar date (UTC), pick the sample closest to HH:MM (UTC).
    Returns rows with columns: date (date), fingerprint, advertised_bw, timestamp.
    """
    data = df.copy()
    # calendar day in UTC (date objects)
    data["date"] = data["timestamp"].dt.tz_convert("UTC").dt.date
    target_time = dt.time(hour=hh, minute=mm)

    def pick_closest(group: pd.DataFrame) -> pd.Series:
        # the group's date (all identical)
        day = group["date"].iloc[0]
        # make a naive Timestamp, then tz-localize to UTC (this is the key fix)
        target_ts = pd.Timestamp.combine(day, target_time).tz_localize("UTC")
        # pick the row whose timestamp is closest to target_ts
        deltas = (group["timestamp"] - target_ts).abs().dt.total_seconds()
        idx = deltas.idxmin()
        row = group.loc[idx]
        return pd.Series(
            {
                "date": pd.to_datetime(day),                 # naive date at midnight (UTC day)
                "fingerprint": row["fingerprint"],
                "advertised_bw": row["value"],
                "timestamp": row["timestamp"],               # actual Onionoo sample (tz-aware)
            }
        )

    picked = (
        data.groupby(["fingerprint", "date"], as_index=False, group_keys=False)
            .apply(pick_closest)
            .reset_index(drop=True)
    )
    # make 'date' column naive (no tz); keep timestamp with tz
    picked["date"] = picked["date"].dt.tz_localize(None)
    return picked[["date", "fingerprint", "advertised_bw", "timestamp"]]


def main():
    parser = argparse.ArgumentParser(description="Export Onionoo consensus_weight histories to CSV.")
    parser.add_argument("--out", type=str, required=True, help="Output CSV path (e.g., daily_bw.csv).")
    parser.add_argument(
        "--window",
        type=str,
        default=None,
        choices=[None, "1week", "1month", "3months", "6months", "1year"],
        help="Preferred history window. Falls back to the longest available if missing.",
    )
    parser.add_argument(
        "--sample-time",
        type=str,
        default="00:00",
        help="Target UTC time-of-day (HH:MM) to sample per day (nearest record is chosen). Default 00:00.",
    )
    args = parser.parse_args()

    preferred = normalize_window(args.window)
    raw = fetch_all(preferred)
    if raw.empty:
        print("No data returned from Onionoo. Try a different window or check connectivity.", file=sys.stderr)
        sys.exit(2)

    # Parse HH:MM
    try:
        hh, mm = map(int, args.sample_time.split(":"))
        if not (0 <= hh < 24 and 0 <= mm < 60):
            raise ValueError
    except Exception:
        print("Invalid --sample-time. Use HH:MM (e.g., 00:00, 06:30).", file=sys.stderr)
        sys.exit(2)

    daily = nearest_daily_sample(raw, hh=hh, mm=mm)

    out_path = Path(args.out).expanduser().resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    daily.to_csv(out_path, index=False)
    print(f"Wrote {len(daily):,} rows to {out_path}")
    print("Columns: date (UTC calendar day), fingerprint, advertised_bw (consensus_weight), timestamp (UTC actual sample)")


if __name__ == "__main__":
    main()