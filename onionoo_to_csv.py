
#!/usr/bin/env python3
"""

Onionoo: The consensus_weight you get in Onionoo is the same weight value assigned to relays in the daily/hourly consensus documents.
Onionoo’s consensus_weight = this final, adjusted, authoritative value — not the raw self-advertised number.


onionoo_to_csv.py

Fetches per-relay consensus weight histories from Onionoo and writes a tidy CSV:
    date,fingerprint,advertised_bw

Examples:
  python onionoo_to_csv.py --out daily_bw.csv                   # default 1year window if available
  python onionoo_to_csv.py --out daily_bw.csv --window 3months

Dependencies:
  - requests
  - pandas
"""

import argparse
import sys
import time
from pathlib import Path
from typing import Dict, Any, Optional, List

import requests
import pandas as pd

BASE = "https://onionoo.torproject.org/weights"
PAGE_SIZE = 5000
TIMEOUT = 20
RETRIES = 4
BACKOFF = 1.5

WINDOW_KEYS = ["1_year", "6_months", "3_months", "1_month", "1_week"]


def get_json(params: Dict[str, Any]) -> Dict[str, Any]:
    """GET with basic retries."""
    last_err = None
    for attempt in range(1, RETRIES + 1):
        try:
            r = requests.get(BASE, params=params, timeout=TIMEOUT,
                             headers={"User-Agent": "tor-metrics-research/1.0"})
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
    # prefer requested window, else fall back to longest available
    if preferred and preferred in cw:
        return cw[preferred]
    for k in WINDOW_KEYS:
        if k in cw:
            return cw[k]
    return None


def unpack_history(hist: Dict[str, Any]) -> pd.DataFrame:
    """Expand Onionoo history into rows with absolute timestamps and values applied with 'factor'."""
    first = pd.to_datetime(hist["first"], utc=True)
    interval = pd.to_timedelta(int(hist.get("interval", 86400)), unit="s")
    factor = hist.get("factor", 1)
    vals = hist.get("values", [])
    if not isinstance(vals, list):
        vals = []
    dates = [first + i * interval for i in range(len(vals))]
    data = {
        "date": pd.to_datetime(dates, utc=True).tz_convert("UTC").normalize(),
        "value": [None if v is None else v * factor for v in vals],
    }
    return pd.DataFrame(data)


def fetch_all(window: Optional[str]) -> pd.DataFrame:
    """Page through all relays and collect consensus_weight history."""
    rows: List[pd.DataFrame] = []
    offset = 0
    while True:
        params = {"limit": PAGE_SIZE, "offset": offset}
        j = get_json(params)
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
        return pd.DataFrame(columns=["date", "fingerprint", "advertised_bw"])

    out = pd.concat(rows, ignore_index=True)
    out.rename(columns={"value": "advertised_bw"}, inplace=True)
    out["date"] = pd.to_datetime(out["date"]).dt.tz_localize(None)  # drop tz
    out = out[["date", "fingerprint", "advertised_bw"]].sort_values(["fingerprint", "date"])
    return out


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


def main():
    parser = argparse.ArgumentParser(description="Export Onionoo consensus_weight histories to CSV.")
    parser.add_argument("--out", type=str, required=True, help="Output CSV path (e.g., daily_bw.csv).")
    parser.add_argument("--window", type=str, default=None,
                        choices=[None, "1week", "1month", "3months", "6months", "1year"],
                        help="Preferred history window. Falls back to longest available if missing.")
    args = parser.parse_args()

    preferred = normalize_window(args.window)
    df = fetch_all(preferred)
    if df.empty:
        print("No data returned from Onionoo. Try a different window or check connectivity.", file=sys.stderr)
        sys.exit(2)

    out_path = Path(args.out).expanduser().resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False)
    print(f"Wrote {len(df):,} rows to {out_path}")


if __name__ == "__main__":
    main()
