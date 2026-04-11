#!/usr/bin/env python3
"""
dump_consensus_panel.py

Downloads Tor consensuses from **CollecTor** monthly tarballs and writes a tidy CSV.

Modes:
  - fixed-hour: pick one consensus per day at a chosen hour (e.g., 00 UTC), with ±N hour fallback.
  - all-hours : dump every hourly consensus in the date range.

Output CSV columns:
  date,fingerprint,advertised_bw,timestamp
    date         : UTC calendar day (naive date)
    fingerprint  : 40-hex uppercase relay ID
    advertised_bw: consensus weight (kB/s), from 'w Bandwidth=...'
    timestamp    : consensus 'valid-after' (tz-aware UTC)

Usage:
  python3 dump_consensus_panel.py --start 2024-08-28 --end 2025-08-27 \
      --mode fixed-hour --hour 0 --hour-fallback 2 --out daily_bw.csv

  python3 dump_consensus_panel.py --start 2024-08-28 --end 2025-08-27 \
      --mode all-hours --out hourly_bw.csv

Notes:
  - This streams rows to CSV (low memory). A year of all-hours is large (~8,760 consensuses).
  - For speed, each monthly tar.xz is downloaded once and reused for all days in that month.
"""

import argparse
import sys
from pathlib import Path
from typing import Optional, Tuple, List, Iterable
import datetime as dt
import io
import tarfile
import lzma
import base64
import re
import requests
import csv

ARCHIVE_INDEX = "https://collector.torproject.org/archive/relay-descriptors/consensuses"
UA = {"User-Agent": "tor-metrics-research/1.0"}
TIMEOUT = 30

# ----------- parsing helpers (consensus text) -----------
VALID_AFTER_RE = re.compile(r"^valid-after\s+(\d{4}-\d{2}-\d{2})\s+(\d{2}):(\d{2}):(\d{2})")
# 'r' line: ... identity (base64) ...
R_RE = re.compile(r"^r\s+\S+\s+([A-Za-z0-9+/=]+)\s+\S+\s+\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}\s+\S+\s+\d+\s+\d+")
# 'w' line: Bandwidth=N
BW_RE = re.compile(r"Bandwidth=(\d+)")

def b64_identity_to_hex(s: str) -> Optional[str]:
    try:
        s = s.strip()
        s += "=" * ((4 - len(s) % 4) % 4)
        raw = base64.b64decode(s, validate=False)
        return raw.hex().upper()
    except Exception:
        return None

def parse_consensus(text: str) -> Tuple[dt.datetime, List[Tuple[str, int]]]:
    """
    Parse consensus text. Returns:
      (valid_after_utc (aware), list of (fingerprint, advertised_bw_kBs))
    """
    valid_after = None
    current_fp = None
    out: List[Tuple[str, int]] = []
    for line in text.splitlines():
        if valid_after is None:
            m = VALID_AFTER_RE.match(line)
            if m:
                valid_after = dt.datetime(
                    int(m.group(1)[:4]), int(m.group(1)[5:7]), int(m.group(1)[8:10]),
                    int(m.group(2)), int(m.group(3)), int(m.group(4)),
                    tzinfo=dt.timezone.utc
                )
                continue
        if line.startswith("r "):
            m = R_RE.match(line)
            current_fp = b64_identity_to_hex(m.group(1)) if m else None
            continue
        if line.startswith("w ") and current_fp:
            m = BW_RE.search(line)
            if m:
                try:
                    out.append((current_fp, int(m.group(1))))
                except ValueError:
                    pass
            continue
    if valid_after is None:
        raise ValueError("valid-after not found")
    return valid_after, out

# ----------- CollecTor tar access -----------
def month_url(year: int, month: int) -> str:
    return f"{ARCHIVE_INDEX}/consensuses-{year}-{month:02d}.tar.xz"

def fetch_month_tar(year: int, month: int) -> Optional[bytes]:
    url = month_url(year, month)
    try:
        r = requests.get(url, headers=UA, timeout=TIMEOUT)
        if r.status_code == 200 and r.content:
            return r.content
        if 400 <= r.status_code < 500:
            return None
    except requests.RequestException:
        pass
    return None

def list_members_for_day(tf: tarfile.TarFile, day: dt.date) -> List[str]:
    # members are named: consensuses-YYYY-MM/DD/YYYY-MM-DD-HH-00-00-consensus
    prefix = f"consensuses-{day:%Y-%m}/{day.day:02d}/{day:%Y-%m-%d}-"
    return [m.name for m in tf.getmembers() if m.name.startswith(prefix) and m.name.endswith("-consensus")]

def read_member_text(tf: tarfile.TarFile, name: str) -> Optional[str]:
    try:
        f = tf.extractfile(name)
        if not f:
            return None
        data = f.read()
        return data.decode("utf-8", errors="replace")
    except Exception:
        return None

# ----------- iterators over consensuses -----------
def iter_consensuses_all_hours(start: dt.date, end: dt.date) -> Iterable[Tuple[dt.datetime, List[Tuple[str, int]]]]:
    """Yield (valid_after, rows) for every hourly consensus in [start,end]."""
    cur = dt.date(start.year, start.month, 1)
    end_month = dt.date(end.year, end.month, 1)
    while cur <= end_month:
        blob = fetch_month_tar(cur.year, cur.month)
        if not blob:
            cur = (cur.replace(day=28) + dt.timedelta(days=4)).replace(day=1)  # next month
            continue
        with lzma.open(io.BytesIO(blob)) as xz:
            with tarfile.open(fileobj=xz, mode="r:*") as tf:
                d = max(start, dt.date(cur.year, cur.month, 1))
                last = (cur.replace(day=28) + dt.timedelta(days=4)).replace(day=1) - dt.timedelta(days=1)
                d_end = min(end, last)
                while d <= d_end:
                    names = list_members_for_day(tf, d)
                    names.sort()  # increasing hour
                    for name in names:
                        text = read_member_text(tf, name)
                        if not text:
                            continue
                        try:
                            ts, rows = parse_consensus(text)
                            yield ts, rows
                        except Exception:
                            continue
                    d += dt.timedelta(days=1)
        cur = (cur.replace(day=28) + dt.timedelta(days=4)).replace(day=1)

def iter_consensuses_fixed_hour(start: dt.date, end: dt.date, hour: int, fallback: int) -> Iterable[Tuple[dt.datetime, List[Tuple[str, int]]]]:
    """Yield one (valid_after, rows) per day: exact hour if present, else nearest within ±fallback hours."""
    cur = dt.date(start.year, start.month, 1)
    end_month = dt.date(end.year, end.month, 1)
    while cur <= end_month:
        blob = fetch_month_tar(cur.year, cur.month)
        if not blob:
            cur = (cur.replace(day=28) + dt.timedelta(days=4)).replace(day=1)
            continue
        with lzma.open(io.BytesIO(blob)) as xz:
            with tarfile.open(fileobj=xz, mode="r:*") as tf:
                d = max(start, dt.date(cur.year, cur.month, 1))
                last = (cur.replace(day=28) + dt.timedelta(days=4)).replace(day=1) - dt.timedelta(days=1)
                d_end = min(end, last)
                while d <= d_end:
                    # try exact hour, then ±1, ±2 ... up to fallback
                    tried = []
                    def name_for(h: int) -> str:
                        return f"consensuses-{d:%Y-%m}/{d.day:02d}/{d:%Y-%m-%d}-{h:02d}-00-00-consensus"
                    hours = [hour] + [h for n in range(1, fallback+1) for h in (hour-n, hour+n) if 0 <= h < 24]
                    found = None
                    for h in hours:
                        name = name_for(h)
                        try:
                            tf.getmember(name)
                        except KeyError:
                            continue
                        text = read_member_text(tf, name)
                        if not text: 
                            continue
                        try:
                            ts, rows = parse_consensus(text)
                            found = (ts, rows)
                            break
                        except Exception:
                            continue
                    if found:
                        yield found
                    # else: silently skip day (no consensus found near target hour)
                    d += dt.timedelta(days=1)
        cur = (cur.replace(day=28) + dt.timedelta(days=4)).replace(day=1)

# ----------- CLI + writer -----------
def main():
    ap = argparse.ArgumentParser(description="Dump Tor consensus weights to CSV from CollecTor monthly tarballs.")
    ap.add_argument("--start", required=True, type=str, help="YYYY-MM-DD inclusive")
    ap.add_argument("--end",   required=True, type=str, help="YYYY-MM-DD inclusive")
    ap.add_argument("--mode",  choices=["fixed-hour","all-hours"], default="fixed-hour")
    ap.add_argument("--hour",  type=int, default=0, help="UTC hour for fixed-hour mode (0-23). Default 0.")
    ap.add_argument("--hour-fallback", type=int, default=2, help="±N hours to search if exact hour missing. Default 2.")
    ap.add_argument("--out", required=True, type=str, help="Output CSV path")
    args = ap.parse_args()

    try:
        start = dt.date.fromisoformat(args.start)
        end   = dt.date.fromisoformat(args.end)
    except Exception:
        print("Invalid dates. Use YYYY-MM-DD.", file=sys.stderr); sys.exit(2)
    if start > end:
        print("--start after --end.", file=sys.stderr); sys.exit(2)
    if args.mode == "fixed-hour" and not (0 <= args.hour <= 23):
        print("--hour must be 0..23.", file=sys.stderr); sys.exit(2)

    out_path = Path(args.out).expanduser().resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # Stream to CSV (low memory)
    with out_path.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["date","fingerprint","advertised_bw","timestamp"])
        if args.mode == "all-hours":
            it = iter_consensuses_all_hours(start, end)
        else:
            it = iter_consensuses_fixed_hour(start, end, args.hour, args.hour_fallback)
        n_rows = 0
        n_days = set()
        for ts, pairs in it:
            day = ts.date()  # UTC day
            for fp, bw in pairs:
                w.writerow([day.isoformat(), fp, bw, ts.isoformat()])
                n_rows += 1
            n_days.add(day)
        print(f"Wrote {n_rows:,} rows across {len(n_days)} day(s) to {out_path}")

if __name__ == "__main__":
    main()
