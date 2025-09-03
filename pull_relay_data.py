#!/usr/bin/env python3
import argparse
import base64
import bz2
import csv
import datetime as dt
import sys
import time
import urllib.request
import lzma 
import re
import tarfile
import os
from io import BytesIO

def log(msg: str):
    print(msg, file=sys.stderr, flush=True)


ARCHIVE_BASE = "https://collector.torproject.org/archive/relay-descriptors/consensuses"
NAME_PAT = r"(?:consensuses-)?{date}-{hh}-00-00(?:-00)?-consensus(?:\.(?:xz|bz2))?$"


def daterange(start_date, end_date):
    day = start_date
    while day <= end_date:
        yield day
        day += dt.timedelta(days=1)


def fetch_from_month_tar(day: dt.date, hour: int, timeout=60):
    month = day.strftime('%Y-%m')
    month_url = f"{ARCHIVE_BASE}/consensuses-{month}.tar.xz"

    cache_dir = os.path.join(".cache", "tor-consensuses")
    os.makedirs(cache_dir, exist_ok=True)
    cache_path = os.path.join(cache_dir, f"consensuses-{month}.tar.xz")

    # use cache if present
    if os.path.exists(cache_path):
        log(f"    • Using cached tar: {cache_path}")
        with open(cache_path, "rb") as fh:
            blob = fh.read()
    else:
        log(f"    • Downloading tar: {month_url}")
        req = urllib.request.Request(month_url, headers={"User-Agent": "tor-relay-data/1.0 (+research use)"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            blob = resp.read()
        with open(cache_path, "wb") as fh:
            fh.write(blob)
        log(f"    • Saved cache: {cache_path}")

    datestr = day.strftime("%Y-%m-%d")
    hh = f"{hour:02d}"
    pat = re.compile(NAME_PAT.format(date=re.escape(datestr), hh=re.escape(hh)))

    with tarfile.open(fileobj=BytesIO(blob), mode="r:xz") as tf:
        for m in tf.getmembers():
            base = m.name.split("/")[-1]
            if pat.match(base):
                f = tf.extractfile(m)
                data = f.read()
                if base.endswith(".xz"):
                    data = lzma.decompress(data)
                elif base.endswith(".bz2"):
                    data = bz2.decompress(data)
                log(f"    ✓ Found in tar: {base}")
                return data.decode("utf-8", errors="replace")

    raise RuntimeError(
        f"Consensus for {datestr} {hh}:00 not found in {month_url}. "
        f"Tried patterns like {datestr}-{hh}-00-00[-00]-consensus[.xz|.bz2]."
    )

def fetch_consensus(day: dt.date, hours, timeout=60):
    last_err = None
    for hour in hours:
        try:
            log(f"  ↳ Using month tar for {day.isoformat()} @ {hour:02d}:00")
            return fetch_from_month_tar(day, hour, timeout=timeout)
        except Exception as e:
            last_err = e
            log(f"    ✗ Not in tar for hour {hour:02d}: {e}")
            continue
    raise RuntimeError(f"Failed to fetch consensus for {day.isoformat()}: {last_err}")

def b64_to_hex(b64_id: str) -> str:
    # Identity on 'r' line is base64 (20 bytes). Convert to UPPERCASE hex fingerprint.
    # Base64 can be unpadded in consensus; add padding if necessary.
    padding = '=' * (-len(b64_id) % 4)
    raw = base64.b64decode(b64_id + padding)
    return raw.hex().upper()

def parse_consensus(consensus_text: str):
    """
    Returns dict[fingerprint_hex] = advertised_bandwidth (int).
      - read 'r' lines to get the relay's identity (base64) -> fingerprint hex
      - then read following 'w' line for 'Bandwidth=<int>'
    """
    results = {}
    current_fp = None

    for line in consensus_text.splitlines():
        if not line:
            continue

        if line.startswith("r "):
            # r <nickname> <id> <digest> <pub1> <pub2> <ip> <orport> <dirport>
            parts = line.split()
            if len(parts) >= 3:
                b64id = parts[2]
                try:
                    current_fp = b64_to_hex(b64id)
                except Exception:
                    current_fp = None
            else:
                current_fp = None

        elif current_fp and line.startswith("w "):
            # w Bandwidth=NNNN [Measured=...] ...
            # Pull the Bandwidth field
            try:
                parts = line.split()
                bw = None
                for p in parts[1:]:
                    if p.startswith("Bandwidth="):
                        bw = int(p.split("=", 1)[1])
                        break
                if bw is not None:
                    results[current_fp] = bw
            except Exception:
                pass
            finally:
                # reset to avoid accidentally pairing the same 'w' with next relay
                current_fp = None

        else:
            # lines like 's', 'v', etc. ignore
            pass

    return results

def build_panel(start_date, end_date, hours):
    """
    Returns:
      per_day: list of tuples (date, dict[fingerprint]=bandwidth)
      common_relays: set of fingerprints present every day
    """
    per_day = []
    for day in daterange(start_date, end_date):
        t0 = time.time()
        log(f"[{day.isoformat()}] Fetching consensus (hours tried: {hours})...")
        text = fetch_consensus(day, hours)
        mapping = parse_consensus(text)
        dt_s = time.time() - t0
        log(f"[{day.isoformat()}] Parsed relays: {len(mapping):,} (in {dt_s:.1f}s)")
        per_day.append((day, mapping))

    # Intersection of relays present all days
    if not per_day:
        return [], set()

    common = set(per_day[0][1].keys())
    for _, m in per_day[1:]:
        common &= set(m.keys())

    return per_day, common

def write_csv(per_day, common_relays, out_path):
    # CSV format: date,fingerprint,relay_bandwidth,timestamp
    # timestamp = ISO string at selected hour (only know the consensus hour from the URL pattern).
    with open(out_path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["date", "fingerprint", "relay_bandwidth", "timestamp"])
        for day, mapping in per_day:
            # Best-effort timestamp: only know the date; hour varies by file fetched.
            # Store date @ 00:00; if you want exact hour, you can return it from fetch_consensus.
            stamp = dt.datetime.combine(day, dt.time(0, 0, 0)).isoformat()
            for fp in common_relays:
                bw = mapping.get(fp)
                if bw is not None:
                    w.writerow([day.isoformat(), fp, bw, stamp])

def parse_args():
    p = argparse.ArgumentParser(description="Build daily advertised bandwidth panel from Tor consensuses.")
    p.add_argument("--start", required=True, help="Start date (YYYY-MM-DD)")
    p.add_argument("--end", required=True, help="End date (YYYY-MM-DD)")
    p.add_argument("--hour", type=int, default=0, help="Preferred hour (0-23)")
    p.add_argument("--hour-fallback", type=int, action="append", default=[],
                   help="Fallback hour(s), can be repeated (e.g., --hour-fallback 2 --hour-fallback 4)")
    p.add_argument("--out", default="daily_bw.csv", help="Output CSV path")
    return p.parse_args()

def main():
    args = parse_args()
    log(f"Starting pull: {args.start} → {args.end} @ hour {args.hour} (fallbacks: {args.hour_fallback})")
    start = dt.datetime.strptime(args.start, "%Y-%m-%d").date()
    end = dt.datetime.strptime(args.end, "%Y-%m-%d").date()
    hours = [args.hour] + args.hour_fallback

    # sanity
    for h in hours:
        if h < 0 or h > 23:
            print(f"Invalid hour: {h}", file=sys.stderr)
            return 2

    per_day, common = build_panel(start, end, hours)
    if not per_day:
        print("No days fetched; nothing to write.", file=sys.stderr)
        return 1

    if not common:
        print("Warning: no relays present on all days in the range. CSV will be empty.", file=sys.stderr)

    log(f"Writing CSV to {args.out} (days={len(per_day)}, common_relays={len(common):,})")
    write_csv(per_day, common, args.out)
    print(f"Wrote panel for {len(per_day)} day(s), common relays: {len(common)} → {args.out}")
    return 0

if __name__ == "__main__":
    sys.exit(main())
