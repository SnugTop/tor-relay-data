# tor-relay-data

## What This Project Does

This project collects and analyzes Tor relay bandwidth data to study how stable relay performance is over time.

The Tor network is made up of thousands of volunteer-run relays. Each relay publishes a bandwidth value (in kB/s) representing how much traffic it can handle. This project asks: **how consistent are those values day-to-day, and does stability differ between average and high-capacity relays?**

The key metric is the **Coefficient of Variation (CV)** — a relay's bandwidth standard deviation divided by its mean over a 7-day rolling window. A low CV means a relay's bandwidth is predictable. A high CV means it fluctuates a lot.

---

## Project Structure

```
tor-relay-data/
├── pull_relay_data.py            # Step 1: collect data (primary — with caching)
├── validate_data.py              # Step 2: validate collected CSV
├── relay_graphs.py               # Step 3: generate analysis graphs
├── Archive/code/
│   ├── dump_consensus_panel.py   # Alternative collector (no caching, more flexible)
│   └── data-check.py             # Alternative validator (more flexible)
├── data/                         # collected CSVs (organized by date range)
│   └── 4.10.25-4.9.26/
│       └── 1yr_daily_bw_strict.csv
└── output_graphs/                # generated PNG graphs and write-ups
```

---

## Setup

```bash
# Create and activate virtual environment
python3 -m venv venv
source venv/bin/activate        # macOS/Linux
venv\Scripts\activate           # Windows

# Install dependencies
pip install --upgrade pip
pip install -r requirements.txt
```

---

## Primary Workflow

### Step 1 — Collect Data

`pull_relay_data.py` downloads Tor consensus files from [CollecTor](https://metrics.torproject.org/collector.html) — the Tor Project's official archive of historical network snapshots. It fetches one consensus per day at a fixed UTC hour, extracts each relay's fingerprint and advertised bandwidth, and writes a tidy CSV.

**Key features:**
- **Caching** — monthly tarballs (~25MB each) are saved to `.cache/tor-consensuses/` on first download and reused on future runs. A full year only requires downloading once.
- **Strict intersection** — only relays present on every successfully fetched day are written to the output. This produces a perfectly balanced panel where every relay has the same number of observations, which is appropriate for rolling-window stability analysis.
- **Graceful fallback** — if the exact target hour is missing from the archive, the script searches ±N hours automatically. If a day is entirely missing, it is skipped with a warning rather than crashing the run.

**Output CSV columns:** `date, hour, fingerprint, relay_bandwidth, timestamp`

**Command:**
```bash
python3 pull_relay_data.py \
  --start 2025-04-10 \
  --end 2026-04-09 \
  --hour 0 \
  --hour-fallback 2 \
  --out data/4.10.25-4.9.26/1yr_daily_bw_strict.csv
```

**Flags:**

| Flag | Description |
|------|-------------|
| `--start` | Start date, inclusive (YYYY-MM-DD) |
| `--end` | End date, inclusive (YYYY-MM-DD) |
| `--hour` | Target UTC hour to pull each day (0–23). Default: `0` (midnight) |
| `--hour-fallback` | Search ±N hours if exact hour is missing. Default: `2` |
| `--out` | Output CSV path |

**Notes:**
- CollecTor's archive typically lags a few days behind the current date. Days not yet in the archive are skipped and reported in the final summary — this is expected behavior.
- Organize output by date range (e.g., `data/4.10.25-4.9.26/`) to keep datasets easy to find.
- The strict intersection means a relay missing even one day is excluded from the entire output. This is intentional — it ensures statistical consistency but should be acknowledged as a methodological constraint (these are the most persistent relays, not the full network).

---

### Step 2 — Validate Data

`validate_data.py` reads the collected CSV and runs a comprehensive health check: schema validation, duplicate detection, date coverage, per-day relay consistency, hour diagnostics, and a SHA256 hash for reproducibility.

**Command:**
```bash
python3 validate_data.py \
  data/4.10.25-4.9.26/1yr_daily_bw_strict.csv \
  --strict-panel \
  --max-missing-days 10
```

**Flags:**

| Flag | Description |
|------|-------------|
| `csv_path` | Path to the CSV to validate (positional argument) |
| `--strict-panel` | Require identical relay set on every day. Use this with `pull_relay_data.py` output, which guarantees this by construction |
| `--max-missing-days` | Allow up to N missing days in the date range. Default: `0` (no gaps). Set to `10` to account for archive lag on recent dates |

**What to look for in the output:**
- **Unique relays** — with strict intersection, this should be the same on every day
- **Missing days** — a small number is normal due to archive lag; anything beyond 10 in a year-long pull warrants investigation
- **Null / negative values** — should be 0
- **Duplicate (fingerprint, date)** — should be 0
- **Zero-bandwidth rows** — a handful is normal; large numbers suggest a data quality issue
- **Hour usage** — most days should be at `00:00`; occasional `01:00` entries reflect the fallback working correctly
- **SHA256** — record this for reproducibility. It uniquely identifies the exact dataset used for analysis

---

### Step 3 — Generate Graphs

`relay_graphs.py` reads the CSV, applies a 7-day rolling window per relay, computes stability statistics, and generates 9 graphs across three relay groups.

**Command:**
```bash
python3 relay_graphs.py \
  data/4.10.25-4.9.26/1yr_daily_bw_strict.csv \
  output_graphs/
```

If you omit the output folder, graphs are displayed interactively instead of saved.

**Arguments:**

| Argument | Description |
|----------|-------------|
| `csv_path` | Path to the relay bandwidth CSV (required) |
| `output_folder` | Folder to save PNG graphs (optional; displays interactively if omitted) |

**Config (top of file):**

| Variable | Default | Description |
|----------|---------|-------------|
| `WINDOW_DAYS` | `7` | Rolling window size in days |
| `MIN_PRESENCE_FRAC` | `0.80` | Minimum fraction of days a relay must appear to be included. When using `pull_relay_data.py` (strict intersection), all relays already pass this — it acts as a safety net for other datasets |

**Output graphs (9 total):**

Three metrics × three relay groups:

| File | What it shows |
|------|--------------|
| `all_relays_bandwidth.png` | Median bandwidth across all relays over time |
| `all_relays_stddev.png` | Median standard deviation across all relays |
| `all_relays_cv.png` | **Primary graph.** Median CV — how stable is the typical relay? |
| `top5_bandwidth.png` | Same metrics, filtered to top 5% by median bandwidth |
| `top5_stddev.png` | |
| `top5_cv.png` | CV for top 5% — are high-capacity relays more or less stable? |
| `top10_bandwidth.png` | Same for top 10% |
| `top10_stddev.png` | |
| `top10_cv.png` | |

Each graph shows:
- **Solid line** — median value across all relays in the group for that 7-day window
- **Shaded band** — 10th–90th percentile range, showing the spread of relay behavior

**Relay group classification:**
Relays are ranked by their **median bandwidth across the full dataset**. This is more representative than using a single day's snapshot.

**Note on the gap at the start of each graph:**
All graphs show a blank area before the first x-axis tick. This is the 7-day rolling window warmup — the first 6 days of the dataset do not have enough observations to compute a rolling value (`min_periods=7`). It is not missing data.

**Column compatibility:**
The script accepts both `relay_bandwidth` (from `pull_relay_data.py`) and `advertised_bw` (from `dump_consensus_panel.py`) column names automatically.

---

## Interpreting Results

- **CV ~0.05** means a relay's bandwidth fluctuates about 5% week-to-week — very stable
- **CV ~0.08–0.10** is typical for high-capacity relays, likely reflecting intentional capacity management by larger operators
- **CV ~0.15–0.35** indicates meaningfully erratic behavior
- A **spike in the CV graph** across all relay groups on the same date suggests a network-wide event — worth cross-referencing with [Tor Metrics](https://metrics.torproject.org) and Tor release history
- The **shaded band** reflects the diversity of relay behavior — even if the median is stable, some relays are always more volatile
- **High-capacity relays** (top 5%, top 10%) tend to show higher CV than average, not because they are less reliable, but because large operators actively adjust capacity

---

## Alternative Pipeline (Flexible / Exploratory)

For exploratory work where a strict balanced panel is not required:

**Collect** (no caching, supports all-hours mode, no strict intersection):
```bash
python3 Archive/code/dump_consensus_panel.py \
  --start 2025-04-10 \
  --end 2026-04-09 \
  --mode fixed-hour \
  --hour 0 \
  --hour-fallback 2 \
  --out data/4.10.25-4.9.26/1yr_daily_bw.csv
```

**Validate** (flexible — reports rather than enforces strict panel):
```bash
python3 Archive/code/data-check.py \
  data/4.10.25-4.9.26/1yr_daily_bw.csv \
  --baseline auto
```

This pipeline produces a larger dataset (all relays seen on each day, not just the intersection) but is appropriate for network-wide snapshots or when studying relay churn. The output uses the column name `advertised_bw` instead of `relay_bandwidth` — `relay_graphs.py` handles both automatically.

---

## Data Source

All data comes from [CollecTor](https://metrics.torproject.org/collector.html), the Tor Project's official archive of historical network consensuses. Each consensus is published hourly and contains the bandwidth weight assigned to every active relay by the directory authorities.
