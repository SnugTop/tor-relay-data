import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os
import sys

# === CONFIG ===
WINDOW_DAYS = 7
MIN_PRESENCE_FRAC = 0.80  # keep relays present on at least this fraction of days

# === PARSE ARGUMENTS ===
if len(sys.argv) < 2:
    print("Usage: python3 relay_graphs.py <csv_path> [output_folder]")
    print("  csv_path      : path to relay bandwidth CSV")
    print("  output_folder : (optional) folder to save graphs; displays interactively if omitted")
    sys.exit(1)

CSV_PATH = sys.argv[1]
FOLDER = sys.argv[2] if len(sys.argv) > 2 else None

if FOLDER:
    os.makedirs(FOLDER, exist_ok=True)
    print(f"📁 Output folder set to: {FOLDER}")
else:
    print("⚙️  No folder specified — graphs will be displayed interactively instead of saved.")

# === LOAD DATA ===
df = pd.read_csv(CSV_PATH, parse_dates=["date"])

# Derive hour from timestamp if present and hour column is missing
if "hour" not in df.columns and "timestamp" in df.columns:
    df["hour"] = pd.to_datetime(df["timestamp"]).dt.hour
elif "hour" not in df.columns:
    df["hour"] = 0

# Normalize column name: collectors output 'advertised_bw', graphs expect 'relay_bandwidth'
if "relay_bandwidth" not in df.columns and "advertised_bw" in df.columns:
    df = df.rename(columns={"advertised_bw": "relay_bandwidth"})

df = df.sort_values(by=["fingerprint", "date", "hour"])

# === PREPARE DAILY BANDWIDTH ===
daily_bw = df.groupby(["fingerprint", "date"])["relay_bandwidth"].mean().reset_index()

# === FILTER BY MINIMUM PRESENCE ===
total_days = daily_bw["date"].nunique()
min_days = int(np.ceil(MIN_PRESENCE_FRAC * total_days))
days_present = daily_bw.groupby("fingerprint")["date"].nunique()
relays_above_threshold = days_present[days_present >= min_days].index
print(f"ℹ️  Presence filter: {len(relays_above_threshold)} / {days_present.shape[0]} relays present on ≥{MIN_PRESENCE_FRAC*100:.0f}% of {total_days} days (≥{min_days} days)")
daily_bw = daily_bw[daily_bw["fingerprint"].isin(relays_above_threshold)]

# === IDENTIFY TOP RELAYS BASED ON FULL-PERIOD MEDIAN ===
median_bw = daily_bw.groupby("fingerprint")["relay_bandwidth"].median()
threshold_5 = median_bw.quantile(0.95)
threshold_10 = median_bw.quantile(0.90)
top5_relays = set(median_bw[median_bw >= threshold_5].index)
top10_relays = set(median_bw[median_bw >= threshold_10].index)
print(f"ℹ️  Top relay thresholds — top 5%: ≥{threshold_5:.0f} kB/s, top 10%: ≥{threshold_10:.0f} kB/s (based on full-period median)")

# === FUNCTION TO COMPUTE ROLLING STATS ===
def compute_rolling_stats(df_subset):
    stats_list = []
    for fp, group in df_subset.groupby("fingerprint"):
        group = group.sort_values("date").set_index("date")
        rolling = group["relay_bandwidth"].rolling(f"{WINDOW_DAYS}D", min_periods=max(WINDOW_DAYS - 2, 1))
        stats = pd.DataFrame({
            "fingerprint": fp,
            "date": group.index,
            "mean_bw": rolling.mean().values,
            "median_bw": rolling.median().values,
            "std_bw": rolling.std().values,
        })
        stats["cv_bw"] = stats["std_bw"] / stats["mean_bw"]
        stats_list.append(stats)
    return pd.concat(stats_list).reset_index(drop=True)

# === FUNCTION TO AGGREGATE AND ADD PERCENTILE BANDS (10th–90th) ===
def _p10(x):
    v = x.dropna().values
    return np.nanpercentile(v, 10) if len(v) > 0 else np.nan

def _p90(x):
    v = x.dropna().values
    return np.nanpercentile(v, 90) if len(v) > 0 else np.nan

def aggregate_stats(all_stats):
    agg = all_stats.groupby("date").agg({
        "median_bw": ["median", _p10, _p90],
        "std_bw":    ["median", _p10, _p90],
        "cv_bw":     ["median", _p10, _p90],
    })
    agg.columns = ["_".join(col).strip() for col in agg.columns.values]
    agg = agg.reset_index()
    return agg

# === FUNCTION TO PLOT OR SAVE ===
def plot_graph(agg, stat, title, color, filename=None):
    plt.figure(figsize=(10, 6))
    plt.plot(agg["date"], agg[f"{stat}_median"], color=color, label=f"Median {stat.upper()} (7-day rolling)")
    plt.fill_between(
        agg["date"],
        agg[f"{stat}__p10"],
        agg[f"{stat}__p90"],
        color=color,
        alpha=0.2,
        label="10th–90th percentile"
    )
    plt.title(title)
    plt.xlabel("Date (window end)")
    plt.ylabel(stat.upper())
    plt.legend()
    plt.grid(True)
    plt.tight_layout()

    if FOLDER:  # save to folder if specified
        save_path = os.path.join(FOLDER, f"{filename}.png")
        plt.savefig(save_path, dpi=300)
        plt.close()
        print(f"💾 Saved {save_path}")
    else:
        plt.show()

# === ANALYSIS PIPELINE ===
def process_and_plot(label, df_subset, file_prefix):
    print(f"🔹 Computing statistics for {label}...")
    all_stats = compute_rolling_stats(df_subset)
    agg = aggregate_stats(all_stats)

    plot_graph(agg, "median_bw", f"Median Relay Bandwidth ({label})", "blue", f"{file_prefix}_bandwidth")
    plot_graph(agg, "std_bw", f"Median Std Dev of Relay Bandwidth ({label})", "orange", f"{file_prefix}_stddev")
    plot_graph(agg, "cv_bw", f"Median Coefficient of Variation ({label})", "green", f"{file_prefix}_cv")

# === RUN ALL GROUPS ===
process_and_plot("All Relays", daily_bw, "all_relays")

top5_bw = daily_bw[daily_bw["fingerprint"].isin(top5_relays)]
process_and_plot("Top 5% Relays", top5_bw, "top5")

top10_bw = daily_bw[daily_bw["fingerprint"].isin(top10_relays)]
process_and_plot("Top 10% Relays", top10_bw, "top10")

if FOLDER:
    print(f"\n✅ All graphs saved in folder: {FOLDER}")
else:
    print("\n✅ All graphs displayed interactively.")
