import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os
import sys

# === CONFIG ===
CSV_PATH = "relay_bandwidths.csv"
WINDOW_DAYS = 7

# === CHECK FOR OUTPUT FOLDER ARGUMENT ===
FOLDER = None
if len(sys.argv) > 1:
    FOLDER = sys.argv[1]
    os.makedirs(FOLDER, exist_ok=True)
    print(f"📁 Output folder set to: {FOLDER}")
else:
    print("⚙️  No folder specified — graphs will be displayed interactively instead of saved.")

# === LOAD DATA ===
df = pd.read_csv(CSV_PATH, parse_dates=["date"])
df = df.sort_values(by=["fingerprint", "date", "hour"])

# === PREPARE DAILY BANDWIDTH ===
daily_bw = df.groupby(["fingerprint", "date"])["relay_bandwidth"].mean().reset_index()

# === IDENTIFY TOP RELAYS BASED ON FIRST DAY ===
first_day = daily_bw["date"].min()
first_day_bw = daily_bw[daily_bw["date"] == first_day]
threshold_5 = first_day_bw["relay_bandwidth"].quantile(0.95)
threshold_10 = first_day_bw["relay_bandwidth"].quantile(0.90)
top5_relays = set(first_day_bw[first_day_bw["relay_bandwidth"] >= threshold_5]["fingerprint"])
top10_relays = set(first_day_bw[first_day_bw["relay_bandwidth"] >= threshold_10]["fingerprint"])

# === FUNCTION TO COMPUTE ROLLING STATS ===
def compute_rolling_stats(df_subset):
    stats_list = []
    for fp, group in df_subset.groupby("fingerprint"):
        group = group.sort_values("date").set_index("date")
        rolling = group["relay_bandwidth"].rolling(f"{WINDOW_DAYS}D", min_periods=WINDOW_DAYS)
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

# === FUNCTION TO AGGREGATE AND ADD IQR ===
def aggregate_stats(all_stats):
    agg = all_stats.groupby("date").agg({
        "median_bw": ["median", lambda x: np.percentile(x, 25), lambda x: np.percentile(x, 75)],
        "std_bw": ["median", lambda x: np.percentile(x, 25), lambda x: np.percentile(x, 75)],
        "cv_bw": ["median", lambda x: np.percentile(x, 25), lambda x: np.percentile(x, 75)],
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
        agg[f"{stat}_<lambda_0>"],
        agg[f"{stat}_<lambda_1>"],
        color=color,
        alpha=0.2,
        label="25th–75th percentile"
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
