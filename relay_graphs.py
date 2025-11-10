import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

# === CONFIG ===
CSV_PATH = "relay_bandwidths.csv"
WINDOW_DAYS = 7

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

# === FUNCTION TO COMPUTE STATS ===
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

# === FUNCTION TO PLOT ===
def plot_stat(agg, stat, title, color):
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
    plt.show()

# === ALL RELAYS ===
print("Computing statistics for all relays...")
all_stats = compute_rolling_stats(daily_bw)
agg_all = aggregate_stats(all_stats)

plot_stat(agg_all, "median_bw", "Median Relay Bandwidth (All Relays)", "blue")
plot_stat(agg_all, "std_bw", "Median Std Dev of Relay Bandwidth (All Relays)", "orange")
plot_stat(agg_all, "cv_bw", "Median Coefficient of Variation (All Relays)", "green")

# === TOP 5% RELAYS ===
print("Computing statistics for top 5% relays...")
top5_bw = daily_bw[daily_bw["fingerprint"].isin(top5_relays)]
all_stats_top5 = compute_rolling_stats(top5_bw)
agg_top5 = aggregate_stats(all_stats_top5)

plot_stat(agg_top5, "median_bw", "Median Relay Bandwidth (Top 5% Relays)", "blue")
plot_stat(agg_top5, "std_bw", "Median Std Dev of Relay Bandwidth (Top 5% Relays)", "orange")
plot_stat(agg_top5, "cv_bw", "Median Coefficient of Variation (Top 5% Relays)", "green")

# === TOP 10% RELAYS ===
print("Computing statistics for top 10% relays...")
top10_bw = daily_bw[daily_bw["fingerprint"].isin(top10_relays)]
all_stats_top10 = compute_rolling_stats(top10_bw)
agg_top10 = aggregate_stats(all_stats_top10)

plot_stat(agg_top10, "median_bw", "Median Relay Bandwidth (Top 10% Relays)", "blue")
plot_stat(agg_top10, "std_bw", "Median Std Dev of Relay Bandwidth (Top 10% Relays)", "orange")
plot_stat(agg_top10, "cv_bw", "Median Coefficient of Variation (Top 10% Relays)", "green")

print("✅ Done.")
