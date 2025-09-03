# tor-relay-data

## Summary
This project collects and analyzes Tor relay bandwidth data using [CollecTor](https://metrics.torproject.org/collector.html).  
It pulls one year of relay data by selecting one reading per relay at the same hour each day.  
The data is stored in a CSV with the following format:

date, fingerprint, relay_bandwidth, timestamp

This dataset is then processed to compute variation statistics (Coefficient of Variation and Standard Deviation) across relays.  
The ultimate goal is to generate graphs showing how relay stability evolves over time.

### Analysis Workflow
1. **Data Collection**  
   - Fetch one year of consensus files (365 total).  
   - For each day, extract advertised bandwidth values for all relays at the same hour.  
   - Take the intersection of relays that appear in all daily snapshots.  

2. **Computation**  
   - Apply a 7-day sliding window.  
   - Compute Coefficient of Variation (CoV) for each relay across that window.  

3. **Visualization**  
   - X-axis: Week number (or raw timestamps).  
   - Y-axis: Median of all relays’ CoV values for that week.  

---

## Setting up Virtual Environment

From the project root:

```shell
# Create a virtual environment
python3 -m venv venv

# Activate it
source venv/bin/activate   # macOS/Linux
venv\Scripts\activate      # Windows PowerShell

# Verify you're using the venv's Python
which python
```

Install Requirements 

```shell 
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

---

## Data Collection 

`pull_relay_data.py` will use collecTor to pull a set timeframe amount of relay data for a specific hour of the day and put it into a csv of your choosing.

How to run:
```shell
python pull_relay_data.py --start 2024-09-01 --end 2024-09-07 --hour 0 --out daily_bw.csv
```


