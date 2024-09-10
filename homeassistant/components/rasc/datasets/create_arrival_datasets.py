import numpy as np
import pandas as pd

dataset_dir = "homeassistant/components/rasc/datasets"

headers = ["timestamp", "routineID", "routineName"]

sources = [
    "arrival_morning.csv",
    "arrival_afternoon.csv",
    "arrival_evening.csv",
]

target_dataset = f"{dataset_dir}/arrival_all_bursty.csv"
target_df = pd.DataFrame()

for source in sources:
    source_path = f"{dataset_dir}/{source}"
    source_df = pd.read_csv(source_path, names=headers)
    source_df.loc[0, "timestamp"] += 30
    target_df = pd.concat([target_df, source_df])

target_df.to_csv(target_dataset, index=False)

print(f"Created {target_dataset} dataset.")

target_dataset = f"{dataset_dir}/arrival_all_hybrid.csv"
target_df = pd.DataFrame()

last_source_start = 0
cumsum = 0
for source in sources:
    source_path = f"{dataset_dir}/{source}"
    source_df = pd.read_csv(source_path, names=headers)
    # Randomly select rows to be bursty at the beginning of the period
    bursty_rows = source_df.sample(frac=0.5, random_state=42)
    bursty_rows.loc[0, "timestamp"] += last_source_start + 60 - cumsum
    last_source_start = bursty_rows.loc[0, "timestamp"]

    # Calculate interarrival times for remaining rows
    non_bursty_rows = source_df.drop(bursty_rows.index)
    interarrival_times = np.random.randint(1, 10, size=len(non_bursty_rows))
    non_bursty_rows["timestamp"] = non_bursty_rows["timestamp"] + interarrival_times

    # Concatenate bursty and non-bursty rows
    source_df = pd.concat([bursty_rows, non_bursty_rows])
    cumsum = source_df["timestamp"].cumsum().iloc[-1]
    target_df = pd.concat([target_df, source_df])

target_df.to_csv(target_dataset, index=False)

print(f"Created {target_dataset} dataset.")
