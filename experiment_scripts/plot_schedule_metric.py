import os

import matplotlib.pyplot as plt
import numpy as np
import yaml


def main():

    results = [f for f in os.listdir("results") if not os.path.isfile(os.path.join("results", f))]
    results = sorted(results)

    schedule_metrics_dict = {}

    for result in results:
        with open(f"results/{result}/rasc_config.yaml") as file:
            config = yaml.load(file, Loader=yaml.FullLoader)

        if not os.path.exists(f"results/{result}/schedule_metrics.yaml"):
            continue

        with open(f"results/{result}/schedule_metrics.yaml") as file:
            schedule_metrics = yaml.load(file, Loader=yaml.FullLoader)

        policy = config["scheduling_policy"]

        if policy == "tl":
            policy = "tl-" + config["rescheduling_policy"]

        if policy not in schedule_metrics_dict:
            schedule_metrics_dict[policy] = []
        schedule_metrics_dict[policy] = schedule_metrics

    headers = []
    data = {
        key: []
        for key in schedule_metrics_dict["fcfs"]
    }
    for policy, schedule_metrics in schedule_metrics_dict.items():
        headers.append(policy)
        for key, metric in schedule_metrics.items():
            data[key].append(metric)

    X = np.arange(len(headers))
    for i, (key, values) in enumerate(data.items()):
        plt.bar(X + 0.1 * i, values, width=0.1, label=key)

    plt.ylim((0, 100))
    plt.legend(ncols=2)
    plt.xticks(X + 0.2, headers)
    plt.savefig("schedule_metric.pdf")

if __name__ == "__main__":
    main()
