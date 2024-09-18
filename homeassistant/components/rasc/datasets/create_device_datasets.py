import json
import os

import numpy as np

devices_to_actions = {
    "coffee_machine": {
        "cappuccino": (90, 5),
        "espresso": (30, 5),
        "latte": (90, 5),
        "mocha": (120, 5),
        "americano": (60, 5),
    },
    "dishwasher": {
        "wash": (7 * 60, 60),
        "rinse": (3 * 60, 60),
        "dry": (5 * 50, 60),
    },
    "dryer": {"dry": (10 * 60, 2 * 60)},
    "microwave": {"heat": (90, 1)},
    "mower": {
        "mow": (10 * 60, 2 * 60),
        "return_to_base": (60, 15),
    },
    "oven": {
        "bake": (5 * 60, 1),
        "broil": (2 * 60, 1),
        "roast": (6 * 60, 1),
    },
    "sprinkler": {"water": (5 * 60, 1)},
    "toaster": {"toast": (180, 1)},
    "vacuum": {
        "clean": (7 * 60, 1 * 60),
        "return_to_base": (30, 5),
    },
    "washer": {
        "wash": (30 * 60, 5),
        "rinse": (10 * 60, 3),
        "spin": (5 * 60, 1),
    },
    "window": {
        "open_cover": (5, 1),
        "close_cover": (5, 1),
    },
}

dataset_dir = "homeassistant/components/rasc/datasets"

for device, actions in devices_to_actions.items():
    target_path = f"{dataset_dir}/{device}.json"
    if os.path.exists(target_path):
        continue

    target_json = {}

    for action, (mean, std) in actions.items():
        random_lengths = [mean + std * i * 0.5 for i in range(4, 8)]
        random_lengths += [mean - std * i * 0.5 for i in range(4, 8)]
        random_lengths += [
            round(np.random.normal(mean, std), 3)
            for _ in range(50 - len(random_lengths))
        ]
        np.random.shuffle(random_lengths)
        target_json[action] = random_lengths

    with open(target_path, "w", encoding="utf-8") as file:
        json.dump(target_json, file, indent=4)

    print(f"Created {target_path} dataset.")
