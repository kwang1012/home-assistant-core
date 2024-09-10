import json

import numpy as np

devices_to_actions = {
    "coffee_machine": {
        "cappuccino": (90, 5),
        "espresso": (30, 5),
        "latte": (90, 5),
        "mocha": (120, 5),
        "americano": (60, 5),
    },
    "microwave": {"heat": (90, 1)},
    "toaster": {"toast": (180, 1)},
    "oven": {
        "bake": (5 * 60, 1),
        "broil": (2 * 60, 1),
        "roast": (6 * 60, 1),
    },
    "dishwasher": {
        "wash": (7 * 60, 10),
        "rinse": (3 * 60, 10),
        "dry": (5 * 50, 10),
    },
    "window": {
        "open": (5, 1),
        "close": (5, 1),
    },
    "sprinkler": {"water": (5 * 60, 1)},
    "mower": {"mow": (5 * 60, 3 * 60)},
    "vacuum": {
        "clean": (7 * 60, 3 * 60),
        "dock": (30, 15),
    },
}

dataset_dir = "homeassistant/components/rasc/datasets"

for device, actions in devices_to_actions.items():
    target_path = f"{dataset_dir}/{device}.json"
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
