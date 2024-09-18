import json
import yaml

def load_config(config_file):
    with open(config_file, 'r') as f:
        return json.load(f)

def main():
    config = load_config("homeassistant/components/rasc/datasets/routines_config.json")
    smart_spaces = config["smart_spaces"]

    devices = {}
    for key, smart_space in smart_spaces.items():
        for device_type, device_count in smart_space["device_types"].items():
            for i in range(device_count):
                if device_type in ("dryer", "toaster", "sprinkler", "mower", "dishwasher", "coffee_machine", "oven", "washer", "microwave"):
                    devices[f"{key}_{device_type}_{i}"] = [{
                        "platform": "timer",
                        "name": f"{key}_{device_type}_{i}",
                        "class": device_type
                    }]
                elif device_type in ("shade", "window", "door"):
                    devices[f"{key}_{device_type}_{i}"] = [{
                        "platform": "cover",
                        "name": f"{key}_{device_type}_{i}",
                        "class": device_type
                    }]
                elif device_type == "thermostat":
                    devices[f"{key}_{device_type}_{i}"] = [{
                        "platform": "climate",
                        "name": f"{key}_{device_type}_{i}",
                    }]
                else:
                    devices[f"{key}_{device_type}_{i}"] = [{
                        "platform": device_type,
                        "name": f"{key}_{device_type}_{i}",
                    }]

    generated_config = {
        "version": 1,
        "devices": devices
    }

    yaml.dump(generated_config, open("homeassistant/components/rasc/datasets/generated.yaml", "w"))

if __name__ == "__main__":
    main()