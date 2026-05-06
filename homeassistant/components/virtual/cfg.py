"""Handles the file based Virtual configuration.

Virtual seems to need a lot device config so rather than get rid of
the options or clutter up the config flow system I'm adding a text file
where the user can configure things.

There are 2 pieces:

- `BlendedCfg`; this class is responsible for loading the new file based
  configuration and merging it with the flow data and options.

- `UpgradeCfg`; A helper class to import configuration from the old YAML
  layout.
"""

import copy
from datetime import timedelta
import json
import logging
import threading
import uuid

import voluptuous as vol

from homeassistant.const import (
    ATTR_ENTITY_ID,
    CONF_PLATFORM,
    CONF_UNIT_OF_MEASUREMENT,
    Platform,
)
from homeassistant.helpers import config_validation as cv
from homeassistant.util import slugify
from homeassistant.util.yaml import load_yaml, save_yaml

from .const import (
    ATTR_DEVICE_ID,
    ATTR_DEVICES,
    ATTR_FILE_NAME,
    ATTR_GROUP_NAME,
    ATTR_UNIQUE_ID,
    ATTR_VERSION,
    COMPONENT_DOMAIN,
    CONF_CLASS,
    CONF_NAME,
    IMPORTED_GROUP_NAME,
    IMPORTED_YAML_FILE,
    META_JSON_FILE,
)
from .entity import virtual_schema

_LOGGER = logging.getLogger(__name__)

BINARY_SENSOR_DEFAULT_INITIAL_VALUE = "off"
BINARY_SENSOR_SCHEMA = vol.Schema(
    virtual_schema(
        BINARY_SENSOR_DEFAULT_INITIAL_VALUE,
        {
            vol.Optional(CONF_CLASS): cv.string,
        },
    )
)

SENSOR_DEFAULT_INITIAL_VALUE = "0"
SENSOR_SCHEMA = vol.Schema(
    virtual_schema(
        SENSOR_DEFAULT_INITIAL_VALUE,
        {
            vol.Optional(CONF_CLASS): cv.string,
            vol.Optional(CONF_UNIT_OF_MEASUREMENT, default=""): cv.string,
        },
    )
)

DB_LOCK = threading.Lock()


def _fix_value(value):
    """If needed, convert value into a type that can be stored in yaml."""
    if isinstance(value, timedelta):
        return max(value.seconds, 1)
    return value


def _load_meta_data(group_name: str):
    """Read in meta data for a particular group."""
    devices = {}
    with DB_LOCK:
        try:
            with open(META_JSON_FILE, encoding="utf-8") as meta_file:
                devices = json.load(meta_file).get(ATTR_DEVICES, {})
        except Exception as e:  # noqa: BLE001
            _LOGGER.debug("no meta data yet %s", e)
    return devices.get(group_name, {})


def _save_meta_data(group_name, meta_data):
    """Save meta data for a particular group name."""
    with DB_LOCK:
        # Read in current meta data
        devices = {}
        try:
            with open(META_JSON_FILE, encoding="utf-8") as meta_file:
                devices = json.load(meta_file).get(ATTR_DEVICES, {})
        except Exception as e:  # noqa: BLE001
            _LOGGER.debug("no meta data yet %s", e)

        # Update (or add) the group piece.
        _LOGGER.debug("meta before %s", devices)
        devices.update({group_name: meta_data})
        _LOGGER.debug("meta after %s", devices)

        # Write it back out.
        try:
            with open(META_JSON_FILE, "w", encoding="utf-8") as meta_file:
                json.dump({ATTR_VERSION: 1, ATTR_DEVICES: devices}, meta_file, indent=4)
        except Exception as e:  # noqa: BLE001
            _LOGGER.debug("couldn't save meta data %s", e)


def _delete_meta_data(group_name):
    """Save meta data for a particular group name."""
    with DB_LOCK:
        # Read in current meta data
        devices = {}
        try:
            with open(META_JSON_FILE, encoding="utf-8") as meta_file:
                devices = json.load(meta_file).get(ATTR_DEVICES, {})
        except Exception as e:  # noqa: BLE001
            _LOGGER.debug("no meta data yet %s", e)

        # Delete the group piece.
        _LOGGER.debug("meta before %s", devices)
        devices.pop(group_name)
        _LOGGER.debug("meta after %s", devices)

        # Write it back out.
        try:
            with open(META_JSON_FILE, "w", encoding="utf-8") as meta_file:
                json.dump({ATTR_VERSION: 1, ATTR_DEVICES: devices}, meta_file, indent=4)
        except Exception as e:  # noqa: BLE001
            _LOGGER.debug("couldn't save meta data %s", e)


def _save_user_data(file_name, devices):
    try:
        save_yaml(file_name, {ATTR_VERSION: 1, ATTR_DEVICES: devices})
    except Exception as e:  # noqa: BLE001
        _LOGGER.debug("couldn't save user data %s", e)


def _load_user_data(file_name):
    entities = {}
    try:
        entities = load_yaml(file_name).get(ATTR_DEVICES, [])
    except Exception as e:  # noqa: BLE001
        _LOGGER.debug("failed to read virtual file %s", e)
    return entities


def _fix_config(config):
    """Find and return the virtual entries from any platform config."""
    _LOGGER.debug("config=%s", config)
    entries = []
    for entry in config:
        if entry[CONF_PLATFORM] == COMPONENT_DOMAIN:
            entry = copy.deepcopy(entry)
            entry.pop(CONF_PLATFORM)
            entries.append(entry)
    return entries


def _upgrade_name(name: str):
    """We're making the non virtual prefix the default so this flips the naming."""
    if name.startswith("!"):
        return name[1:]
    if name.startswith("virtual_"):
        return f"+{name[8:]}"
    return f"+{name}"


def _parse_old_config(devices, configs, platform):
    """Parse out config into different devices.

    We do several things:
    - insert a platform key/value, i.e, this this is a switch
    - fix the naming
    - create and store the entity under a device, for imported config there
      will only be one entity per device
    """
    for config in configs:
        if not isinstance(config, dict):
            _LOGGER.debug("not dictionary=%s", config)
            continue
        if config[CONF_PLATFORM] != COMPONENT_DOMAIN:
            continue

        # Copy and fix up config.
        config = copy.deepcopy(config)
        config[CONF_PLATFORM] = platform
        config[CONF_NAME] = _upgrade_name(config[CONF_NAME])

        # Fix values that need to be saved in yaml
        config = {k: _fix_value(v) for k, v in config.items()}

        # Insert or create a device for it.
        if config[CONF_NAME] in devices:
            devices[config[CONF_NAME]].append(config)
        else:
            devices[config[CONF_NAME]] = [config]

    return devices


def _make_original_unique_id(name):
    if name.startswith("+"):
        return slugify(name[1:])
    return slugify(name)


def _make_name(name):
    if name.startswith("+"):
        return name[1:]
    return name


def _make_entity_id(platform, name):
    if name.startswith("+"):
        return f"{platform}.{COMPONENT_DOMAIN}_{slugify(name[1:])}"
    return f"{platform}.{slugify(name)}"


def _make_unique_id():
    return f"{uuid.uuid4()}.{COMPONENT_DOMAIN}"


def _make_suffix(platform, device_class):
    """Make a suitable suffix for an unnamed entity.

    Binary sensors, covers and sensors have a class so we append that,
    everything else gets left as-is.
    """
    if platform in [Platform.BINARY_SENSOR, Platform.COVER, Platform.SENSOR]:
        if device_class is None:
            return platform
        return f"{device_class}"
    return ""


class BlendedCfg:
    """Helper class to get at Virtual configuration options.

    Reads in non config flow settings from the external config file and merges
    them with flow data and options.
    """

    def __init__(self, flow_data):
        """Initialize BlendedCfg with flow data."""
        self._group_name = flow_data[ATTR_GROUP_NAME]
        self._file_name = flow_data[ATTR_FILE_NAME]
        self._changed: bool = False

        self._meta_data = {}
        self._orphaned_entities = {}
        self._devices = []
        self._entities = {}

    def _load_meta_data(self):
        return _load_meta_data(self._group_name)

    def _save_meta_data(self):
        _save_meta_data(self._group_name, self._meta_data)
        self._changed = False

    def _load_user_data(self):
        return _load_user_data(self._file_name)

    def load(self):
        """Load and merge meta data and user data into internal state."""
        meta_data = self._load_meta_data()
        devices = self._load_user_data()

        _LOGGER.debug("loaded-meta-data=%s", meta_data)
        _LOGGER.debug("loaded-devices=%s", devices)

        # Let's fix up the devices/entities
        for device_name, entities in devices.items():
            # Create device. One per all entities.
            self._devices.append(
                {ATTR_DEVICE_ID: device_name, CONF_NAME: _make_name(device_name)}
            )

            for entity in entities:
                platform = entity.pop(CONF_PLATFORM)
                device_class = entity.get(CONF_CLASS, None)

                # Figure out the name. We use the one provided and if that isn't
                # there the device name and, optionally, the class.
                name = entity.get(CONF_NAME, None)
                if name is None:
                    name = f"{device_name} {_make_suffix(platform, device_class)}"

                # Look up unique id for this device. If not there this is a new
                # device.
                unique_id = meta_data.get(name, {}).get(ATTR_UNIQUE_ID, None)
                if unique_id is None:
                    _LOGGER.debug("creating %s", name)
                    unique_id = _make_unique_id()
                    meta_data.update(
                        {
                            name: {
                                ATTR_UNIQUE_ID: unique_id,
                                ATTR_ENTITY_ID: _make_entity_id(platform, name),
                            }
                        }
                    )
                    self._changed = True

                # Now copy over the entity id of the device. Not having this is a
                # bug.
                entity_id = meta_data.get(name, {}).get(ATTR_ENTITY_ID, None)
                if entity_id is None:
                    _LOGGER.info("Problem creating %s, no entity id", name)
                    continue

                # Update the entity.
                entity.update(
                    {
                        CONF_NAME: _make_name(name),
                        ATTR_ENTITY_ID: entity_id,
                        ATTR_UNIQUE_ID: unique_id,
                        ATTR_DEVICE_ID: device_name,
                    }
                )
                _LOGGER.debug("added entity %s/%s", platform, entity)

                # Now store in the correct place. Move off temporary meta
                # data list.
                # _LOGGER.debug(f"entities={self._entities}")
                if platform not in self._entities:
                    self._entities[platform] = []
                self._entities[platform].append(entity)
                self._meta_data.update({name: meta_data.pop(name)})

        # Create orphaned list. If we have anything here we need to update
        # the saved meta data.
        for switch, values in meta_data.items():
            values[CONF_NAME] = switch
            self._orphaned_entities.update({values[ATTR_UNIQUE_ID]: values})
            self._changed = True

        # Make sure changes are kept.
        if self._changed:
            self._save_meta_data()

        _LOGGER.debug("meta-data=%s", self._meta_data)
        _LOGGER.debug("devices=%s", self._devices)
        _LOGGER.debug("entities=%s", self._entities)
        _LOGGER.debug("orphaned-entities=%s", self._orphaned_entities)

    def delete(self):
        """Delete meta data for this group."""
        _LOGGER.debug("deleting %s", self._group_name)
        _delete_meta_data(self._group_name)

    @property
    def devices(self):
        """Return the list of devices."""
        return self._devices

    @property
    def entities(self):
        """Return the entity config dict keyed by platform."""
        return self._entities

    @property
    def orphaned_entities(self):
        """Return entities present in meta data but not in user data."""
        return self._orphaned_entities

    @property
    def binary_sensor_config(self):
        """Return binary sensor entity configs."""
        return self._entities.get(Platform.BINARY_SENSOR, [])

    @property
    def sensor_config(self):
        """Return sensor entity configs."""
        return self._entities.get(Platform.SENSOR, [])

    @property
    def switch_config(self):
        """Return switch entity configs."""
        return self._entities.get(Platform.SWITCH, [])


class UpgradeCfg:
    """Read in the old YAML config and convert it to the new format."""

    @staticmethod
    def import_yaml(config):
        """Take the current virtual config and make the new yaml file.

        Virtual needs a lot of fine tuning so rather than get rid of the
        options or clutter up the config flow system I'm adding a text file
        where the user can configure things.
        """

        devices_meta_data = {}
        devices = {}

        # Add in the easily formatted devices.
        for platform in (
            Platform.BINARY_SENSOR,
            Platform.SENSOR,
            Platform.FAN,
            Platform.LIGHT,
            Platform.LOCK,
            Platform.SWITCH,
        ):
            devices = _parse_old_config(
                devices, config.get(platform, []), str(platform)
            )

        # Device tracker is awkward, we have to split it out and fake looking
        # like the other entities.
        all_device_trackers = config.get(Platform.DEVICE_TRACKER, [])
        for device_trackers in all_device_trackers:
            if device_trackers[CONF_PLATFORM] != COMPONENT_DOMAIN:
                continue
            for device_tracker_name in device_trackers.get("devices", []):
                if isinstance(device_tracker_name, dict):
                    device_tracker_name = device_tracker_name[CONF_NAME]
                devices = _parse_old_config(
                    devices,
                    [{CONF_PLATFORM: COMPONENT_DOMAIN, CONF_NAME: device_tracker_name}],
                    str(Platform.DEVICE_TRACKER),
                )

        _LOGGER.info("Devices=%s", devices)

        # Here we have all the original devices we build the meta data.
        # For import
        #  - we can only have one entity per device, which means...
        #  - devices are their own parent
        for name, values in devices.items():
            unique_id = _make_original_unique_id(name)
            entity_id = _make_entity_id(values[0][CONF_PLATFORM], name)

            _LOGGER.debug("uid=%s", unique_id)
            _LOGGER.debug("eid=%s", entity_id)
            devices_meta_data.update(
                {name: {ATTR_UNIQUE_ID: unique_id, ATTR_ENTITY_ID: entity_id}}
            )

        _LOGGER.debug("devices-meta-data=%s", devices_meta_data)

        _save_user_data(IMPORTED_YAML_FILE, devices)
        _save_meta_data(IMPORTED_GROUP_NAME, devices_meta_data)

    @staticmethod
    def create_flow_data(_config):
        """Take the current aarlo config and make the new flow configuration."""
        return {
            ATTR_GROUP_NAME: IMPORTED_GROUP_NAME,
            ATTR_FILE_NAME: IMPORTED_YAML_FILE,
        }
