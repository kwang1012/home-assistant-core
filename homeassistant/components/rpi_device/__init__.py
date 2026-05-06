"""The rpi_camera component."""

import voluptuous as vol

from homeassistant.config_entries import SOURCE_IMPORT, ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import ConfigEntryNotReady
import homeassistant.helpers.config_validation as cv
import homeassistant.helpers.device_registry as dr
from homeassistant.helpers.typing import ConfigType

from .api.device import RaspberryPiDevice
from .api.discover import Discover
from .const import ATTR_NODES_FILE, DOMAIN, PLATFORMS

CONFIG_SCHEMA = vol.Schema(
    {
        DOMAIN: vol.Schema(
            {
                vol.Optional(ATTR_NODES_FILE): cv.string,
            }
        )
    },
    extra=vol.ALLOW_EXTRA,
)


async def _async_get_or_create_rpi_device_in_registry(
    hass: HomeAssistant, entry: ConfigEntry, device: RaspberryPiDevice
) -> None:
    device_registry = dr.async_get(hass)
    device_registry.async_get_or_create(
        config_entry_id=entry.entry_id,
        identifiers={(DOMAIN, device.unique_id)},
        manufacturer="RASC group",
        model=device.model,
        name=device.sys_info["dev_name"],
        sw_version=device.hw_info["sw_ver"],
        hw_version=device.hw_info["hw_ver"],
    )


async def async_setup(hass: HomeAssistant, config: ConfigType) -> bool:
    """Set up the rpi_device integration."""
    if DOMAIN not in config:
        return True

    hass.data[DOMAIN] = {}  # pylint: disable=hass-use-runtime-data
    # Only create a config entry if none exists
    if not hass.config_entries.async_entries(DOMAIN):
        hass.async_create_task(
            hass.config_entries.flow.async_init(
                DOMAIN,
                context={"source": SOURCE_IMPORT},
                data=config[DOMAIN],
            )
        )

    return True


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up Rpi device from a config entry."""
    nodes_file = entry.data.get(ATTR_NODES_FILE, None)
    if nodes_file is None:
        return True

    def _read_nodes_file() -> list[str]:
        with open(nodes_file, encoding="utf-8") as f:
            return f.readlines()

    hosts = await hass.async_add_executor_job(_read_nodes_file)
    try:
        devices: list[RaspberryPiDevice] = await Discover.discover_all(hosts)
    except ValueError as ex:
        raise ConfigEntryNotReady from ex

    hass.data[DOMAIN][entry.entry_id] = {}  # pylint: disable=hass-use-runtime-data
    for device in devices:
        await _async_get_or_create_rpi_device_in_registry(hass, entry, device)
        if device.device_type not in hass.data[DOMAIN][entry.entry_id]:  # pylint: disable=hass-use-runtime-data
            hass.data[DOMAIN][entry.entry_id][device.device_type] = []  # pylint: disable=hass-use-runtime-data
        hass.data[DOMAIN][entry.entry_id][device.device_type].append(device)  # pylint: disable=hass-use-runtime-data

    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)

    return True


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload a config entry."""
    unload_ok = await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
    if unload_ok:
        hass.data[DOMAIN].pop(entry.entry_id)  # pylint: disable=hass-use-runtime-data

    return unload_ok
