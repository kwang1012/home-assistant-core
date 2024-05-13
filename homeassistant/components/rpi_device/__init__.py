"""The rpi_camera component."""
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import CONF_HOST
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import ConfigEntryNotReady
from homeassistant.helpers import config_validation as cv, device_registry as dr
from homeassistant.helpers.typing import ConfigType

from .api.device import RaspberryPiDevice
from .api.discover import Discover
from .const import DOMAIN, PLATFORMS

CONFIG_SCHEMA = cv.config_entry_only_config_schema(DOMAIN)


def setup(hass: HomeAssistant, config: ConfigType) -> bool:
    """Set up the rpi_camera integration."""
    hass.data[DOMAIN] = {}

    return True


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up Rpi device from a config entry."""
    host = entry.data[CONF_HOST]
    try:
        device: RaspberryPiDevice = await Discover.discover_single(host)
    except ValueError as ex:
        raise ConfigEntryNotReady from ex

    found_mac = dr.format_mac(device.mac)
    if found_mac != entry.unique_id:
        # If the mac address of the device does not match the unique_id
        # of the config entry, it likely means the DHCP lease has expired
        # and the device has been assigned a new IP address. We need to
        # wait for the next discovery to find the device at its new address
        # and update the config entry so we do not mix up devices.
        raise ConfigEntryNotReady(
            f"Unexpected device found at {host}; expected {entry.unique_id}, found {found_mac}"
        )

    hass.data[DOMAIN][entry.entry_id] = device
    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)

    return True
