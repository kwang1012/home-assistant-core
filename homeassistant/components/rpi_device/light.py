"""Support for TPLink HS100/HS110/HS200 smart switch."""
from __future__ import annotations

import logging

from homeassistant.config_entries import ConfigEntry
from homeassistant.const import CONF_HOST
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import ConfigEntryNotReady
from homeassistant.helpers import device_registry as dr
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .api.device import RaspberryPiDevice
from .api.discover import Discover

_LOGGER = logging.getLogger(__name__)


async def async_setup_entry(
    hass: HomeAssistant,
    config_entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up switches."""
    host = config_entry.data[CONF_HOST]
    try:
        device: RaspberryPiDevice = await Discover.discover_single(host)
        if device.device_type != "light":
            return
        # device = cast(RaspberryPiDoor, device)
        # async_add_entities([RpiDoor(device)])
    except ValueError as ex:
        raise ConfigEntryNotReady from ex

    found_mac = dr.format_mac(device.mac)
    if found_mac != config_entry.unique_id:
        # If the mac address of the device does not match the unique_id
        # of the config entry, it likely means the DHCP lease has expired
        # and the device has been assigned a new IP address. We need to
        # wait for the next discovery to find the device at its new address
        # and update the config entry so we do not mix up devices.
        raise ConfigEntryNotReady(
            f"Unexpected device found at {host}; expected {config_entry.unique_id}, found {found_mac}"
        )
