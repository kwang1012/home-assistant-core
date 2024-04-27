"""Support for TPLink HS100/HS110/HS200 smart switch."""
from __future__ import annotations

import logging
from typing import Any, cast

from homeassistant.components.cover import CoverDeviceClass, CoverEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import CONF_HOST
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import ConfigEntryNotReady
from homeassistant.helpers import device_registry as dr
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .api.device import RaspberryPiDevice
from .api.discover import Discover
from .api.door import RaspberryPiDoor
from .entity import RpiEntity

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
        if device.device_type != "door":
            return
        device = cast(RaspberryPiDoor, device)
        async_add_entities([RpiDoor(device)])
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


class RpiDoor(RpiEntity, CoverEntity):
    """Representation of door for Rpi."""

    device: RaspberryPiDoor

    def __init__(self, device: RaspberryPiDoor) -> None:
        """Initialize the Rpi door."""
        super().__init__(device)

        self._attr_device_class = CoverDeviceClass.DOOR
        self._attr_unique_id = f"{self.device.mac}_door"

    @property
    def current_cover_position(self) -> int | None:
        """Return current position of cover.

        None is unknown, 0 is closed, 100 is fully open.
        """
        return self.device.door_position

    @property
    def is_opening(self) -> bool | None:
        """Return if the cover is opening or not."""
        return self.device.is_opening

    @property
    def is_closing(self) -> bool | None:
        """Return if the cover is closing or not."""
        return self.device.is_closing

    @property
    def is_closed(self) -> bool | None:
        """Return if the cover is closed or not."""
        return self.device.is_closed

    async def async_open_cover(self, **kwargs: Any) -> None:
        """Turn the LED switch on."""
        await self.device.open_door()

    async def async_close_cover(self, **kwargs: Any) -> None:
        """Turn the LED switch off."""
        await self.device.close_door()
