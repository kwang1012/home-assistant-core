"""Support for TPLink HS100/HS110/HS200 smart switch."""
from __future__ import annotations

import logging
from typing import Any, cast

from homeassistant.components.cover import CoverDeviceClass, CoverEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .api.device import RaspberryPiDevice
from .api.door import RaspberryPiDoor
from .api.shade import RaspberryPiShade
from .const import DOMAIN
from .entity import RpiEntity

_LOGGER = logging.getLogger(__name__)


async def async_setup_entry(
    hass: HomeAssistant,
    config_entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up switches."""
    device: RaspberryPiDevice = hass.data[DOMAIN][config_entry.entry_id]
    if device.is_door:
        async_add_entities([RpiDoor(cast(RaspberryPiDoor, device))])
    elif device.is_shade:
        async_add_entities([RpiShade(cast(RaspberryPiShade, device))])


class RpiShade(RpiEntity, CoverEntity):
    """Representation of door for Rpi."""

    device: RaspberryPiShade

    def __init__(self, device: RaspberryPiShade) -> None:
        """Initialize the Rpi door."""
        super().__init__(device)

        self._attr_device_class = CoverDeviceClass.SHADE
        self._attr_unique_id = f"{self.device.mac}_shade"

    @property
    def current_cover_position(self) -> int | None:
        """Return current position of cover.

        None is unknown, 0 is closed, 100 is fully open.
        """
        return self.device.shade_position

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
        await self.device.open_shade()

    async def async_close_cover(self, **kwargs: Any) -> None:
        """Turn the LED switch off."""
        await self.device.close_shade()


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
