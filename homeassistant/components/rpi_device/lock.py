"""Support for TPLink HS100/HS110/HS200 smart switch."""

import logging
from typing import Any

from homeassistant.components.lock import LockEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddConfigEntryEntitiesCallback

from .api.lock import RaspberryPiLock
from .const import DOMAIN
from .entity import RpiEntity

_LOGGER = logging.getLogger(__name__)


async def async_setup_entry(
    hass: HomeAssistant,
    config_entry: ConfigEntry,
    async_add_entities: AddConfigEntryEntitiesCallback,
) -> None:
    """Set up fans."""
    devices: list[RaspberryPiLock] = hass.data[DOMAIN][config_entry.entry_id].get(  # pylint: disable=hass-use-runtime-data
        "lock", []
    )
    async_add_entities([RpiLock(device) for device in devices])


class RpiLock(RpiEntity, LockEntity):
    """Representation of door for Rpi."""

    device: RaspberryPiLock

    @property
    def changed_by(self) -> str | None:
        """Last change triggered by."""
        return self.device.changed_by

    @property
    def code_format(self) -> str | None:
        """Regex for code format or None if no code is required."""
        return self.device.code_format

    @property
    def is_locked(self) -> bool | None:
        """Return true if the lock is locked."""
        return self.device.is_locked

    @property
    def is_locking(self) -> bool | None:
        """Return true if the lock is locking."""
        return self.device.is_locking

    @property
    def is_unlocking(self) -> bool | None:
        """Return true if the lock is unlocking."""
        return self.device.is_unlocking

    @property
    def is_jammed(self) -> bool | None:
        """Return true if the lock is jammed (incomplete locking)."""
        return self.device.is_jammed

    async def async_lock(self, **kwargs: Any) -> None:
        """Lock the lock."""
        await self.device.lock(**kwargs)

    async def async_unlock(self, **kwargs: Any) -> None:
        """Unlock the lock."""
        await self.device.unlock(**kwargs)

    async def async_open(self, **kwargs: Any) -> None:
        """Open the door latch."""
        await self.device.open(**kwargs)
