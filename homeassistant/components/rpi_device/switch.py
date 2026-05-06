"""Support for TPLink HS100/HS110/HS200 smart switch."""

import logging
from typing import Any

from homeassistant.components.switch import SwitchEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddConfigEntryEntitiesCallback

from .api.switch import RaspberryPiSwitch
from .const import DOMAIN
from .entity import RpiEntity

_LOGGER = logging.getLogger(__name__)


async def async_setup_entry(
    hass: HomeAssistant,
    config_entry: ConfigEntry,
    async_add_entities: AddConfigEntryEntitiesCallback,
) -> None:
    """Set up switches."""
    devices: list[RaspberryPiSwitch] = hass.data[DOMAIN][config_entry.entry_id].get(  # pylint: disable=hass-use-runtime-data
        "switch", []
    )
    async_add_entities([RpiSwitch(device) for device in devices])


class RpiSwitch(RpiEntity, SwitchEntity):
    """Representation of door for Rpi."""

    device: RaspberryPiSwitch

    @property
    def is_on(self) -> bool | None:
        """Return true if the entity is on."""
        return self.device.is_on

    async def async_turn_on(self, **kwargs: Any) -> None:
        """Turn on the entity."""
        await self.device.turn_on(**kwargs)

    async def async_turn_off(self, **kwargs: Any) -> None:
        """Turn off the entity."""
        await self.device.turn_off(**kwargs)
