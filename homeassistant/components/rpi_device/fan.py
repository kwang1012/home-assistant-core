"""Support for TPLink HS100/HS110/HS200 smart switch."""

import logging
from typing import Any

from homeassistant.components.fan import FanEntity, FanEntityFeature
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddConfigEntryEntitiesCallback

from .api.fan import RaspberryPiFan
from .const import DOMAIN
from .entity import RpiEntity

_LOGGER = logging.getLogger(__name__)


async def async_setup_entry(
    hass: HomeAssistant,
    config_entry: ConfigEntry,
    async_add_entities: AddConfigEntryEntitiesCallback,
) -> None:
    """Set up fans."""
    devices: list[RaspberryPiFan] = hass.data[DOMAIN][config_entry.entry_id].get(  # pylint: disable=hass-use-runtime-data
        "fan", []
    )
    async_add_entities([RpiFan(device) for device in devices])


class RpiFan(RpiEntity, FanEntity):
    """Representation of door for Rpi."""

    device: RaspberryPiFan

    def __init__(self, device: RaspberryPiFan) -> None:
        """Initialize the Rpi fan."""
        super().__init__(device)

        self._attr_supported_features = (
            FanEntityFeature.SET_SPEED
            | FanEntityFeature.OSCILLATE
            | FanEntityFeature.DIRECTION
            | FanEntityFeature.TURN_ON
            | FanEntityFeature.TURN_OFF
        )

    @property
    def is_on(self) -> bool | None:
        """Return true if the entity is on."""
        return self.device.is_on

    @property
    def percentage(self) -> int | None:
        """Return the current speed as a percentage."""
        return self.device.percentage

    @property
    def speed_count(self) -> int:
        """Return the number of speeds the fan supports."""
        return self.device.speed_count

    @property
    def percentage_step(self) -> float:
        """Return the step size for percentage."""
        return self.device.percentage_step

    @property
    def current_direction(self) -> str | None:
        """Return the current direction of the fan."""
        return self.device.current_direction

    @property
    def oscillating(self) -> bool | None:
        """Return whether or not the fan is currently oscillating."""
        return self.device.oscillating

    @property
    def preset_mode(self) -> str | None:
        """Return the current preset mode, e.g., auto, smart, interval, favorite.

        Requires FanEntityFeature.SET_SPEED.
        """
        return self.device.preset_mode

    @property
    def preset_modes(self) -> list[str] | None:
        """Return a list of available preset modes.

        Requires FanEntityFeature.SET_SPEED.
        """
        return self.device.preset_modes

    async def async_set_percentage(self, percentage: int) -> None:
        """Set the speed of the fan, as a percentage."""
        await self.device.set_percentage(percentage)

    async def async_set_preset_mode(self, preset_mode: str) -> None:
        """Set new preset mode."""
        await self.device.set_preset_mode(preset_mode)

    async def async_turn_on(
        self,
        percentage: int | None = None,
        preset_mode: str | None = None,
        **kwargs: Any,
    ) -> None:
        """Turn on the entity."""
        await self.device.turn_on(percentage, preset_mode, **kwargs)

    async def async_turn_off(self, **kwargs: Any) -> None:
        """Turn off the entity."""
        await self.device.turn_off()

    async def async_set_direction(self, direction: str) -> None:
        """Set the direction of the fan."""
        await self.device.set_direction(direction)

    async def async_oscillate(self, oscillating: bool) -> None:
        """Set oscillation."""
        await self.device.oscillate(oscillating)
