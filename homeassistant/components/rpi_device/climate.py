"""Provide support for a rpi thermostat."""
from __future__ import annotations

from typing import Any, cast

from homeassistant.components.climate import (
    PRESET_NONE,
    ClimateEntity,
    ClimateEntityFeature,
    HVACMode,
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import ATTR_TEMPERATURE, UnitOfTemperature
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .api.device import RaspberryPiDevice
from .api.thermostat import RaspberryPiThermostat
from .const import DOMAIN
from .entity import RpiEntity


async def async_setup_entry(
    hass: HomeAssistant,
    config_entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up thermostat."""
    device: RaspberryPiDevice = hass.data[DOMAIN][config_entry.entry_id]
    if device.is_thermostat:
        async_add_entities([RpiThermostat(cast(RaspberryPiThermostat, device))])


class RpiThermostat(RpiEntity, ClimateEntity):
    """Representation of door for Rpi."""

    device: RaspberryPiThermostat
    _attr_hvac_modes = [HVACMode.HEAT, HVACMode.OFF]
    _attr_preset_modes = [PRESET_NONE]

    _attr_min_temp = 62.0
    _attr_max_temp = 85.0

    def __init__(self, device: RaspberryPiThermostat) -> None:
        """Initialize the Virtual thermostat device."""
        super().__init__(device)

        self._attr_temperature_unit = UnitOfTemperature.FAHRENHEIT
        self._attr_supported_features = (
            ClimateEntityFeature.PRESET_MODE | ClimateEntityFeature.TARGET_TEMPERATURE
        )

        self._attr_unique_id = f"{self.device.mac}_thermostat"

    @property
    def is_on(self) -> bool:
        """Return true if thermostat is on."""
        return self.device.hvac_mode != HVACMode.OFF

    @property
    def current_temperature(self):
        """Return current temperature."""
        return self.device.current_temperature

    @property
    def target_temperature(self):
        """Return target temperature."""
        return self.device.target_temperature

    @property
    def hvac_mode(self):
        """Return HVAC mode."""
        return self.device.hvac_mode

    @property
    def preset_mode(self):
        """Return preset mode."""
        return self.device.preset_mode

    async def async_set_temperature(self, **kwargs: Any) -> None:
        """Set new target temperature."""
        temperature = kwargs[ATTR_TEMPERATURE]
        del kwargs[ATTR_TEMPERATURE]
        await self.device.set_temperature(temperature, **kwargs)

    async def async_set_hvac_mode(self, hvac_mode: HVACMode) -> None:
        """Set new target hvac mode."""
        await self.device.set_hvac_mode(hvac_mode)

    async def async_set_preset_mode(self, preset_mode: str) -> None:
        """Set new preset mode."""
        await self.device.set_preset_mode(preset_mode)
