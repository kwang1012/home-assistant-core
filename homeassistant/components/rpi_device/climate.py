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
from homeassistant.const import ATTR_TEMPERATURE, CONF_HOST, UnitOfTemperature
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import ConfigEntryNotReady
from homeassistant.helpers import device_registry as dr
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .api.device import RaspberryPiDevice
from .api.discover import Discover
from .api.thermostat import RaspberryPiThermostat
from .entity import RpiEntity


async def async_setup_entry(
    hass: HomeAssistant,
    config_entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up switches."""
    host = config_entry.data[CONF_HOST]
    try:
        device: RaspberryPiDevice = await Discover.discover_single(host)
        if device.device_type != "thermostat":
            return
        device = cast(RaspberryPiThermostat, device)
        async_add_entities([RpiThermostat(device)])
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
        await self.device.set_temperature(temperature)

    async def async_set_hvac_mode(self, hvac_mode: HVACMode) -> None:
        """Set new target hvac mode."""
        await self.device.set_hvac_mode(hvac_mode)

    async def async_set_preset_mode(self, preset_mode: str) -> None:
        """Set new preset mode."""
        await self.device.set_preset_mode(preset_mode)
