"""Support for TPLink HS100/HS110/HS200 smart switch."""

from __future__ import annotations

import logging
from typing import Any

from homeassistant.components.light import ColorMode, LightEntity, LightEntityFeature
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddConfigEntryEntitiesCallback

from .api.light import RaspberryPiLight
from .const import DOMAIN
from .entity import RpiEntity

_LOGGER = logging.getLogger(__name__)


async def async_setup_entry(
    hass: HomeAssistant,
    config_entry: ConfigEntry,
    async_add_entities: AddConfigEntryEntitiesCallback,
) -> None:
    """Set up lights."""
    devices: list[RaspberryPiLight] = hass.data[DOMAIN][config_entry.entry_id].get(  # pylint: disable=hass-use-runtime-data
        "light", []
    )
    async_add_entities([RpiLight(device) for device in devices])


class RpiLight(RpiEntity, LightEntity):
    """Representation of door for Rpi."""

    device: RaspberryPiLight

    def __init__(self, device: RaspberryPiLight) -> None:
        """Initialize the Rpi door."""
        super().__init__(device)

        self._attr_supported_color_modes = {
            ColorMode.ONOFF,
            ColorMode.BRIGHTNESS,
            ColorMode.HS,
            ColorMode.COLOR_TEMP,
        }
        self._attr_supported_features = (
            LightEntityFeature.EFFECT | LightEntityFeature.TRANSITION
        )
        self._attr_transition: bool | None = None
        self._attr_effect_list = ["rainbow", "none"]

    @property
    def is_on(self) -> bool | None:
        """Return true if the light is on."""
        return self.device.is_on

    @property
    def brightness(self) -> int | None:
        """Return the brightness of this light between 0..255."""
        return self.device.brightness

    @property
    def color_mode(self) -> ColorMode | None:
        """Return the color mode of the light."""
        return self.device.color_mode

    @property
    def hs_color(self) -> tuple[float, float] | None:
        """Return the hue and saturation color value [float, float]."""
        return self.device.hs_color

    @property
    def xy_color(self) -> tuple[float, float] | None:
        """Return the xy color value [float, float]."""
        return self.device.xy_color

    @property
    def rgb_color(self) -> tuple[int, int, int] | None:
        """Return the rgb color value [int, int, int]."""
        return self.device.rgb_color

    @property
    def rgbw_color(self) -> tuple[int, int, int, int] | None:
        """Return the rgbw color value [int, int, int, int]."""
        return self.device.rgbw_color

    @property
    def rgbww_color(self) -> tuple[int, int, int, int, int] | None:
        """Return the rgbww color value [int, int, int, int, int]."""
        return self.device.rgbww_color

    @property
    def color_temp(self) -> int | None:
        """Return the CT color value in mireds."""
        return self.device.color_temp

    @property
    def color_temp_kelvin(self) -> int | None:
        """Return the CT color value in Kelvin."""
        return self.device.color_temp_kelvin

    @property
    def min_mireds(self) -> int:
        """Return the coldest color_temp that this light supports."""
        return self.device.min_mireds

    @property
    def max_mireds(self) -> int:
        """Return the warmest color_temp that this light supports."""
        return self.device.max_mireds

    @property
    def min_color_temp_kelvin(self) -> int:
        """Return the warmest color_temp_kelvin that this light supports."""
        return self.device.min_color_temp_kelvin

    @property
    def max_color_temp_kelvin(self) -> int:
        """Return the coldest color_temp_kelvin that this light supports."""
        return self.device.max_color_temp_kelvin

    @property
    def effect_list(self) -> list[str] | None:
        """Return the list of supported effects."""
        return self.device.effect_list

    @property
    def effect(self) -> str | None:
        """Return the current effect."""
        return self.device.effect

    async def async_turn_on(self, **kwargs: Any) -> None:
        """Turn the light on."""
        await self.device.turn_on(**kwargs)

    async def async_turn_off(self, **kwargs: Any) -> None:
        """Turn the light off."""
        await self.device.turn_off(**kwargs)
