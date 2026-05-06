"""Rpi device door api."""

import asyncio
from typing import Any

from homeassistant.components.light import ColorMode

from .device import RaspberryPiDevice


class RaspberryPiLight(RaspberryPiDevice):
    """RaspberryPiLight component."""

    LIGHT_SERVICE = "pi.virtual.light"
    SET_LIGHT_METHOD = "transition_light_state"

    async def get_light_state(self) -> None:
        """Get light state."""
        self._state = await self._query_helper(self.LIGHT_SERVICE, "get_light_state")

    @property
    def light_state(self) -> dict[str, Any]:
        """Query the light state."""
        light_state: dict[str, Any] = self.sys_info["light_state"]
        if light_state is None:
            raise ValueError(
                "The device has no door_state or you have not called update()"
            )

        return light_state

    @property
    def is_on(self) -> bool | None:
        """Return true if the entity is on."""
        light_state = self.light_state
        return bool(light_state.get("is_on"))

    @property
    def brightness(self) -> int | None:
        """Return the brightness of this light between 0..255."""
        light_state = self.light_state
        return light_state.get("brightness")

    @property
    def color_mode(self) -> ColorMode | None:
        """Return the color mode of the light."""
        light_state = self.light_state
        return light_state.get("color_mode")

    @property
    def hs_color(self) -> tuple[float, float] | None:
        """Return the hue and saturation color value [float, float]."""
        light_state = self.light_state
        return light_state.get("hs_color")

    @property
    def xy_color(self) -> tuple[float, float] | None:
        """Return the xy color value [float, float]."""
        light_state = self.light_state
        return light_state.get("xy_color")

    @property
    def rgb_color(self) -> tuple[int, int, int] | None:
        """Return the rgb color value [int, int, int]."""
        light_state = self.light_state
        return light_state.get("rgb_color")

    @property
    def rgbw_color(self) -> tuple[int, int, int, int] | None:
        """Return the rgbw color value [int, int, int, int]."""
        light_state = self.light_state
        return light_state.get("rgbw_color")

    @property
    def rgbww_color(self) -> tuple[int, int, int, int, int] | None:
        """Return the rgbww color value [int, int, int, int, int]."""
        light_state = self.light_state
        return light_state.get("rgbww_color")

    @property
    def color_temp(self) -> int | None:
        """Return the CT color value in mireds."""
        light_state = self.light_state
        return light_state.get("color_temp")

    @property
    def color_temp_kelvin(self) -> int | None:
        """Return the CT color value in Kelvin."""
        light_state = self.light_state
        return light_state.get("color_temp_kelvin")

    @property
    def min_mireds(self) -> int:
        """Return the coldest color_temp that this light supports."""
        light_state = self.light_state
        return light_state.get("min_mireds", 153)

    @property
    def max_mireds(self) -> int:
        """Return the warmest color_temp that this light supports."""
        light_state = self.light_state
        return light_state.get("max_mireds", 500)

    @property
    def min_color_temp_kelvin(self) -> int:
        """Return the warmest color_temp_kelvin that this light supports."""
        light_state = self.light_state
        return light_state.get("min_color_temp_kelvin", 2000)

    @property
    def max_color_temp_kelvin(self) -> int:
        """Return the coldest color_temp_kelvin that this light supports."""
        light_state = self.light_state
        return light_state.get("max_color_temp_kelvin", 6535)

    @property
    def effect_list(self) -> list[str] | None:
        """Return the list of supported effects."""
        light_state = self.light_state
        return light_state.get("effect_list")

    @property
    def effect(self) -> str | None:
        """Return the current effect."""
        light_state = self.light_state
        return light_state.get("effect")

    async def turn_on(self, **kwargs):
        """Turn the light on."""
        _state = {"on_off": 1, **kwargs}

        return await self._query_helper(
            self.LIGHT_SERVICE, self.SET_LIGHT_METHOD, _state
        )

    async def turn_off(self, **kwargs):
        """Turn the light off."""
        _state = {"on_off": 0, **kwargs}

        return await self._query_helper(
            self.LIGHT_SERVICE, self.SET_LIGHT_METHOD, _state
        )


async def main():
    """Run light device test."""
    device = RaspberryPiLight("127.0.0.1", 9999)
    await device.update()
    await device.turn_on(brightness=100)
    await asyncio.sleep(2)
    await device.update()


if __name__ == "__main__":
    asyncio.run(main())
