"""Rpi device fan api."""

import asyncio
from typing import Any

from .device import RaspberryPiDevice


class RaspberryPiFan(RaspberryPiDevice):
    """RaspberryPiDoor component."""

    FAN_SERVICE = "pi.virtual.fan"
    SET_FAN_METHOD = "transition_fan_state"

    async def get_fan_state(self) -> None:
        """Get shade state."""
        self._state = await self._query_helper(self.FAN_SERVICE, "get_fan_state")

    @property
    def fan_state(self) -> dict[str, Any]:
        """Query the fan state."""
        fan_state: dict[str, Any] = self.sys_info["fan_state"]
        if fan_state is None:
            raise ValueError(
                "The device has no shade_state or you have not called update()"
            )

        return fan_state

    @property
    def is_on(self) -> bool | None:
        """Return true if the entity is on."""
        return (
            self.percentage is not None and self.percentage > 0
        ) or self.preset_mode is not None

    @property
    def percentage(self) -> int | None:
        """Return the current fan speed percentage."""
        fan_state = self.fan_state
        return fan_state.get("percentage")

    @property
    def speed_count(self) -> int:
        """Return the number of speed steps."""
        fan_state = self.fan_state
        return fan_state.get("speed_count", 0)

    @property
    def percentage_step(self) -> float:
        """Return the step size for percentage changes."""
        fan_state = self.fan_state
        return fan_state.get("percentage_step", 1.0)

    @property
    def current_direction(self) -> str | None:
        """Return the current fan direction."""
        fan_state = self.fan_state
        return fan_state.get("direction")

    @property
    def oscillating(self) -> bool | None:
        """Return whether the fan is oscillating."""
        fan_state = self.fan_state
        return fan_state.get("oscillating")

    @property
    def preset_mode(self) -> str | None:
        """Return the current preset mode."""
        fan_state = self.fan_state
        return fan_state.get("preset_mode")

    @property
    def preset_modes(self) -> list[str] | None:
        """Return a list of available preset modes.

        Requires FanEntityFeature.SET_SPEED.
        """
        if hasattr(self, "_attr_preset_modes"):
            return self._attr_preset_modes
        return None

    async def set_percentage(self, percentage: int) -> None:
        """Set the speed of the fan, as a percentage."""

        _state = {"percentage": percentage}

        await self._query_helper(self.FAN_SERVICE, self.SET_FAN_METHOD, _state)

    async def set_preset_mode(self, preset_mode: str) -> None:
        """Set new preset mode."""
        _state = {"preset_mode": preset_mode}

        return await self._query_helper(self.FAN_SERVICE, self.SET_FAN_METHOD, _state)

    async def turn_on(
        self,
        percentage: int | None = None,
        preset_mode: str | None = None,
        **kwargs: Any,
    ) -> None:
        """Turn on the entity."""
        _state = {"on_off": 1}

        return await self._query_helper(self.FAN_SERVICE, self.SET_FAN_METHOD, _state)

    async def turn_off(self, **kwargs: Any) -> None:
        """Turn off the entity."""
        _state = {"on_off": 0}

        return await self._query_helper(self.FAN_SERVICE, self.SET_FAN_METHOD, _state)

    async def set_direction(self, direction: str) -> None:
        """Set the direction of the fan."""
        _state = {"direction": direction}

        return await self._query_helper(self.FAN_SERVICE, self.SET_FAN_METHOD, _state)

    async def oscillate(self, oscillating: bool) -> None:
        """Set oscillation."""
        _state = {"oscillating": oscillating}

        return await self._query_helper(self.FAN_SERVICE, self.SET_FAN_METHOD, _state)


async def main():
    """Run fan device test."""
    device = RaspberryPiFan("127.0.0.1", 9999)
    await device.update()
    await device.turn_on()
    await asyncio.sleep(2)
    await device.update()
    await device.set_percentage(100)
    await asyncio.sleep(2)
    await device.update()


if __name__ == "__main__":
    asyncio.run(main())
