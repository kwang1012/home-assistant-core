"""Rpi device door api."""

import asyncio
from typing import Any

from .device import RaspberryPiDevice


class RaspberryPiSwitch(RaspberryPiDevice):
    """RaspberryPiSwitch component."""

    SWITCH_SERVICE = "pi.virtual.switch"
    SET_SWITCH_METHOD = "transition_switch_state"

    async def get_switch_state(self) -> None:
        """Get switch state."""
        self._state = await self._query_helper(self.SWITCH_SERVICE, "get_switch_state")

    @property
    def switch_state(self) -> dict[str, str]:
        """Query the switch state."""
        switch_state = self.sys_info["switch_state"]
        if switch_state is None:
            raise ValueError(
                "The device has no switch_state or you have not called update()"
            )

        return switch_state

    @property
    def is_on(self) -> bool:
        """Return True is the switch is on."""
        switch_state = self.switch_state
        return bool(switch_state.get("is_on"))

    async def turn_on(self, **kwargs: Any) -> None:
        """Turn on the switch."""
        _state = {"on_off": 1}

        return await self._query_helper(
            self.SWITCH_SERVICE, self.SET_SWITCH_METHOD, _state
        )

    async def turn_off(self, **kwargs: Any) -> None:
        """Turn off the switch."""
        _state = {"on_off": 0}

        return await self._query_helper(
            self.SWITCH_SERVICE, self.SET_SWITCH_METHOD, _state
        )


async def main():
    """Run switch device test."""
    device = RaspberryPiSwitch("127.0.0.1", 9999)
    await device.update()
    await device.turn_on()
    await device.update()


if __name__ == "__main__":
    asyncio.run(main())
