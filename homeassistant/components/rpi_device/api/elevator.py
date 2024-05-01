"""Rpi device door api."""
from typing import Any, Optional

from .device import RaspberryPiDevice


class RaspberryPiShade(RaspberryPiDevice):
    """RaspberryPiDoor component."""

    SHADE_SERVICE = "pi.virtual.shade"
    SET_SHADE_METHOD = "transition_shade_state"

    def __init__(self, host, port=None) -> None:
        """Initialize RaspberryPiDoor api."""
        super().__init__(host, port)
        self._state: dict[str, Any] = {}

    async def get_shade_state(self) -> None:
        """Get shade state."""
        self._state = await self._query_helper(self.SHADE_SERVICE, "get_shade_state")

    @property
    def shade_state(self) -> dict[str, str]:
        """Query the shade state."""
        shade_state = self.sys_info["shade_state"]
        if shade_state is None:
            raise ValueError(
                "The device has no shade_state or you have not called update()"
            )

        return shade_state

    @property
    def is_closed(self) -> bool:
        """Return True is the shade is closed."""
        shade_state = self.shade_state
        return bool(shade_state.get("closed"))

    @property
    def shade_position(self):
        """Return current shade position."""
        shade_state = self.shade_state
        return shade_state.get("current_position")

    @property
    def is_opening(self):
        """Return True is the shade is opening."""
        shade_state = self.shade_state
        return shade_state.get("opening")

    @property
    def is_closing(self):
        """Return True is the shade is closing."""
        shade_state = self.shade_state
        return shade_state.get("closing")

    async def open_shade(self, transition: Optional[int] = None):
        """Open the shade."""
        _state = {"on_off": 1}

        if transition is not None:
            _state["transition_period"] = transition

        shade_state = await self._query_helper(
            self.SHADE_SERVICE, self.SET_SHADE_METHOD, _state
        )

        return shade_state

    async def close_shade(self, transition: Optional[int] = None):
        """Close the shade."""
        _state = {"on_off": 0}

        if transition is not None:
            _state["transition_period"] = transition

        shade_state = await self._query_helper(
            self.SHADE_SERVICE, self.SET_SHADE_METHOD, _state
        )

        return shade_state
