"""Rpi device door api."""
from typing import Any, Optional

from .device import RaspberryPiDevice


class RaspberryPiDoor(RaspberryPiDevice):
    """RaspberryPiDoor component."""

    DOOR_SERVICE = "pi.virtual.door"
    SET_DOOR_METHOD = "transition_door_state"

    def __init__(self, host, port=None) -> None:
        """Initialize RaspberryPiDoor api."""
        super().__init__(host, port)
        self._state: dict[str, Any] = {}

    async def get_door_state(self) -> None:
        """Get door state."""
        self._state = await self._query_helper(self.DOOR_SERVICE, "get_door_state")

    @property
    def door_state(self) -> dict[str, str]:
        """Query the door state."""
        door_state = self.sys_info["door_state"]
        if door_state is None:
            raise ValueError(
                "The device has no door_state or you have not called update()"
            )

        return door_state

    @property
    def is_closed(self) -> bool:
        """Return True is the door is closed."""
        door_state = self.door_state
        return bool(door_state.get("closed"))

    @property
    def door_position(self):
        """Return current door position."""
        door_state = self.door_state
        return door_state.get("current_position")

    @property
    def is_opening(self):
        """Return True is the door is opening."""
        door_state = self.door_state
        return door_state.get("opening")

    @property
    def is_closing(self):
        """Return True is the door is closing."""
        door_state = self.door_state
        return door_state.get("closing")

    async def open_door(self, transition: Optional[int] = None):
        """Open the door."""
        _state = {"on_off": 1}

        if transition is not None:
            _state["transition_period"] = transition

        door_state = await self._query_helper(
            self.DOOR_SERVICE, self.SET_DOOR_METHOD, _state
        )

        return door_state

    async def close_door(self, transition: Optional[int] = None):
        """Close the door."""
        _state = {"on_off": 0}

        if transition is not None:
            _state["transition_period"] = transition

        door_state = await self._query_helper(
            self.DOOR_SERVICE, self.SET_DOOR_METHOD, _state
        )

        return door_state
