"""Rpi device door api."""
from typing import Any

from homeassistant.components.climate import HVACMode

from .device import RaspberryPiDevice


class RaspberryPiThermostat(RaspberryPiDevice):
    """RaspberryPiDoor component."""

    THERMOSTAT_SERVICE = "pi.virtual.thermostat"
    SET_THERMOSTAT_METHOD = "transition_thermostat_state"

    def __init__(self, host, port=None) -> None:
        """Initialize RaspberryPiThermostat api."""
        super().__init__(host, port)
        self._state: dict[str, Any] = {}

    async def get_door_state(self) -> None:
        """Get door state."""
        self._state = await self._query_helper(
            self.THERMOSTAT_SERVICE, "get_thermostat_state"
        )

    @property
    def thermostat_state(self) -> dict[str, str]:
        """Query the door state."""
        thermostat_state = self.sys_info["thermostat_state"]
        if thermostat_state is None:
            raise ValueError(
                "The device has no door_state or you have not called update()"
            )

        return thermostat_state

    @property
    def current_temperature(self) -> float | None:
        """Return True is the door is closed."""
        thermostat_state = self.thermostat_state
        temperature = thermostat_state.get("current_temperature")
        if temperature is not None:
            return float(temperature)
        return temperature

    @property
    def target_temperature(self) -> float | None:
        """Return current door position."""
        thermostat_state = self.thermostat_state
        temperature = thermostat_state.get("target_temperature")
        if temperature is not None:
            return float(temperature)
        return temperature

    @property
    def hvac_mode(self):
        """Return True is the door is opening."""
        thermostat_state = self.thermostat_state
        return thermostat_state.get("hvac_mode")

    @property
    def preset_mode(self):
        """Return True is the door is closing."""
        thermostat_state = self.thermostat_state
        return thermostat_state.get("preset_mode")

    async def set_temperature(self, temperature: float, **kwargs: Any) -> None:
        """Set new target temperature."""
        _state = {"temperature": temperature, **kwargs}

        themostat_state = await self._query_helper(
            self.THERMOSTAT_SERVICE, self.SET_THERMOSTAT_METHOD, _state
        )

        return themostat_state

    async def set_hvac_mode(self, hvac_mode: HVACMode) -> None:
        """Set new target hvac mode."""
        _state = {"hvac_mode": hvac_mode}

        themostat_state = await self._query_helper(
            self.THERMOSTAT_SERVICE, self.SET_THERMOSTAT_METHOD, _state
        )

        return themostat_state

    async def set_preset_mode(self, preset_mode: str) -> None:
        """Set new preset mode."""
        _state = {"preset_mode": preset_mode}

        themostat_state = await self._query_helper(
            self.THERMOSTAT_SERVICE, self.SET_THERMOSTAT_METHOD, _state
        )

        return themostat_state
