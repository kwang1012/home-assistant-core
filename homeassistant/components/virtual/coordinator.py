"""Component to coordinate virtual devices."""
from __future__ import annotations

from datetime import timedelta
import logging

from homeassistant.core import HomeAssistant
from homeassistant.helpers.debounce import Debouncer
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator

_LOGGER = logging.getLogger(__name__)

REQUEST_REFRESH_DELAY = 0.35


class VirtualDataUpdateCoordinator(DataUpdateCoordinator[None]):
    """DataUpdateCoordinator to gather data for a specific TPLink device."""

    def __init__(
        self,
        hass: HomeAssistant,
    ) -> None:
        """Initialize DataUpdateCoordinator to gather data for specific SmartPlug."""
        self.update_children = True
        update_interval = timedelta(seconds=5)
        super().__init__(
            hass,
            _LOGGER,
            name="123",
            update_interval=update_interval,
            # We don't want an immediate refresh since the device
            # takes a moment to reflect the state change
            request_refresh_debouncer=Debouncer(
                hass, _LOGGER, cooldown=REQUEST_REFRESH_DELAY, immediate=False
            ),
        )

    async def async_request_refresh_without_children(self) -> None:
        """Request a refresh without the children."""
        # If the children do get updated this is ok as this is an
        # optimization to reduce the number of requests on the device
        # when we do not need it.
        self.update_children = False
        await self.async_request_refresh()

    async def _async_update_data(self) -> None:
        """Fetch all device and sensor data from api."""
