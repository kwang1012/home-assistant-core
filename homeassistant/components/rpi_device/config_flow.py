"""Config flow for Raspberry PI Device."""
from __future__ import annotations

from typing import Any

import voluptuous as vol

from homeassistant import config_entries
from homeassistant.const import CONF_HOST
from homeassistant.core import callback
from homeassistant.data_entry_flow import FlowResult
from homeassistant.helpers import device_registry as dr

from .api.device import RaspberryPiDevice
from .api.discover import Discover
from .const import DOMAIN


class ConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    """Handle a config flow for rpi device."""

    VERSION = 1

    def __init__(self) -> None:
        """Initialize the config flow."""

    async def async_step_user(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Handle the initial step."""
        errors = {}
        if user_input is not None:
            host = user_input[CONF_HOST]
            try:
                device = await self._async_try_connect(host, raise_on_progress=False)
            except ValueError:
                errors["base"] = "cannot_connect"
            else:
                return self._async_create_entry_from_device(device)

        return self.async_show_form(
            step_id="user",
            data_schema=vol.Schema({vol.Optional(CONF_HOST, default=""): str}),
            description_placeholders={CONF_HOST: "host"},
            errors=errors,
        )

    @callback
    def _async_create_entry_from_device(self, device: RaspberryPiDevice) -> FlowResult:
        """Create a config entry from a smart device."""
        self._abort_if_unique_id_configured(updates={CONF_HOST: device.host})
        return self.async_create_entry(
            title=f"{device.alias} {device.model}",
            data={
                CONF_HOST: device.host,
            },
        )

    async def _async_try_connect(
        self, host: str, raise_on_progress: bool = True
    ) -> RaspberryPiDevice:
        """Try to connect."""
        self._async_abort_entries_match({CONF_HOST: host})
        device: RaspberryPiDevice = await Discover.discover_single(host)
        await self.async_set_unique_id(
            dr.format_mac(device.mac), raise_on_progress=raise_on_progress
        )
        return device
