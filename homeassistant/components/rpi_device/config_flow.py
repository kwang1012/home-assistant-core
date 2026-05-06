"""Config flow for Raspberry PI Device."""

from typing import Any

import voluptuous as vol

from homeassistant import config_entries
from homeassistant.config_entries import ConfigFlowResult
from homeassistant.const import CONF_HOST
from homeassistant.core import callback

from .api.device import RaspberryPiDevice
from .api.discover import Discover
from .const import ATTR_NODES_FILE, DOMAIN


class ConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    """Handle a config flow for rpi device."""

    VERSION = 1

    def __init__(self) -> None:
        """Initialize the config flow."""

    async def async_step_import(self, import_data: dict[str, Any]) -> ConfigFlowResult:
        """Handle YAML import."""
        # If already configured, abort
        if self._async_current_entries():
            return self.async_abort(reason="already_configured")

        # Forward to the user step logic
        return await self.async_step_user(import_data)

    async def async_step_user(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Handle the initial step."""
        errors = {}
        if user_input is not None:
            nodes_file = user_input[ATTR_NODES_FILE]
            try:
                return self.async_create_entry(
                    title="Raspberry Pi Devices",
                    data={ATTR_NODES_FILE: nodes_file},
                )
            except ValueError:
                errors["base"] = "cannot_connect"
            # else:
            #     return self._async_create_entry_from_device(device)

        return self.async_show_form(
            step_id="user",
            data_schema=vol.Schema({vol.Required(ATTR_NODES_FILE): str}),
            description_placeholders={
                ATTR_NODES_FILE: "homeassistant/components/rpi_device/nodes.txt"
            },
            errors=errors,
        )

    @callback
    def _async_create_entry_from_device(
        self, device: RaspberryPiDevice
    ) -> ConfigFlowResult:
        """Create a config entry from a smart device."""
        self._abort_if_unique_id_configured(updates={CONF_HOST: device.addr})
        return self.async_create_entry(
            title=f"{device.alias} {device.model}",
            data={
                CONF_HOST: device.addr,
            },
        )

    async def _async_try_connect(
        self, host: str, raise_on_progress: bool = True
    ) -> RaspberryPiDevice:
        """Try to connect."""
        self._async_abort_entries_match({CONF_HOST: host})
        device: RaspberryPiDevice = await Discover.discover_single(host)
        await self.async_set_unique_id(
            device.unique_id, raise_on_progress=raise_on_progress
        )
        return device
