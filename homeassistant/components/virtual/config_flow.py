"""Config flow for Aarlo."""

import logging

import voluptuous as vol

from homeassistant import config_entries, exceptions
from homeassistant.config_entries import ConfigFlowResult

from .cfg import UpgradeCfg
from .const import (
    ATTR_FILE_NAME,
    ATTR_GROUP_NAME,
    COMPONENT_DOMAIN,
    IMPORTED_GROUP_NAME,
    IMPORTED_YAML_FILE,
)
from .coordinator import VirtualDataUpdateCoordinator

_LOGGER = logging.getLogger(__name__)


class VirtualFlowHandler(config_entries.ConfigFlow, domain=COMPONENT_DOMAIN):
    """Aarlo config flow."""

    VERSION = 1

    async def validate_input(self, user_input):
        """Validate the user input."""
        for group, values in self.hass.data.get(COMPONENT_DOMAIN, {}).items():
            _LOGGER.debug("checking %s", group)
            if isinstance(values, VirtualDataUpdateCoordinator):
                continue
            if group == user_input[ATTR_GROUP_NAME]:
                raise GroupNameAlreadyUsed
            if values[ATTR_FILE_NAME] == user_input[ATTR_FILE_NAME]:
                raise FileNameAlreadyUsed
        return {"title": f"{user_input[ATTR_GROUP_NAME]} - {COMPONENT_DOMAIN}"}

    async def async_step_user(self, user_input: dict | None = None) -> ConfigFlowResult:
        """Handle the initial user step."""
        _LOGGER.debug("step user %s", user_input)

        errors = {}
        if user_input is not None:
            try:
                info = await self.validate_input(user_input)
                return self.async_create_entry(
                    title=info["title"],
                    data={
                        ATTR_GROUP_NAME: user_input[ATTR_GROUP_NAME],
                        ATTR_FILE_NAME: user_input[ATTR_FILE_NAME],
                    },
                )
            except GroupNameAlreadyUsed:
                errors["base"] = "group_name_used"
            except FileNameAlreadyUsed:
                errors["base"] = "file_name_used"

        else:
            # Fill in some defaults.
            user_input = {
                ATTR_GROUP_NAME: IMPORTED_GROUP_NAME,
                ATTR_FILE_NAME: IMPORTED_YAML_FILE,
            }

        return self.async_show_form(
            step_id="user",
            data_schema=vol.Schema(
                {
                    vol.Required(
                        ATTR_GROUP_NAME, default=user_input[ATTR_GROUP_NAME]
                    ): str,
                    vol.Required(
                        ATTR_FILE_NAME, default=user_input[ATTR_FILE_NAME]
                    ): str,
                }
            ),
            errors=errors,
        )

    async def async_step_import(self, import_data) -> ConfigFlowResult:
        """Import momentary config from configuration.yaml."""
        _LOGGER.debug("importing aarlo YAML %s", import_data)
        UpgradeCfg.import_yaml(import_data)
        data = UpgradeCfg.create_flow_data(import_data)

        return self.async_create_entry(title="Imported Virtual Group", data=data)


class GroupNameAlreadyUsed(exceptions.HomeAssistantError):
    """Error indicating group name already used."""


class FileNameAlreadyUsed(exceptions.HomeAssistantError):
    """Error indicating file name already used."""
