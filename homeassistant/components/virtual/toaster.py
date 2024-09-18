"""Provide support for a virtual toaster."""

from datetime import timedelta
import logging
from typing import Any, cast

import numpy as np
import voluptuous as vol

from homeassistant.components.rasc.helpers import Dataset, load_dataset
from homeassistant.components.timer import DOMAIN as PLATFORM_DOMAIN, STATUS_IDLE
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
import homeassistant.helpers.config_validation as cv
from homeassistant.helpers.config_validation import PLATFORM_SCHEMA
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from . import get_entity_configs
from .const import (
    ATTR_GROUP_NAME,
    COMPONENT_DOMAIN,
    COMPONENT_NETWORK,
    CONF_CLASS,
    CONF_COORDINATED,
    CONF_SIMULATE_NETWORK,
)
from .coordinator import VirtualDataUpdateCoordinator
from .entity import CoordinatedVirtualEntity, VirtualEntity, virtual_schema
from .network import NetworkProxy
from .timer import VirtualTimer, VirtualTimerDeviceClass

_LOGGER = logging.getLogger(__name__)

DEPENDENCIES = [COMPONENT_DOMAIN]

DEFAULT_TOASTER_STATUS = STATUS_IDLE

PLATFORM_SCHEMA = PLATFORM_SCHEMA.extend(
    virtual_schema(
        DEFAULT_TOASTER_STATUS,
        {
            vol.Optional(CONF_CLASS): cv.string,
        },
    )
)
TOASTER_SCHEMA = vol.Schema(
    virtual_schema(
        DEFAULT_TOASTER_STATUS,
        {
            vol.Optional(CONF_CLASS): cv.string,
        },
    )
)

async def async_setup_entity(hass, entity_config, coordinator):
    entity_config = TOASTER_SCHEMA(entity_config)
    if entity_config[CONF_COORDINATED]:
        entity = cast(
            VirtualToaster, CoordinatedVirtualToaster(entity_config, coordinator)
        )
    else:
        entity = VirtualToaster(entity_config)

    if entity_config[CONF_SIMULATE_NETWORK]:
        entity = cast(VirtualToaster, NetworkProxy(entity))
        hass.data[COMPONENT_NETWORK][entity.entity_id] = entity

    return entity

class VirtualToaster(VirtualTimer):
    """Representation of a Virtual toaster."""

    def __init__(self, config) -> None:
        """Initialize the Virtual toaster device."""
        super().__init__(config)

        self._attr_device_class = VirtualTimerDeviceClass.TOASTER
        self._dataset = load_dataset(Dataset.TOASTER)

    def async_start(self, **kwargs: Any) -> None:
        """Start the coffee machine."""
        self.toast(**kwargs)

    def toast(self, **kwargs: Any):
        """Toast."""
        action_length = np.random.choice(self._dataset["toast"])
        self._start(action_length)


class CoordinatedVirtualToaster(CoordinatedVirtualEntity, VirtualToaster):
    """Representation of a Virtual switch."""

    def __init__(self, config, coordinator) -> None:
        """Initialize the Virtual switch device."""
        CoordinatedVirtualEntity.__init__(self, coordinator)
        VirtualToaster.__init__(self, config)
