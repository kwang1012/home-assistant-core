"""Provide support for a virtual washer."""

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

DEFAULT_WASHER_STATUS = STATUS_IDLE

PLATFORM_SCHEMA = PLATFORM_SCHEMA.extend(
    virtual_schema(
        DEFAULT_WASHER_STATUS,
        {
            vol.Optional(CONF_CLASS): cv.string,
        },
    )
)
WASHER_SCHEMA = vol.Schema(
    virtual_schema(
        DEFAULT_WASHER_STATUS,
        {
            vol.Optional(CONF_CLASS): cv.string,
        },
    )
)


async def async_setup_entity(hass, entity_config, coordinator):
    entity_config = WASHER_SCHEMA(entity_config)
    if entity_config[CONF_COORDINATED]:
        entity = cast(
            VirtualWasher, CoordinatedVirtualWasher(entity_config, coordinator)
        )
    else:
        entity = VirtualWasher(entity_config)

    if entity_config[CONF_SIMULATE_NETWORK]:
        entity = cast(VirtualWasher, NetworkProxy(entity))
        hass.data[COMPONENT_NETWORK][entity.entity_id] = entity

    return entity


class VirtualWasher(VirtualTimer):
    """Representation of a Virtual washer."""

    def __init__(self, config) -> None:
        """Initialize the Virtual washer device."""
        super().__init__(config)

        self._attr_device_class = VirtualTimerDeviceClass.WASHER
        self._dataset = load_dataset(Dataset.WASHER)

    def wash(self, **kwargs: Any):
        """Wash."""
        action_length = np.random.choice(self._dataset["wash"])
        self._start(action_length)

    def rinse(self, **kwargs: Any):
        """Rinse."""
        action_length = np.random.choice(self._dataset["rinse"])
        self._start(action_length)

    def spin(self, **kwargs: Any):
        """Spin."""
        action_length = np.random.choice(self._dataset["spin"])
        self._start(action_length)


class CoordinatedVirtualWasher(CoordinatedVirtualEntity, VirtualWasher):
    """Representation of a Virtual switch."""

    def __init__(self, config, coordinator) -> None:
        """Initialize the Virtual switch device."""
        CoordinatedVirtualEntity.__init__(self, coordinator)
        VirtualWasher.__init__(self, config)
