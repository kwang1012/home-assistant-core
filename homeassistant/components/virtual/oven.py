"""Provide support for a virtual oven."""

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

DEFAULT_OVEN_STATUS = STATUS_IDLE

PLATFORM_SCHEMA = PLATFORM_SCHEMA.extend(
    virtual_schema(
        DEFAULT_OVEN_STATUS,
        {
            vol.Optional(CONF_CLASS): cv.string,
        },
    )
)
OVEN_SCHEMA = vol.Schema(
    virtual_schema(
        DEFAULT_OVEN_STATUS,
        {
            vol.Optional(CONF_CLASS): cv.string,
        },
    )
)


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up ovens."""

    coordinator: VirtualDataUpdateCoordinator = hass.data[COMPONENT_DOMAIN][
        entry.entry_id
    ]
    entities: list[VirtualOven] = []
    for entity_config in get_entity_configs(
        hass, entry.data[ATTR_GROUP_NAME], PLATFORM_DOMAIN
    ):
        entity_config = OVEN_SCHEMA(entity_config)
        if entity_config[CONF_COORDINATED]:
            entity = cast(
                VirtualOven, CoordinatedVirtualOven(entity_config, coordinator)
            )
        else:
            entity = VirtualOven(entity_config)

        if entity_config[CONF_SIMULATE_NETWORK]:
            entity = cast(VirtualOven, NetworkProxy(entity))
            hass.data[COMPONENT_NETWORK][entity.entity_id] = entity

        entities.append(entity)

    async_add_entities(entities)


class VirtualOven(VirtualEntity, VirtualTimer):
    """Representation of a Virtual oven."""

    def __init__(self, config) -> None:
        """Initialize the Virtual oven device."""
        super().__init__(config, PLATFORM_DOMAIN)

        self._attr_device_class = VirtualTimerDeviceClass.OVEN
        self._dataset = load_dataset(Dataset.OVEN)

    def bake(self, **kwargs: Any):
        """Bake."""
        action_length = np.random.choice(self._dataset["bake"])
        self._start(action_length)

    def broil(self, **kwargs: Any):
        """Broil."""
        action_length = np.random.choice(self._dataset["broil"])
        self._start(action_length)

    def roast(self, **kwargs: Any):
        """Roast."""
        action_length = np.random.choice(self._dataset["roast"])
        self._start(action_length)


class CoordinatedVirtualOven(CoordinatedVirtualEntity, VirtualOven):
    """Representation of a Virtual switch."""

    def __init__(self, config, coordinator) -> None:
        """Initialize the Virtual switch device."""
        CoordinatedVirtualEntity.__init__(self, coordinator)
        VirtualOven.__init__(self, config)
