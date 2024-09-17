"""Provide support for a virtual dryer."""

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

DEFAULT_DRYER_STATUS = STATUS_IDLE

PLATFORM_SCHEMA = PLATFORM_SCHEMA.extend(
    virtual_schema(
        DEFAULT_DRYER_STATUS,
        {
            vol.Optional(CONF_CLASS): cv.string,
        },
    )
)
DRYER_SCHEMA = vol.Schema(
    virtual_schema(
        DEFAULT_DRYER_STATUS,
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
    """Set up dryers."""

    coordinator: VirtualDataUpdateCoordinator = hass.data[COMPONENT_DOMAIN][
        entry.entry_id
    ]
    entities: list[VirtualDryer] = []
    for entity_config in get_entity_configs(
        hass, entry.data[ATTR_GROUP_NAME], PLATFORM_DOMAIN
    ):
        entity_config = DRYER_SCHEMA(entity_config)
        if entity_config[CONF_COORDINATED]:
            entity = cast(
                VirtualDryer, CoordinatedVirtualDryer(entity_config, coordinator)
            )
        else:
            entity = VirtualDryer(entity_config)

        if entity_config[CONF_SIMULATE_NETWORK]:
            entity = cast(VirtualDryer, NetworkProxy(entity))
            hass.data[COMPONENT_NETWORK][entity.entity_id] = entity

        entities.append(entity)

    async_add_entities(entities)


class VirtualDryer(VirtualEntity, VirtualTimer):
    """Representation of a Virtual dryer."""

    def __init__(self, config) -> None:
        """Initialize the Virtual dryer device."""
        super().__init__(config, PLATFORM_DOMAIN)

        self._attr_device_class = VirtualTimerDeviceClass.DRYER
        self._dataset = load_dataset(Dataset.DRYER)

    def dry(self, **kwargs: Any):
        """Dry."""
        action_length = np.random.choice(self._dataset["dry"])
        self._start(action_length)


class CoordinatedVirtualDryer(CoordinatedVirtualEntity, VirtualDryer):
    """Representation of a Virtual switch."""

    def __init__(self, config, coordinator) -> None:
        """Initialize the Virtual switch device."""
        CoordinatedVirtualEntity.__init__(self, coordinator)
        VirtualDryer.__init__(self, config)
