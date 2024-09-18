"""Provide support for a virtual dishwasher."""

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

DEFAULT_COFFEE_MACHINE_STATUS = STATUS_IDLE

PLATFORM_SCHEMA = PLATFORM_SCHEMA.extend(
    virtual_schema(
        DEFAULT_COFFEE_MACHINE_STATUS,
        {
            vol.Optional(CONF_CLASS): cv.string,
        },
    )
)
COFFEE_MACHINE_SCHEMA = vol.Schema(
    virtual_schema(
        DEFAULT_COFFEE_MACHINE_STATUS,
        {
            vol.Optional(CONF_CLASS): cv.string,
        },
    )
)


async def async_setup_entity(hass, entity_config, coordinator):
    entity_config = COFFEE_MACHINE_SCHEMA(entity_config)
    if entity_config[CONF_COORDINATED]:
        entity = cast(
            VirtualDishwasher, CoordinatedVirtualDishwasher(entity_config, coordinator)
        )
    else:
        entity = VirtualDishwasher(entity_config)

    if entity_config[CONF_SIMULATE_NETWORK]:
        entity = cast(VirtualDishwasher, NetworkProxy(entity))
        hass.data[COMPONENT_NETWORK][entity.entity_id] = entity

    return entity


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up covers."""

    coordinator: VirtualDataUpdateCoordinator = hass.data[COMPONENT_DOMAIN][
        entry.entry_id
    ]
    entities: list[VirtualDishwasher] = []
    for entity_config in get_entity_configs(
        hass, entry.data[ATTR_GROUP_NAME], PLATFORM_DOMAIN
    ):
        entity_config = COFFEE_MACHINE_SCHEMA(entity_config)
        if entity_config[CONF_COORDINATED]:
            entity = cast(
                VirtualDishwasher, CoordinatedVirtualDishwasher(entity_config, coordinator)
            )
        else:
            entity = VirtualDishwasher(entity_config)

        if entity_config[CONF_SIMULATE_NETWORK]:
            entity = cast(VirtualDishwasher, NetworkProxy(entity))
            hass.data[COMPONENT_NETWORK][entity.entity_id] = entity

        entities.append(entity)

    async_add_entities(entities)


class VirtualDishwasher(VirtualTimer):
    """Representation of a Virtual dishwasher."""

    def __init__(self, config) -> None:
        """Initialize the Virtual dishwasher device."""
        super().__init__(config)

        self._attr_device_class = VirtualTimerDeviceClass.DISHWASHER
        self._dataset = load_dataset(Dataset.DISHWASHER)

    def async_start(self, **kwargs: Any) -> None:
        """Start the coffee machine."""
        wash_type = kwargs.get("type")
        handler = getattr(self, wash_type, None)
        if not handler:
            raise ValueError(f"Invalid coffee type: {wash_type}")
        handler(**kwargs)

    def wash(self, **kwargs: Any):
        """Wash."""
        action_length = np.random.choice(self._dataset["wash"])
        self._start(action_length)

    def rinse(self, **kwargs: Any):
        """Rinse."""
        action_length = np.random.choice(self._dataset["rinse"])
        self._start(action_length)

    def dry(self, **kwargs: Any):
        """Dry."""
        action_length = np.random.choice(self._dataset["dry"])
        self._start(action_length)


class CoordinatedVirtualDishwasher(CoordinatedVirtualEntity, VirtualDishwasher):
    """Representation of a Virtual switch."""

    def __init__(self, config, coordinator) -> None:
        """Initialize the Virtual switch device."""
        CoordinatedVirtualEntity.__init__(self, coordinator)
        VirtualDishwasher.__init__(self, config)
