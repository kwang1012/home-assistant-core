"""Provide support for a virtual coffee machine."""

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
            VirtualCoffeeMachine, CoordinatedVirtualCoffeeMachine(entity_config, coordinator)
        )
    else:
        entity = VirtualCoffeeMachine(entity_config)

    if entity_config[CONF_SIMULATE_NETWORK]:
        entity = cast(VirtualCoffeeMachine, NetworkProxy(entity))
        hass.data[COMPONENT_NETWORK][entity.entity_id] = entity

    return entity

async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up coffee machines."""

    coordinator: VirtualDataUpdateCoordinator = hass.data[COMPONENT_DOMAIN][
        entry.entry_id
    ]
    entities: list[VirtualCoffeeMachine] = []
    for entity_config in get_entity_configs(
        hass, entry.data[ATTR_GROUP_NAME], PLATFORM_DOMAIN
    ):
        entity_config = COFFEE_MACHINE_SCHEMA(entity_config)
        if entity_config[CONF_COORDINATED]:
            entity = cast(
                VirtualCoffeeMachine, CoordinatedVirtualCoffeeMachine(entity_config, coordinator)
            )
        else:
            entity = VirtualCoffeeMachine(entity_config)

        if entity_config[CONF_SIMULATE_NETWORK]:
            entity = cast(VirtualCoffeeMachine, NetworkProxy(entity))
            hass.data[COMPONENT_NETWORK][entity.entity_id] = entity

        entities.append(entity)

    async_add_entities(entities)


class VirtualCoffeeMachine(VirtualTimer):
    """Representation of a Virtual coffee machine."""

    def __init__(self, config) -> None:
        """Initialize the Virtual coffee machine device."""
        super().__init__(config)

        self._attr_device_class = VirtualTimerDeviceClass.COFFEE_MACHINE
        self._dataset = load_dataset(Dataset.COFFEE_MACHINE)

    def async_start(self, **kwargs: Any) -> None:
        """Start the coffee machine."""
        coffee_type = kwargs.get("type")
        handler = getattr(self, coffee_type, None)
        if not handler:
            raise ValueError(f"Invalid coffee type: {coffee_type}")
        handler(**kwargs)

    def espresso(self, **kwargs: Any):
        """Make an espresso."""
        action_length = np.random.choice(self._dataset["espresso"])
        self._start(action_length)

    def americano(self, **kwargs: Any):
        """Make an americano."""
        action_length = np.random.choice(self._dataset["americano"])
        self._start(action_length)

    def cappuccino(self, **kwargs: Any):
        """Make a cappuccino."""
        action_length = np.random.choice(self._dataset["cappuccino"])
        self._start(action_length)

    def latte(self, **kwargs: Any):
        """Make a latte."""
        action_length = np.random.choice(self._dataset["latte"])
        self._start(action_length)

    def mocha(self, **kwargs: Any):
        """Make a mocha."""
        action_length = np.random.choice(self._dataset["mocha"])
        self._start(action_length)


class CoordinatedVirtualCoffeeMachine(CoordinatedVirtualEntity, VirtualCoffeeMachine):
    """Representation of a Virtual switch."""

    def __init__(self, config, coordinator) -> None:
        """Initialize the Virtual switch device."""
        CoordinatedVirtualEntity.__init__(self, coordinator)
        VirtualCoffeeMachine.__init__(self, config)
