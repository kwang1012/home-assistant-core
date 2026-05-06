"""Provide support for a virtual dryer."""

from datetime import timedelta
import logging
from typing import Any, cast

import numpy as np
import voluptuous as vol

from homeassistant.components.rasc import Dataset, load_dataset
from homeassistant.components.timer import STATUS_IDLE
import homeassistant.helpers.config_validation as cv

from .const import (
    COMPONENT_DOMAIN,
    COMPONENT_NETWORK,
    CONF_CLASS,
    CONF_COORDINATED,
    CONF_SIMULATE_NETWORK,
)
from .entity import CoordinatedVirtualEntity, virtual_schema
from .network import NetworkProxy
from .timer import VirtualTimer, VirtualTimerDeviceClass

_LOGGER = logging.getLogger(__name__)

DEPENDENCIES = [COMPONENT_DOMAIN]

DEFAULT_DRYER_STATUS = STATUS_IDLE

PLATFORM_SCHEMA = cv.PLATFORM_SCHEMA.extend(
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


async def async_setup_entity(hass, entity_config, coordinator):
    """Set up a dryer entity."""
    entity_config = DRYER_SCHEMA(entity_config)
    if entity_config[CONF_COORDINATED]:
        entity = cast(VirtualDryer, CoordinatedVirtualDryer(entity_config, coordinator))
    else:
        entity = VirtualDryer(entity_config)

    if entity_config[CONF_SIMULATE_NETWORK]:
        entity = cast(VirtualDryer, NetworkProxy(entity))
        hass.data[COMPONENT_NETWORK][entity.entity_id] = entity

    return entity


class VirtualDryer(VirtualTimer):
    """Representation of a Virtual dryer."""

    def __init__(self, config) -> None:
        """Initialize the Virtual dryer device."""
        super().__init__(config)

        self._attr_device_class = VirtualTimerDeviceClass.DRYER
        self._dataset = load_dataset(Dataset.DRYER)

    def async_start(self, duration: timedelta | None = None, **kwargs: Any) -> None:
        """Start the coffee machine."""
        self.dry(**kwargs)

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
