"""Provide support for a virtual mower."""

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

DEFAULT_MOWER_STATUS = STATUS_IDLE

PLATFORM_SCHEMA = cv.PLATFORM_SCHEMA.extend(
    virtual_schema(
        DEFAULT_MOWER_STATUS,
        {
            vol.Optional(CONF_CLASS): cv.string,
        },
    )
)
MOWER_SCHEMA = vol.Schema(
    virtual_schema(
        DEFAULT_MOWER_STATUS,
        {
            vol.Optional(CONF_CLASS): cv.string,
        },
    )
)


async def async_setup_entity(hass, entity_config, coordinator):
    """Set up a mower entity."""
    entity_config = MOWER_SCHEMA(entity_config)
    if entity_config[CONF_COORDINATED]:
        entity = cast(VirtualMower, CoordinatedVirtualMower(entity_config, coordinator))
    else:
        entity = VirtualMower(entity_config)

    if entity_config[CONF_SIMULATE_NETWORK]:
        entity = cast(VirtualMower, NetworkProxy(entity))
        hass.data[COMPONENT_NETWORK][entity.entity_id] = entity

    return entity


class VirtualMower(VirtualTimer):
    """Representation of a Virtual mower."""

    def __init__(self, config) -> None:
        """Initialize the Virtual mower device."""
        super().__init__(config)

        self._attr_device_class = VirtualTimerDeviceClass.MOWER
        self._dataset = load_dataset(Dataset.MOWER)

    def async_start(self, duration: timedelta | None = None, **kwargs: Any) -> None:
        """Start the coffee machine."""
        mow_type = kwargs.get("type")
        handler = getattr(self, mow_type, None)  # type: ignore[arg-type]
        if not handler:
            raise ValueError(f"Invalid mower type: {mow_type}")
        handler(**kwargs)

    def mow(self, **kwargs: Any):
        """Mow."""
        action_length = np.random.choice(self._dataset["mow"])
        self._start(action_length)

    def return_to_base(self, **kwargs: Any):
        """Return to base."""
        action_length = np.random.choice(self._dataset["return_to_base"])
        self._start(action_length)


class CoordinatedVirtualMower(CoordinatedVirtualEntity, VirtualMower):
    """Representation of a Virtual switch."""

    def __init__(self, config, coordinator) -> None:
        """Initialize the Virtual switch device."""
        CoordinatedVirtualEntity.__init__(self, coordinator)
        VirtualMower.__init__(self, config)
