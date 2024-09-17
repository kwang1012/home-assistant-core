"""Provide support for a virtual cover."""

import asyncio
from datetime import timedelta
from enum import StrEnum
import logging
from typing import Any, cast

import voluptuous as vol

from homeassistant.components.timer import (
    ATTR_DURATION,
    ATTR_FINISHES_AT,
    ATTR_REMAINING,
    ATTR_RESTORE,
    CONF_DURATION,
    CONF_RESTORE,
    DEFAULT_RESTORE,
    DOMAIN as PLATFORM_DOMAIN,
    Timer,
    _format_timedelta,
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import ATTR_DEVICE_CLASS, ATTR_EDITABLE, STATE_CLOSED
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

_LOGGER = logging.getLogger(__name__)

DEPENDENCIES = [COMPONENT_DOMAIN]

CONF_CHANGE_TIME = "start_time"

DEFAULT_TIMER_VALUE = "start"
DEFAULT_CHANGE_TIME = timedelta(seconds=0)

PLATFORM_SCHEMA = PLATFORM_SCHEMA.extend(
    virtual_schema(
        DEFAULT_TIMER_VALUE,
        {
            vol.Optional(CONF_CLASS): cv.string,
            vol.Optional(CONF_CHANGE_TIME, default=DEFAULT_CHANGE_TIME): vol.All(
                cv.time_period, cv.positive_timedelta
            ),
        },
    )
)
TIMER_SCHEMA = vol.Schema(
    virtual_schema(
        DEFAULT_TIMER_VALUE,
        {
            vol.Optional(CONF_CLASS): cv.string,
            vol.Optional(CONF_CHANGE_TIME, default=DEFAULT_CHANGE_TIME): vol.All(
                cv.time_period, cv.positive_timedelta
            ),
        },
    )
)


class VirtualTimerDeviceClass(StrEnum):
    """Virtual timer device classes."""

    COFFEE_MACHINE = "coffee_machine"
    DISHWASHER = "dishwasher"
    DRYER = "dryer"
    MICROWAVE = "microwave"
    MOWER = "mower"
    OVEN = "oven"
    SPRINKLER = "sprinkler"
    TOASTER = "toaster"
    WASHER = "washer"

    @property
    def name(self) -> str:
        """Return name of the device class."""


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up timers."""

    coordinator: VirtualDataUpdateCoordinator = hass.data[COMPONENT_DOMAIN][
        entry.entry_id
    ]
    entities: list[VirtualTimer] = []
    for entity_config in get_entity_configs(
        hass, entry.data[ATTR_GROUP_NAME], PLATFORM_DOMAIN
    ):
        entity_config = TIMER_SCHEMA(entity_config)
        if entity_config[CONF_COORDINATED]:
            entity = cast(
                VirtualTimer, CoordinatedVirtualTimer(entity_config, coordinator)
            )
        else:
            entity = VirtualTimer(entity_config)

        if entity_config[CONF_SIMULATE_NETWORK]:
            entity = cast(VirtualTimer, NetworkProxy(entity))
            hass.data[COMPONENT_NETWORK][entity.entity_id] = entity

        entities.append(entity)

    async_add_entities(entities)


class VirtualTimer(VirtualEntity, Timer):
    """Representation of a Virtual cover."""

    def __init__(self, config) -> None:
        """Initialize the Virtual cover device."""
        super().__init__(config, PLATFORM_DOMAIN)

        self._attr_is_remaining = False
        if self._attr_device_class is None:
            self._attr_device_class = VirtualTimerDeviceClass.COFFEE_MACHINE

        self._change_time: timedelta = config.get(CONF_CHANGE_TIME)

        # cancel transition
        self.timer_tasks: set[asyncio.Task] = set()

    def _create_state(self, config):
        super()._create_state(config)

        self._config = config
        self._duration = cv.time_period_str(config[CONF_DURATION])
        self._restore = self._config.get(CONF_RESTORE, DEFAULT_RESTORE)
        self._attr_is_remaining = False

    def _restore_state(self, state, config):
        super()._restore_state(state, config)

        if config.get(CONF_RESTORE, DEFAULT_RESTORE):
            self._state = state.state
            self._duration = cv.time_period(state.extra_state_attributes[ATTR_DURATION])
            self._remaining = cv.time_period(state.extra_state_attributes[ATTR_REMAINING])
            self._attr_is_remaining = self._remaining.total_seconds() > 0
            self._end = cv.datetime(state.extra_state_attributes[ATTR_FINISHES_AT])
        else:
            self._create_state(config)

    def _update_attributes(self):
        super()._update_attributes()
        self._attr_extra_state_attributes.update(
            {
                name: value
                for name, value in (
                    (ATTR_DEVICE_CLASS, self._attr_device_class),
                    (ATTR_EDITABLE, self.editable),
                )
                if value is not None
            }
        )
        if self._duration is not None:
            self._attr_extra_state_attributes[ATTR_DURATION] = _format_timedelta(
                self._duration
            )
        if self._end is not None:
            self._attr_extra_state_attributes[ATTR_FINISHES_AT] = self._end.isoformat()
        if self._remaining is not None:
            self._attr_extra_state_attributes[ATTR_REMAINING] = _format_timedelta(
                self._remaining
            )
        self._attr_is_remaining = (
            self._remaining.total_seconds() > 0 if self._remaining is not None else False
        )
        if self._restore:
            self._attr_extra_state_attributes[ATTR_RESTORE] = self._restore

    def _time_remaining(self) -> None:
        self._attr_is_remaining = True
        self._update_attributes()

    def _timer_done(self) -> None:
        self._attr_is_remaining = False
        self._update_attributes()

    async def _start_operation(self):
        try:
            self._remaining = self._duration.total_seconds() + 1
            while True:
                self._remaining -= timedelta(seconds=1)
                if self._remaining.total_seconds() <= 0:
                    self._remaining = timedelta(seconds=0)
                    self._timer_done()
                    break
                self._update_attributes()
                await asyncio.sleep(1)
        except asyncio.CancelledError:
            if self._remaining.total_seconds() <= 0:
                self._remaining = timedelta(seconds=0)
                self._timer_done()
            self._update_attributes()

    def _start(self, duration: float) -> None:
        """Start the timer."""
        self._duration = timedelta(seconds=duration) if duration is not None else self._change_time
        if self._duration == DEFAULT_CHANGE_TIME:
            self._timer_done()
        else:
            self._time_remaining()
            task = self.hass.async_create_task(self._start_operation())
            self.timer_tasks.add(task)

    def stop(self, **kwargs: Any) -> None:
        """Stop the timer."""
        for task in self.timer_tasks:
            task.cancel()
        self.timer_tasks.clear()


class CoordinatedVirtualTimer(CoordinatedVirtualEntity, VirtualTimer):
    """Representation of a Virtual switch."""

    def __init__(self, config, coordinator) -> None:
        """Initialize the Virtual switch device."""
        CoordinatedVirtualEntity.__init__(self, coordinator)
        VirtualTimer.__init__(self, config)
