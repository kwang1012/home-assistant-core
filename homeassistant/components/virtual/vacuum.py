"""Provide support for a virtual vaxuum."""

import asyncio
from datetime import timedelta
import logging
from typing import Any, cast

import numpy as np
import voluptuous as vol

from homeassistant.components.vacuum import (
    ATTR_CLEANED_AREA,
    ATTR_STATUS,
    DOMAIN as PLATFORM_DOMAIN,
    VacuumEntity,
    VacuumEntityFeature,
    STATE_CLEANING,
    STATE_DOCKED,
    STATE_RETURNING,
)
from homeassistant.components.rasc.helpers import Dataset, load_dataset
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import ATTR_BATTERY_LEVEL, ATTR_STATE
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

CONF_CHANGE_TIME = "opening_time"

DEFAULT_COVER_VALUE = "open"
DEFAULT_CHANGE_TIME = timedelta(seconds=0)

PLATFORM_SCHEMA = PLATFORM_SCHEMA.extend(
    virtual_schema(
        DEFAULT_COVER_VALUE,
        {
            vol.Optional(CONF_CLASS): cv.string,
            vol.Optional(CONF_CHANGE_TIME, default=DEFAULT_CHANGE_TIME): vol.All(
                cv.time_period, cv.positive_timedelta
            ),
        },
    )
)
COVER_SCHEMA = vol.Schema(
    virtual_schema(
        DEFAULT_COVER_VALUE,
        {
            vol.Optional(CONF_CLASS): cv.string,
            vol.Optional(CONF_CHANGE_TIME, default=DEFAULT_CHANGE_TIME): vol.All(
                cv.time_period, cv.positive_timedelta
            ),
        },
    )
)


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up vacuums."""

    coordinator: VirtualDataUpdateCoordinator = hass.data[COMPONENT_DOMAIN][
        entry.entry_id
    ]
    entities: list[VirtualVacuum] = []
    for entity_config in get_entity_configs(
        hass, entry.data[ATTR_GROUP_NAME], PLATFORM_DOMAIN
    ):
        entity_config = COVER_SCHEMA(entity_config)
        if entity_config[CONF_COORDINATED]:
            entity = cast(
                VirtualVacuum, CoordinatedVirtualVacuum(entity_config, coordinator)
            )
        else:
            entity = VirtualVacuum(entity_config)

        if entity_config[CONF_SIMULATE_NETWORK]:
            entity = cast(VirtualVacuum, NetworkProxy(entity))
            hass.data[COMPONENT_NETWORK][entity.entity_id] = entity

        entities.append(entity)

    async_add_entities(entities)


class VirtualVacuum(VirtualEntity, VacuumEntity):
    """Representation of a Virtual vacuum."""

    def __init__(self, name: str, supported_features: VacuumEntityFeature) -> None:
        """Initialize the vacuum."""
        super().__init__(name, PLATFORM_DOMAIN)
        self._attr_name = name
        self._attr_supported_features = supported_features
        self._attr_status = STATE_DOCKED
        self._cleaned_area: float = 0
        self._distance: float = 0
        self._is_cleaning = False
        self._is_clean = False
        self._is_returning = False
        self._docked = True
        self._dataset = load_dataset("vacuum")
        self.tasks: set[asyncio.Task] = set()

    def _create_state(self, config):
        super()._create_state(config)

        self._attr_status = STATE_DOCKED
        self._cleaned_area = 0.0
        self._distance = 0.0
        self._is_cleaning = False
        self._is_clean = False
        self._is_returning = False
        self._docked = True

    def _restore_state(self, state, config):
        super()._restore_state(state, config)

        self._attr_status = state.get(ATTR_STATUS, STATE_DOCKED)
        self._cleaned_area = state.get(ATTR_CLEANED_AREA, 0)
        self._distance = state.get("distance", 0)
        self._is_cleaning = False
        self._is_clean = self._cleaned_area >= 100
        self._is_returning = False
        self._docked = self._distance <= 0

    def _update_attributes(self):
        super()._update_attributes()
        self._attr_extra_state_attributes.update(
            {
                name: value
                for name, value in (
                    (ATTR_STATUS, self._attr_status),
                    (ATTR_CLEANED_AREA, self._cleaned_area),
                    ("distance", self._distance),
                    ("is_cleaning", self._is_cleaning),
                    ("is_clean", self._is_clean),
                    ("is_returning", self._is_returning),
                    ("docked", self._docked),
                )
                if value is not None
            }
        )

    @property
    def distance(self) -> int:
        """Return the distance of the vacuum from the docking station."""
        return max(0, min(100, self._distance))

    @property
    def cleaned_area(self) -> float:
        """Return the cleaned area."""
        return self._cleaned_area

    def _cleaning(self):
        self._is_clean = False
        self._is_cleaning = True
        self._attr_status = STATE_CLEANING
        self._update_attributes()

    def _cleaned(self):
        self._is_clean = True
        self._is_cleaning = False
        self._attr_status = ""
        self._update_attributes()

    def _returning(self):
        self._is_returning = True
        self._docked = False
        self._attr_status = STATE_RETURNING
        self._update_attributes()

    def _returned(self):
        self._is_returning = False
        self._docked = True
        self._attr_status = STATE_DOCKED
        self._update_attributes()

    async def _start_operation(self, action_length: float) -> None:
        try:
            if self._is_cleaning:
                target_cleaned_area = 100.0
                step = (target_cleaned_area - self._cleaned_area) / action_length
            elif self._is_returning:
                target_distance = 0.0
                step = (target_distance - self._distance) / action_length
            while True:
                if self._is_cleaning:
                    self._cleaned_area += step
                    if self._cleaned_area >= 100:
                        self._cleaned_area = 100
                        self._cleaned()
                        break
                elif self._is_returning:
                    self._distance += step
                    if self._distance <= 0:
                        self._distance = 0
                        self._returned()
                        break
                self._update_attributes()
                await asyncio.sleep(1)
        except asyncio.CancelledError:
            if self._cleaned_area >= 100:
                self._cleaned_area = 100
                self._cleaned()
            elif self._distance <= 0:
                self._distance = 0
                self._returned()
            self._update_attributes()

    def clean(self, **kwargs: Any) -> None:
        """Perform a spot clean-up."""
        self._cleaning()
        action_length = np.random.choice(self._dataset["clean"])
        task = self.hass.async_create_task(self._start_operation(action_length))
        self.tasks.add(task)

    def return_to_base(self, **kwargs: Any) -> None:
        """Tell the vacuum to return to its dock."""
        self._returning()
        action_length = np.random.choice(self._dataset["return_to_base"])
        task = self.hass.async_create_task(self._start_operation(action_length))
        self.tasks.add(task)


class CoordinatedVirtualVacuum(CoordinatedVirtualEntity, VirtualVacuum):
    """Representation of a Virtual switch."""

    def __init__(self, config, coordinator) -> None:
        """Initialize the Virtual switch device."""
        CoordinatedVirtualEntity.__init__(self, coordinator)
        VirtualVacuum.__init__(self, config)
