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
    StateVacuumEntity,
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

DEFAULT_VACUUM_VALUE = "off"
DEFAULT_CHANGE_TIME = timedelta(seconds=0)

PLATFORM_SCHEMA = PLATFORM_SCHEMA.extend(
    virtual_schema(
        DEFAULT_VACUUM_VALUE,
        {}
    )
)
VACUUM_SCHEMA = vol.Schema(
    virtual_schema(
        DEFAULT_VACUUM_VALUE,
        {}
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
        entity_config = VACUUM_SCHEMA(entity_config)
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


class VirtualVacuum(VirtualEntity, StateVacuumEntity):
    """Representation of a Virtual vacuum."""

    def __init__(self, config) -> None:
        """Initialize the vacuum."""
        super().__init__(config, PLATFORM_DOMAIN)
        self._attr_supported_features = VacuumEntityFeature.START | VacuumEntityFeature.STOP
        self._attr_status = STATE_DOCKED
        self._cleaned_area: float = 0
        self._distance: float = 0
        self._is_cleaning = False
        self._is_clean = False
        self._is_returning = False
        self._docked = True
        self._dataset = load_dataset(Dataset.VACUUM)
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

        self._attr_status = state.attributes.get(ATTR_STATUS, STATE_DOCKED)
        self._cleaned_area = state.attributes.get(ATTR_CLEANED_AREA, 0.0)
        self._distance = state.attributes.get("distance", 0.0)
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

    @property
    def is_cleaning(self) -> bool:
        """Return if the vacuum is cleaning."""
        return self._is_cleaning

    @property
    def is_clean(self) -> bool:
        """Return if the vacuum is clean."""
        return self._is_clean

    @property
    def is_returning(self) -> bool:
        """Return if the vacuum is returning to the dock."""
        return self._is_returning

    @property
    def docked(self) -> bool:
        """Return if the vacuum is docked."""
        return self._docked

    @property
    def status(self) -> str:
        """Return the status of the vacuum."""
        return self._attr_status

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

    def async_start(self, **kwargs: Any) -> None:
        """Perform a spot clean-up."""
        self._cleaning()
        action_length = np.random.choice(self._dataset["clean"])
        task = self.hass.async_create_task(self._start_operation(action_length))
        self.tasks.add(task)

    def async_stop(self, **kwargs: Any) -> None:
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
