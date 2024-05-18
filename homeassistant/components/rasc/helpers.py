"""RASC integration helpers."""
from __future__ import annotations

import asyncio
import csv
from enum import Enum
import json
import logging
from logging import Logger
import math
import time
from typing import Any

import psutil

from homeassistant.const import (
    ACTION_LENGTH_ESTIMATION,
    ATTR_ACTION_ID,
    ATTR_ENTITY_ID,
    ATTR_SERVICE,
    CONF_RESCHEDULING_POLICY,
    CONF_RESCHEDULING_TRIGGER,
    CONF_ROUTINE_ARRIVAL_FILENAME,
    CONF_SCHEDULING_POLICY,
    RASC_RESPONSE,
)
from homeassistant.core import Event, HomeAssistant

from .const import CONF_ENABLED

_LOGGER = logging.getLogger(__name__)


class OverheadMeasurement:
    """Overhead measurement component."""

    def __init__(self, hass: HomeAssistant, config: dict[str, Any]) -> None:
        """Initialize the measurement."""
        self._loop = hass.loop
        self._hass = hass
        self._terminate_flag = asyncio.Event()
        self._cpu_usage: list[float] = []
        self._mem_usage: list[float] = []
        self._config = config
        self._start_time = time.time()
        self._reschedule_intervals: list[tuple[float, float]] = []
        self._hass.bus.async_listen("reschedule_event", self._handle_reschedule_event)

    def _handle_reschedule_event(self, event: Event):
        start_time = event.data["from"] - self._start_time
        end_time = event.data["to"] - self._start_time
        diff = event.data["diff"]
        self._reschedule_intervals.append((start_time, end_time, diff))

    def start(self) -> None:
        """Start the measurement."""
        _LOGGER.info("Start measurement")
        self._start_time = time.time()
        self._loop.create_task(self._start())

    async def _start(self) -> None:
        while not self._terminate_flag.is_set():
            cpu_usage = psutil.cpu_percent()
            mem_usage = psutil.virtual_memory().percent
            self._cpu_usage.append(cpu_usage)
            self._mem_usage.append(mem_usage)
            await asyncio.sleep(1)

    def stop(self) -> None:
        """Stop the measurement."""
        self._terminate_flag.set()
        with open("results/" + str(self) + ".json", "w", encoding="utf-8") as f:
            json.dump(
                {
                    "cpu": self._cpu_usage,
                    "mem": self._mem_usage,
                    "reschedule": self._reschedule_intervals,
                },
                f,
            )

    def __str__(self) -> str:
        """Return measurement name."""
        filename = self._config[CONF_ROUTINE_ARRIVAL_FILENAME].split(".")[0]
        if not self._config.get(CONF_ENABLED):
            return f"om_{filename}"
        return f"om_{self._config[CONF_SCHEDULING_POLICY]}_{self._config[CONF_RESCHEDULING_POLICY]}_{self._config[CONF_RESCHEDULING_TRIGGER]}_{self._config[ACTION_LENGTH_ESTIMATION]}_{filename}"


def fire(
    hass: HomeAssistant,
    rasc_type: str,
    entity_id: str,
    action: str,
    logger: Logger | None = None,
    service_data: dict[str, Any] | None = None,
):
    """Fire rasc response."""
    if logger:
        logger.info(
            "%s %s %s: %s",
            entity_id,
            action,
            service_data.get(ATTR_ACTION_ID, ""),
            rasc_type,
        )
    service_data = service_data or {}
    hass.bus.async_fire(
        RASC_RESPONSE,
        {
            "type": rasc_type,
            ATTR_SERVICE: action,
            ATTR_ENTITY_ID: entity_id,
            **{
                str(key): value
                for key, value in service_data.items()
                if key != ATTR_ENTITY_ID
            },
        },
    )


class Dataset(Enum):
    """Dataset enum."""

    THERMOSTAT = "thermostat"
    DOOR = "door"
    ELEVATOR = "elevator"
    PROJECTOR = "projector"
    SHADE = "shade"


def load_dataset(name: Dataset, action: str | None = None):
    """Load dataset."""
    if name.value == "thermostat":
        dataset = _get_thermo_datasets()
    else:
        with open(
            f"homeassistant/components/rasc/datasets/{name.value}.json",
            encoding="utf-8",
        ) as f:
            dataset = json.load(f)

    if action is None:
        return dataset

    if action not in dataset:
        _LOGGER.info(
            "Action not found! Available actions:\n%s", "\n".join(list(dataset.keys()))
        )
    return dataset[action]


def _get_thermo_datasets():
    with open(
        "homeassistant/components/rasc/datasets/hvac-actions.csv", encoding="utf-8"
    ) as f:
        reader = csv.reader(f)

        src_dst_map = {}

        for row in reader:
            start, target, length = row
            if start == "temp_start":
                continue
            key = f"{math.floor(float(start))},{math.floor(float(target))}"

            if key not in src_dst_map:
                src_dst_map[key] = []

            src_dst_map[key].append(float(length))

        datasets = {}
        for key, values in src_dst_map.items():
            src_dst_map[key] = list(filter(lambda value: value < 3600, values))
            if len(values) > 50:
                datasets[key] = src_dst_map[key]
    return datasets
