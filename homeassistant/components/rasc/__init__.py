"""The rasc integration."""
from __future__ import annotations

import datetime
import logging
import os
import shutil

import numpy as np
import voluptuous as vol

from homeassistant.components.climate import SERVICE_SET_TEMPERATURE
from homeassistant.const import (
    ANTICIPATORY,
    CONF_OPTIMAL_SCHEDULE_METRIC,
    CONF_RECORD_RESULTS,
    CONF_RESCHEDULING_POLICY,
    CONF_RESCHEDULING_TRIGGER,
    CONF_RESCHEDULING_WINDOW,
    CONF_ROUTINE_ARRIVAL_FILENAME,
    CONF_ROUTINE_PRIORITY_POLICY,
    CONF_SCHEDULING_POLICY,
    DOMAIN_RASCALRESCHEDULER,
    DOMAIN_RASCALSCHEDULER,
    EARLIEST,
    EARLY_START,
    EVENT_HOMEASSISTANT_STARTED,
    FCFS,
    FCFS_POST,
    GLOBAL_FIRST,
    GLOBAL_LONGEST,
    GLOBAL_SHORTEST,
    JIT,
    LATEST,
    LOCAL_FIRST,
    LOCAL_LONGEST,
    LOCAL_SHORTEST,
    LONGEST,
    MAX_AVG_PARALLELISM,
    MAX_P05_PARALLELISM,
    MIN_AVG_IDLE_TIME,
    MIN_AVG_RTN_LATENCY,
    MIN_AVG_RTN_WAIT_TIME,
    MIN_LENGTH,
    MIN_P95_IDLE_TIME,
    MIN_P95_RTN_LATENCY,
    MIN_P95_RTN_WAIT_TIME,
    MIN_RTN_EXEC_TIME_STD_DEV,
    NONE,
    OPTIMALW,
    OPTIMALWO,
    PROACTIVE,
    REACTIVE,
    RESCHEDULE_ALL,
    RESCHEDULE_SOME,
    RESCHEDULING_ACCURACY,
    RESCHEDULING_ESTIMATION,
    RV,
    SHORTEST,
    SJFW,
    SJFWO,
    TIMELINE,
)
from homeassistant.core import HomeAssistant
import homeassistant.helpers.config_validation as cv
from homeassistant.helpers.typing import ConfigType

from .abstraction import RASCAbstraction
from .const import (
    CONF_RESULTS_DIR,
    DOMAIN,
    LOGGER,
    RASC_ACTION,
    RASC_ENTITY_ID,
    RASC_EXPERIMENT_SETTING,
    RASC_FIXED_HISTORY,
    RASC_INTERRUPTION_MOMENT,
    RASC_INTERRUPTION_TIME,
    RASC_SLO,
    RASC_THERMOSTAT,
    RASC_THERMOSTAT_START,
    RASC_THERMOSTAT_TARGET,
    RASC_USE_UNIFORM,
    RASC_WORST_Q,
    SUPPORTED_PLATFORMS,
)
from .rescheduler import RascalRescheduler
from .scheduler import RascalScheduler

supported_scheduling_policies = [FCFS, FCFS_POST, JIT, TIMELINE]
supported_rescheduling_policies = [
    RV,
    EARLY_START,
    LOCAL_FIRST,
    LOCAL_SHORTEST,
    LOCAL_LONGEST,
    GLOBAL_FIRST,
    GLOBAL_SHORTEST,
    GLOBAL_LONGEST,
    NONE,
    OPTIMALW,
    OPTIMALWO,
    SJFW,
    SJFWO,
]
supported_rescheduling_triggers = [PROACTIVE, REACTIVE, ANTICIPATORY]
supported_optimal_metrics = [
    MIN_LENGTH,
    MIN_AVG_RTN_WAIT_TIME,
    MIN_P95_RTN_WAIT_TIME,
    MIN_AVG_RTN_LATENCY,
    MIN_P95_RTN_LATENCY,
    MIN_RTN_EXEC_TIME_STD_DEV,
    MIN_AVG_IDLE_TIME,
    MIN_P95_IDLE_TIME,
    MAX_AVG_PARALLELISM,
    MAX_P05_PARALLELISM,
]
supported_routine_priority_policies = [SHORTEST, LONGEST, EARLIEST, LATEST]
supported_rescheduling_accuracies = [RESCHEDULE_ALL, RESCHEDULE_SOME]

CONFIG_SCHEMA = vol.Schema(
    {
        DOMAIN: vol.Schema(
            {
                vol.Optional(CONF_SCHEDULING_POLICY, default=TIMELINE): vol.In(
                    supported_scheduling_policies
                ),
                vol.Optional(CONF_RESCHEDULING_POLICY, default=SJFW): vol.In(
                    supported_rescheduling_policies
                ),
                vol.Optional(CONF_RESCHEDULING_TRIGGER, default=PROACTIVE): vol.In(
                    supported_rescheduling_triggers
                ),
                vol.Optional(
                    CONF_OPTIMAL_SCHEDULE_METRIC, default=MIN_AVG_RTN_LATENCY
                ): vol.In(supported_optimal_metrics),
                vol.Optional(CONF_RESCHEDULING_WINDOW, default=10.0): cv.positive_float,
                vol.Optional(CONF_ROUTINE_PRIORITY_POLICY, default=EARLIEST): vol.In(
                    supported_routine_priority_policies
                ),
                vol.Optional(
                    CONF_ROUTINE_ARRIVAL_FILENAME, default="arrival_debug.csv"
                ): cv.string,
                vol.Optional(CONF_RECORD_RESULTS, default=True): cv.boolean,
                vol.Optional(RESCHEDULING_ESTIMATION, default=True): cv.boolean,
                vol.Optional(RESCHEDULING_ACCURACY, default=RESCHEDULE_ALL): vol.In(
                    supported_rescheduling_accuracies
                ),
                vol.Optional("mthresh", default=1.0): cv.positive_float,  # seconds
                vol.Optional("mithresh", default=2.0): cv.positive_float,  # seconds
                # vol.Optional(RESCHEDULING_ESTIMATION, default=True): cv.boolean,
                # vol.Optional(RESCHEDULING_ACCURACY, default=RESCHEDULE_ALL): vol.In(
                #     supported_rescheduling_accuracies
                # ),
                # vol.Optional("mthresh", default=1.0): cv.positive_float,  # seconds
                # vol.Optional("mithresh", default=2.0): cv.positive_float,  # seconds
                **{
                    vol.Optional(platform.value): vol.Schema(
                        {
                            vol.Optional(RASC_WORST_Q): cv.positive_float,
                            vol.Optional(RASC_SLO): cv.positive_float,
                        }
                    )
                    for platform in SUPPORTED_PLATFORMS
                },
                vol.Optional(RASC_USE_UNIFORM): cv.boolean,
                vol.Optional(RASC_FIXED_HISTORY): cv.boolean,
                vol.Optional(RASC_EXPERIMENT_SETTING): vol.Schema(
                    {
                        vol.Required(RASC_ENTITY_ID): cv.string,
                        vol.Required(RASC_ACTION): cv.string,
                        vol.Optional(RASC_INTERRUPTION_MOMENT): cv.positive_float,
                        vol.Optional(RASC_THERMOSTAT): vol.Schema(
                            {
                                vol.Required(RASC_THERMOSTAT_START): cv.string,
                                vol.Required(RASC_THERMOSTAT_TARGET): cv.string,
                            }
                        ),
                    }
                ),
            }
        )
    },
    extra=vol.ALLOW_EXTRA,
)

LOGGER.level = logging.DEBUG


def run_experiments(hass: HomeAssistant, rasc: RASCAbstraction):
    """Run experiments."""

    async def wrapper(_):
        settings = rasc.config[RASC_EXPERIMENT_SETTING]
        key = f"{settings[RASC_ENTITY_ID]},{settings[RASC_ACTION]}"
        if settings[RASC_ACTION] == SERVICE_SET_TEMPERATURE:
            if RASC_THERMOSTAT not in settings:
                raise ValueError("Thermostat setting not found")
            key += f",{settings[RASC_THERMOSTAT][RASC_THERMOSTAT_START]},{settings[RASC_THERMOSTAT][RASC_THERMOSTAT_TARGET]}"
        for i, level in enumerate(range(0, 105, 5)):
            LOGGER.debug("RUN: %d, level=%d", i + 1, level)
            avg_complete_time = np.mean(rasc.get_history(key))
            interruption_time = avg_complete_time * level * 0.01
            interruption_moment = settings[RASC_INTERRUPTION_MOMENT]
            # a_coro, s_coro, c_coro = hass.services.rasc_call("cover", "open_cover", {"entity_id": "cover.rpi_device_door"})
            a_coro, s_coro, c_coro = hass.services.rasc_call(
                "climate",
                "set_temperature",
                {"temperature": 69, "entity_id": "climate.rpi_device_thermostat"},
                {
                    RASC_INTERRUPTION_TIME: interruption_time,
                    RASC_INTERRUPTION_MOMENT: interruption_moment,
                },
            )
            await a_coro
            await s_coro
            await c_coro
            LOGGER.debug("complete!68->69")
            # a_coro, s_coro, c_coro = hass.services.rasc_call("cover", "close_cover", {"entity_id": "cover.rpi_device_door"})
            a_coro, s_coro, c_coro = hass.services.rasc_call(
                "climate",
                "set_temperature",
                {"temperature": 68, "entity_id": "climate.rpi_device_thermostat"},
            )
            await a_coro
            await s_coro
            await c_coro
            LOGGER.debug("complete!69->68")

    return wrapper


def _create_result_dir() -> str:
    """Create the result directory."""
    if not os.path.exists(CONF_RESULTS_DIR):
        os.mkdir(CONF_RESULTS_DIR)

    result_dirname = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S-%f")
    result_dirpath = os.path.join(CONF_RESULTS_DIR, result_dirname)
    if os.path.isdir(result_dirpath):
        shutil.rmtree(result_dirpath)
    os.mkdir(result_dirpath)
    return result_dirpath


def _save_rasc_configs(configs: ConfigType, result_dir: str) -> None:
    """Save the rasc configurations."""
    with open(f"{result_dir}/rasc_config.yaml", "w", encoding="utf-8") as f:
        for key, value in configs.items():
            f.write(f"{key}: {value}\n")


async def async_setup(hass: HomeAssistant, config: ConfigType) -> bool:
    """Set up the RASC component."""
    result_dir = _create_result_dir()
    _save_rasc_configs(config[DOMAIN], result_dir)

    component = hass.data[DOMAIN] = RASCAbstraction(
        LOGGER, DOMAIN, hass, config[DOMAIN]
    )
    LOGGER.debug("RASC config: %s", config[DOMAIN])
    scheduler = hass.data[DOMAIN_RASCALSCHEDULER] = RascalScheduler(
        hass, config[DOMAIN], result_dir
    )
    if config[DOMAIN][CONF_RESCHEDULING_POLICY] != NONE:
        rescheduler = hass.data[DOMAIN_RASCALRESCHEDULER] = RascalRescheduler(
            hass, scheduler, config[DOMAIN], result_dir
        )
        scheduler.reschedule_handler = rescheduler.handle_event

    await component.async_load()

    if RASC_EXPERIMENT_SETTING in config[DOMAIN]:
        hass.bus.async_listen_once(
            EVENT_HOMEASSISTANT_STARTED, run_experiments(hass, component)
        )

    return True
