"""The rasc integration."""
from __future__ import annotations

import asyncio
import datetime
import json
import os
import shutil

import numpy as np
import voluptuous as vol

from homeassistant.components.climate import SERVICE_SET_TEMPERATURE
from homeassistant.const import (
    ACTION_LENGTH_ESTIMATION,
    ANTICIPATORY,
    CONF_ACTION_START_METHOD,
    CONF_OPTIMAL_SCHEDULE_METRIC,
    CONF_RECORD_RESULTS,
    CONF_RESCHEDULING_POLICY,
    CONF_RESCHEDULING_TRIGGER,
    CONF_RESCHEDULING_WINDOW,
    CONF_ROUTINE_ARRIVAL_FILENAME,
    CONF_ROUTINE_PRIORITY_POLICY,
    CONF_SCHEDULING_POLICY,
    DO_COMPARISON,
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
    MEAN_ESTIMATION,
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
    OVERHEAD_MEASUREMENT,
    P50_ESTIMATION,
    P70_ESTIMATION,
    P80_ESTIMATION,
    P90_ESTIMATION,
    P95_ESTIMATION,
    P99_ESTIMATION,
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
    START_EVENT_BASED,
    START_TIME_BASED,
    TIMELINE,
)
from homeassistant.core import HomeAssistant
import homeassistant.helpers.config_validation as cv
from homeassistant.helpers.typing import ConfigType

from .abstraction import RASCAbstraction, ServiceFailureError
from .const import (
    CONF_ENABLED,
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
from .helpers import OverheadMeasurement
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
supported_action_length_estimations = [
    MEAN_ESTIMATION,
    P50_ESTIMATION,
    P70_ESTIMATION,
    P80_ESTIMATION,
    P90_ESTIMATION,
    P95_ESTIMATION,
    P99_ESTIMATION,
]
supported_start_methods = [START_EVENT_BASED, START_TIME_BASED]

CONFIG_SCHEMA = vol.Schema(
    {
        DOMAIN: vol.Schema(
            {
                vol.Optional(CONF_ENABLED, default=True): bool,
                vol.Optional(OVERHEAD_MEASUREMENT, default=False): bool,
                vol.Optional("routine_setup_filename", default={}): dict,
                vol.Optional("rasc_history_filename"): cv.string,
                vol.Optional(ACTION_LENGTH_ESTIMATION, default="mean"): vol.In(
                    supported_action_length_estimations
                ),
                vol.Optional(DO_COMPARISON, default=True): bool,
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
                vol.Optional(
                    CONF_ACTION_START_METHOD, default=START_EVENT_BASED
                ): vol.In(supported_start_methods),
            }
        )
    },
    extra=vol.ALLOW_EXTRA,
)


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


async def initialize_entity_state(
    hass: HomeAssistant, entity_id: str, service_calls: list[dict]
):
    domain = entity_id.split(".")[0]
    try:
        for service_call in service_calls:
            service = service_call.pop("service")
            _, _, c_coro = hass.services.rasc_call(
                domain, service, {"entity_id": entity_id, "params": service_call}
            )
            await c_coro
        return True
    except ServiceFailureError:
        LOGGER.error(f"Failed to initialize entity state: {entity_id}")
        return False


async def setup_routine(hass: HomeAssistant, config: ConfigType):
    routine_setup_conf = config[DOMAIN]["routine_setup_filename"]
    tasks = []
    for entity_id, service_calls in routine_setup_conf.items():
        tasks.append(
            hass.async_create_task(
                initialize_entity_state(hass, entity_id, service_calls)
            )
        )

    results = await asyncio.gather(*tasks)
    if any(not result for result in results):
        LOGGER.error("Failed to setup routine")
    else:
        print("Setup routine completed")
        hass.bus.async_fire("rasc_routine_setup")


def examine_final_state(hass: HomeAssistant, config: ConfigType):
    routine_setup_conf = config[DOMAIN]["routine_setup_filename"]
    final_states = {}
    for entity_id in routine_setup_conf:
        state = hass.states.get(entity_id)
        final_states[entity_id] = {
            "state": state.state,
        }
        if state.domain == "light":
            final_states[entity_id]["brightness"] = state.attributes["brightness"]

    path = "homeassistant/components/rasc/datasets"
    # with open(os.path.join(path, "all_final_state.json"), "w") as f:
    #     json.dump(final_states, f, indent=4)
    dataset = config["rasc"][CONF_ROUTINE_ARRIVAL_FILENAME].split(".")[0].split("_")[1]
    if "debug" in dataset:
        return
    with open(os.path.join(path, f"{dataset}_final_state.json")) as f:
        fcfs_states = json.load(f)

    differ_states = {}
    for entity_id, state in final_states.items():
        if state != fcfs_states[entity_id]:
            differ_states[entity_id] = f"{state} vs {fcfs_states[entity_id]}"

    if differ_states:
        LOGGER.error(
            "Final states are different from FCFS:\n%s",
            json.dumps(differ_states, indent=2),
        )


async def async_setup(hass: HomeAssistant, config: ConfigType) -> bool:
    """Set up the RASC component."""

    # cpu/memory measurement

    om = OverheadMeasurement(hass, config[DOMAIN])

    def handle_measurement_stop(event):
        om.stop()
        examine_final_state(hass, config)
        hass.stop()

    hass.bus.async_listen_once("rasc_measurement_start", lambda _: om.start())
    hass.bus.async_listen_once("rasc_measurement_stop", handle_measurement_stop)

    if not config[DOMAIN][CONF_ENABLED]:
        return True

    result_dir = _create_result_dir()
    _save_rasc_configs(config[DOMAIN], result_dir)

    rasc_history_conf = config[DOMAIN].get("rasc_history_filename")
    path = "homeassistant/components/rasc/datasets"
    storage_path = "config/.storage"
    if rasc_history_conf:
        shutil.copy(os.path.join(path, rasc_history_conf), storage_path)

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

    hass.data["rasc_events"] = []

    await component.async_load()

    if config[DOMAIN].get(OVERHEAD_MEASUREMENT):
        hass.bus.async_listen_once(EVENT_HOMEASSISTANT_STARTED, setup_routine(hass, config))

    if RASC_EXPERIMENT_SETTING in config[DOMAIN]:
        hass.bus.async_listen_once(
            EVENT_HOMEASSISTANT_STARTED, run_experiments(hass, component)
        )

    return True
