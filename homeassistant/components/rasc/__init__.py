"""The rasc integration."""

from __future__ import annotations

import asyncio
import json
import os
import shutil

import numpy as np
import voluptuous as vol

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
    CONF_USE_VOPT,
    DOMAIN,
    LOGGER,
    RASC_ACTION,
    RASC_DETECTION_TIME_EXPS,
    RASC_ENTITY_ID,
    RASC_FIXED_HISTORY,
    RASC_INTERRUPTION_EXPS,
    RASC_INTERRUPTION_MOMENT,
    RASC_INTERRUPTION_TIME,
    RASC_SLO,
    RASC_USE_UNIFORM,
    RASC_WORST_Q,
    SUPPORTED_PLATFORMS,
)
from .decorator import (
    rasc_push_event as rasc_push_event,
    rasc_target_state as rasc_target_state,
)
from .entity import BaseRoutineEntity as BaseRoutineEntity
from .helpers import (
    Dataset as Dataset,
    OverheadMeasurement,
    load_dataset as load_dataset,
)
from .rescheduler import RascalRescheduler
from .scheduler import RascalScheduler, create_routine as create_routine

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
                vol.Optional(CONF_USE_VOPT, default=False): bool,
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
                vol.Optional(CONF_RECORD_RESULTS, default=False): cv.boolean,
                vol.Optional(RESCHEDULING_ESTIMATION, default=True): cv.boolean,
                vol.Optional(RESCHEDULING_ACCURACY, default=RESCHEDULE_ALL): vol.In(
                    supported_rescheduling_accuracies
                ),
                # seconds
                vol.Optional("mthresh", default=1.0): cv.positive_float,
                # seconds
                vol.Optional("mithresh", default=2.0): cv.positive_float,
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
                vol.Optional(RASC_DETECTION_TIME_EXPS): vol.Schema(
                    [
                        {
                            vol.Required(RASC_ENTITY_ID): cv.string,
                            vol.Required(RASC_ACTION): vol.Schema(
                                {
                                    vol.Optional("service"): cv.string,
                                    vol.Optional("service_data"): dict,
                                }
                            ),
                            vol.Required("reset_action"): vol.Schema(
                                {
                                    vol.Optional("service"): cv.string,
                                    vol.Optional("service_data"): dict,
                                }
                            ),
                        }
                    ]
                ),
                vol.Optional(RASC_INTERRUPTION_EXPS): cv.boolean,
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
        if RASC_DETECTION_TIME_EXPS in rasc.config:
            settings = rasc.config[RASC_DETECTION_TIME_EXPS]
            for setting in settings:
                entity_id = setting[RASC_ENTITY_ID]
                device = entity_id.split(".")[0]
                action = setting[RASC_ACTION]["service"]
                service_data = setting[RASC_ACTION].get("service_data", {})
                reset_action = setting["reset_action"]["service"]
                reset_service_data = setting["reset_action"].get("service_data", {})
                for _ in range(1 if os.environ.get("RASC_SHORT") else 10):
                    LOGGER.info(
                        "Start action %s on %s with data %s",
                        action,
                        entity_id,
                        service_data,
                    )
                    a_coro, s_coro, c_coro = hass.services.rasc_call(
                        device, action, {"entity_id": entity_id, **service_data}
                    )
                    await a_coro
                    await s_coro
                    await c_coro
                    LOGGER.info("Completed action %s on %s", action, entity_id)

                    LOGGER.info(
                        "Start reset action %s on %s with data %s",
                        reset_action,
                        entity_id,
                        reset_service_data,
                    )
                    a_coro, s_coro, c_coro = hass.services.rasc_call(
                        device,
                        reset_action,
                        {"entity_id": entity_id, **reset_service_data},
                    )
                    await a_coro
                    await s_coro
                    await c_coro
                    LOGGER.info(
                        "Completed reset action %s on %s", reset_action, entity_id
                    )
        elif rasc.config.get(RASC_INTERRUPTION_EXPS):
            # interruption
            key = "climate.rpi_device_thermostat,set_temperature,68,69"
            for interruption_moment in (0.5, 0.8):
                LOGGER.info("Interruption moment: %.2f", interruption_moment)
                for level in range(0, 105, 5):
                    LOGGER.info("Interruption level=%d", level)
                    avg_complete_time = np.mean(rasc.get_history(key))
                    interruption_time = avg_complete_time * level * 0.01
                    a_coro, s_coro, c_coro = hass.services.rasc_call(
                        "climate",
                        "set_temperature",
                        {
                            "temperature": 69,
                            "entity_id": "climate.rpi_device_thermostat",
                        },
                        {
                            RASC_INTERRUPTION_TIME: interruption_time,
                            RASC_INTERRUPTION_MOMENT: interruption_moment,
                        },
                    )
                    await a_coro
                    await s_coro
                    await c_coro
                    LOGGER.info("Complete!68->69")
                    a_coro, s_coro, c_coro = hass.services.rasc_call(
                        "climate",
                        "set_temperature",
                        {
                            "temperature": 68,
                            "entity_id": "climate.rpi_device_thermostat",
                        },
                    )
                    await a_coro
                    await s_coro
                    await c_coro
                    LOGGER.info("Complete!69->68")
        hass.stop()

    return wrapper


def _create_result_dir(config: ConfigType) -> str:
    """Create the result directory."""
    if not os.path.exists(CONF_RESULTS_DIR):
        os.mkdir(CONF_RESULTS_DIR)

    result_dirname = (
        config[CONF_ROUTINE_ARRIVAL_FILENAME].split(".")[0]
        + f"_{config[CONF_SCHEDULING_POLICY]}"
    )
    if config[CONF_RESCHEDULING_POLICY] != "none":
        result_dirname += f"_{config[CONF_RESCHEDULING_POLICY]}"
    result_dirpath = os.path.join(CONF_RESULTS_DIR, result_dirname)
    if os.path.isdir(result_dirpath):
        shutil.rmtree(result_dirpath)
    os.mkdir(result_dirpath)
    return result_dirpath


def _save_rasc_configs(configs: ConfigType, result_dir: str) -> None:
    """Save the rasc configurations."""
    with open(f"{result_dir}/rasc_config.yaml", "w", encoding="utf-8") as f:
        f.writelines(f"{key}: {value}\n" for key, value in configs.items())


async def initialize_entity_state(
    hass: HomeAssistant, entity_id: str, service_calls: list[dict]
) -> bool:
    """Initialize the entity state by calling the given services."""
    domain = entity_id.split(".", maxsplit=1)[0]
    try:
        for service_call in service_calls:
            service = service_call.pop("service")
            _, _, c_coro = hass.services.rasc_call(
                domain, service, {"entity_id": entity_id, "params": service_call}
            )
            await c_coro
    except ServiceFailureError as e:
        LOGGER.error(
            "Failed to initialize entity state: %s, service: %s, error: %s",
            entity_id,
            service,
            e,
        )
        return False
    else:
        return True


async def setup_routine(hass: HomeAssistant, config: ConfigType) -> None:
    """Set up routines by initializing entity states as defined in config."""
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
        hass.bus.async_fire("rasc_routine_setup")

    if os.environ.get("RASC_IS_EXAMPLE"):
        hass.stop()


def examine_final_state(hass: HomeAssistant, config: ConfigType) -> None:
    """Examine and log differences between final and expected entity states."""
    routine_setup_conf = config[DOMAIN]["routine_setup_filename"]
    final_states = {}
    for entity_id in routine_setup_conf:
        state = hass.states.get(entity_id)
        final_states[entity_id] = {
            "state": state.state,  # type: ignore[union-attr]
        }
        if state.domain == "light":  # type: ignore[union-attr]
            final_states[entity_id]["brightness"] = state.attributes.get(  # type: ignore[union-attr]
                "brightness", None
            )

    path = "homeassistant/components/rasc/datasets"
    # with open(os.path.join(path, "all_final_state.json"), "w") as f:
    #     json.dump(final_states, f, indent=4)
    dataset = config["rasc"][CONF_ROUTINE_ARRIVAL_FILENAME].split(".")[0].split("_")[1]
    if "debug" in dataset:
        return
    with open(os.path.join(path, f"{dataset}_final_state.json"), encoding="utf-8") as f:
        fcfs_states = json.load(f)

    differ_states = {}
    for entity_id, state in final_states.items():  # type: ignore[assignment]
        if state != fcfs_states[entity_id]:
            differ_states[entity_id] = f"{state} vs {fcfs_states[entity_id]}"

    if differ_states:
        LOGGER.error(
            "Final states are different from FCFS:\n%s",
            json.dumps(differ_states, indent=2),
        )


async def async_setup(hass: HomeAssistant, config: ConfigType) -> bool:
    """Set up the RASC component."""

    LOGGER.info("RASC config: %s", config[DOMAIN])
    # cpu/memory measurement

    om = OverheadMeasurement(hass, config[DOMAIN])

    def handle_measurement_stop(event):
        om.stop()
        # examine_final_state(hass, config)
        hass.stop()

    hass.bus.async_listen_once("rasc_measurement_start", lambda _: om.start())
    hass.bus.async_listen_once("rasc_measurement_stop", handle_measurement_stop)
    hass.bus.async_listen("rasc_measurement_update", lambda _: om.save())

    if not config[DOMAIN][CONF_ENABLED]:
        hass.bus.async_listen_once(
            EVENT_HOMEASSISTANT_STARTED,
            lambda _: hass.bus.async_fire("rasc_routine_setup"),
        )
        return True

    if config[DOMAIN][CONF_RECORD_RESULTS]:
        config[DOMAIN]["results_dir"] = result_dir = _create_result_dir(config[DOMAIN])
        _save_rasc_configs(config[DOMAIN], result_dir)

    rasc_history_conf = config[DOMAIN].get("rasc_history_filename")
    path = "homeassistant/components/rasc/datasets"

    storage_path = f"{hass.config.config_dir}/.storage"
    if rasc_history_conf:
        await hass.async_add_executor_job(
            shutil.copy,
            os.path.join(path, rasc_history_conf),
            storage_path,
        )

    component = hass.data[DOMAIN] = RASCAbstraction(
        LOGGER, DOMAIN, hass, config[DOMAIN]
    )
    scheduler = hass.data[DOMAIN_RASCALSCHEDULER] = RascalScheduler(
        hass, config[DOMAIN]
    )
    if config[DOMAIN][CONF_RESCHEDULING_POLICY] != NONE:
        hass.data[DOMAIN_RASCALRESCHEDULER] = RascalRescheduler(
            hass, scheduler, config[DOMAIN]
        )

    hass.data["rasc_events"] = []

    await component.async_load()

    async def _handle_start(_):
        await setup_routine(hass, config)

    if config[DOMAIN].get("routine_setup_filename"):
        hass.bus.async_listen_once(
            EVENT_HOMEASSISTANT_STARTED,
            _handle_start,
        )

    if (
        RASC_INTERRUPTION_EXPS in config[DOMAIN]
        or RASC_DETECTION_TIME_EXPS in config[DOMAIN]
    ):
        hass.bus.async_listen_once(
            EVENT_HOMEASSISTANT_STARTED, run_experiments(hass, component)
        )

    return True
