"""Constants for the rasc integration."""
import logging
from typing import Final

from homeassistant.const import Platform

DOMAIN = "rasc"
LOGGER = logging.getLogger(__package__)

CONF_ENABLED = "enabled"
RASC_ACK = "ack"
RASC_START = "start"
RASC_COMPLETE = "complete"
RASC_RESPONSE = "rasc_response"
RASC_SCHEDULED = "scheduled"
RASC_WORST_Q = "worst_q"
RASC_SLO = "slo"
RASC_USE_UNIFORM = "use_uniform"
RASC_FIXED_HISTORY = "fixed_history"
RASC_EXPERIMENT_SETTING = "experiment_settings"
RASC_INTERRUPTION_MOMENT = "interruption_moment"
RASC_INTERRUPTION_TIME = "interruption_time"
RASC_ENTITY_ID = "entity_id"
RASC_ACTION = "action"
RASC_THERMOSTAT = "thermostat"
RASC_THERMOSTAT_START = "start"
RASC_THERMOSTAT_TARGET = "target"

CONF_TRANSITION = "transition"
DEFAULT_FAILURE_TIMEOUT = 30  # s
MIN_RESCHEDULE_TIME = 0.05  # second
ACK_TO_START = 0.4  # second

SUPPORTED_PLATFORMS: Final[list[Platform]] = [
    Platform.SWITCH,
    Platform.LIGHT,
    Platform.COVER,
    Platform.LOCK,
    Platform.FAN,
    Platform.CLIMATE,
]
CONF_RESULTS_DIR = "results"

LOGGER = logging.getLogger(__package__)
