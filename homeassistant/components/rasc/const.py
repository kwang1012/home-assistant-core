"""Constants for the rasc integration."""

import logging
from typing import Final

from homeassistant.const import Platform

DOMAIN = "rasc"
LOGGER = logging.getLogger(__package__)
LOGGER.setLevel(logging.INFO)

CONF_ENABLED = "enabled"
CONF_USE_VOPT = "use_vopt"
RASC_ACK = "ack"
RASC_START = "start"
RASC_COMPLETE = "complete"
RASC_FAIL = "fail"
RASC_RESPONSE = "rasc_response"
RASC_SCHEDULED = "scheduled"
RASC_WORST_Q = "worst_q"
RASC_SLO = "slo"
RASC_USE_UNIFORM = "use_uniform"
RASC_FIXED_HISTORY = "fixed_history"
RASC_DETECTION_TIME_EXPS = "detection_time_exps"
RASC_INTERRUPTION_EXPS = "interruption_exps"
RASC_INTERRUPTION_MOMENT = "interruption_moment"
RASC_INTERRUPTION_TIME = "interruption_time"
RASC_ENTITY_ID = "entity_id"
RASC_ACTION = "action"
RASC_THERMOSTAT = "thermostat"
RASC_THERMOSTAT_START = "start"
RASC_THERMOSTAT_TARGET = "target"

CONF_TRANSITION = "transition"
DEFAULT_FAILURE_TIMEOUT = 1000  # s
PROCESSING_OVERHEADS = 1.5  # s
MAX_SCHEDULE_TIME = 0.5  # s
MIN_RESCHEDULE_TIME = 0.05  # second

SUPPORTED_PLATFORMS: Final[list[Platform]] = [
    Platform.SWITCH,
    Platform.LIGHT,
    Platform.COVER,
    Platform.LOCK,
    Platform.FAN,
    Platform.CLIMATE,
]
CONF_RESULTS_DIR = "results"
