"""Constants for the rasc integration."""
import logging
from typing import Final

from homeassistant.const import Platform

DOMAIN = "rasc"
LOGGER = logging.getLogger(__package__)

RASC_ACK = "ack"
RASC_START = "start"
RASC_COMPLETE = "complete"
RASC_RESPONSE = "rasc_response"
RASC_SCHEDULED = "scheduled"
RASC_WORST_Q = "worst_q"
RASC_SLO = "slo"

CONF_TRANSITION = "transition"
DEFAULT_FAILURE_TIMEOUT = 30  # s
ACTION_LENGTH_PADDING = 2.0  # second
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
