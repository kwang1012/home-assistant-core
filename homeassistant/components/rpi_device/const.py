"""Consts used by rpi_device."""

from typing import Final

from homeassistant.const import Platform

DOMAIN = "rpi_device"

PLATFORMS: Final = [Platform.COVER, Platform.CLIMATE]
