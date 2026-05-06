"""RPI Device base entity."""

from homeassistant.helpers import device_registry as dr
from homeassistant.helpers.device_registry import DeviceInfo
from homeassistant.helpers.entity import Entity

from .api.device import RaspberryPiDevice
from .const import DOMAIN


class RpiEntity(Entity):
    """Base Rpi entity."""

    def __init__(self, device: RaspberryPiDevice) -> None:
        """Initialize the switch."""
        self.device: RaspberryPiDevice = device
        self._attr_unique_id = self.device.unique_id
        self._attr_device_info = DeviceInfo(
            connections={(dr.CONNECTION_NETWORK_MAC, device.mac)},
            identifiers={(DOMAIN, str(device.unique_id))},
            manufacturer="RASC group",
            model=device.model,
            name=device.sys_info["dev_name"],
            sw_version=device.hw_info["sw_ver"],
            hw_version=device.hw_info["hw_ver"],
        )
        self.entity_id = self.device.sys_info["entity_id"]

    async def async_update(self) -> None:
        """Update the device."""
        await self.device.update()
