"""Rpi discovery component."""

from __future__ import annotations

from .device import RaspberryPiDevice
from .door import RaspberryPiDoor
from .fan import RaspberryPiFan
from .light import RaspberryPiLight
from .lock import RaspberryPiLock
from .shade import RaspberryPiShade
from .switch import RaspberryPiSwitch
from .thermostat import RaspberryPiThermostat


class Discover:
    """Discover component."""

    @staticmethod
    async def discover_all(hosts: list[str]) -> list[RaspberryPiDevice]:
        """Discover all devices on the network.

        :param hosts: List of IP addresses to query
        :rtype: list[SmartDevice]
        :return: List of objects for querying/controlling found devices.
        """
        devices = []

        try_ports = [9999 + i for i in range(120)]
        for host in hosts:
            for port in try_ports:
                try:
                    device = await Discover.discover_single(host, port=port)
                    devices.append(device)
                except ValueError:
                    pass

        return devices

    @staticmethod
    async def discover_single(
        host: str, *, port: int | None = None
    ) -> RaspberryPiDevice:
        """Discover a single device by the given IP address.

        :param host: Hostname of device to query
        :rtype: SmartDevice
        :return: Object for querying/controlling found device.
        """
        device = RaspberryPiDevice(host, port=port)

        info = await device.query(
            {
                "system": {"get_sysinfo": None},
            }
        )

        device_class = Discover._get_device_class(info)
        dev = device_class(host, port=port)
        await dev.update()

        return dev

    @staticmethod
    def _get_device_class(info: dict) -> type[RaspberryPiDevice]:
        """Find SmartDevice subclass for device described by passed data."""
        if "system" not in info or "get_sysinfo" not in info["system"]:
            raise ValueError("No 'system' or 'get_sysinfo' in response")

        sysinfo = info["system"]["get_sysinfo"]
        type_ = sysinfo.get("type", sysinfo.get("mic_type"))
        if type_ is None:
            raise ValueError("Unable to find the device type field!")

        if "door" in type_.lower():
            return RaspberryPiDoor

        if "thermostat" in type_.lower():
            return RaspberryPiThermostat

        if "shade" in type_.lower():
            return RaspberryPiShade

        if "fan" in type_.lower():
            return RaspberryPiFan

        if "light" in type_.lower():
            return RaspberryPiLight

        if "switch" in type_.lower():
            return RaspberryPiSwitch

        if "lock" in type_.lower():
            return RaspberryPiLock

        raise ValueError(f"Unknown device type: {type_}")
