"""RPI Device API."""
from __future__ import annotations

import asyncio
import contextlib
import errno
import json
import pickle
import struct
from typing import Any, Optional, Union

from async_timeout import timeout as asyncio_timeout

_NO_RETRY_ERRORS = {errno.EHOSTDOWN, errno.EHOSTUNREACH, errno.ECONNREFUSED}


class RaspberryPiDevice:
    """RasberryPiDevice component."""

    DEFAULT_PORT = 9999
    DEFAULT_TIMEOUT = 5
    BLOCK_SIZE = 4

    def __init__(self, host, port=None) -> None:
        """Initialize device."""
        self.host = host
        self.port = port or RaspberryPiDevice.DEFAULT_PORT
        self.reader: Optional[asyncio.StreamReader] = None
        self.writer: Optional[asyncio.StreamWriter] = None
        self.query_lock: Optional[asyncio.Lock] = None
        self.loop: Optional[asyncio.AbstractEventLoop] = None

        self._last_update: Any = None
        self._sys_info: Any = None

    @property
    def sys_info(self) -> dict[str, Any]:
        """Return system information."""
        return self._sys_info

    @property
    def device_type(self) -> str:
        """Return system information."""
        return self._sys_info["type"]

    @property
    def model(self) -> str:
        """Return device model."""
        sys_info = self.sys_info
        return str(sys_info["model"])

    @property
    def alias(self) -> str:
        """Return device name (alias)."""
        sys_info = self.sys_info
        return str(sys_info["alias"])

    @property
    def device_id(self) -> str:
        """Return unique ID for the device.

        If not overridden, this is the MAC address of the device.
        Individual sockets on strips will override this.
        """
        return self.mac

    @property
    def hw_info(self) -> dict:
        """Return hardware information.

        This returns just a selection of sysinfo keys that are related to hardware.
        """
        keys = [
            "sw_ver",
            "hw_ver",
            "mac",
            "type",
            "hwId",
            "fwId",
            "dev_name",
        ]
        sys_info = self.sys_info
        return {key: sys_info[key] for key in keys if key in sys_info}

    @property
    def mac(self) -> str:
        """Return mac address.

        :return: mac address in hexadecimal with colons, e.g. 01:23:45:67:89:ab
        """
        sys_info = self.sys_info

        mac = sys_info.get("mac", sys_info.get("mic_mac"))
        if not mac:
            raise ValueError(
                "Unknown mac, please submit a bug report with sys_info output."
            )

        if ":" not in mac:
            mac = ":".join(format(s, "02x") for s in bytes.fromhex(mac[2:]))

        return mac

    async def update(self):
        """Query the device to update the data.

        Needed for properties that are decorated with `requires_update`.
        """
        req = {}
        req.update(self._create_request("system", "get_sysinfo"))

        self._last_update = await self.query(req)
        self._sys_info = self._last_update["system"]["get_sysinfo"]

    async def query(self, request: Union[str, dict], retry_count: int = 3):
        """Request information from a Rpi Device.

        :param str host: host name or ip address of the device
        :param request: command to send to the device (can be either dict or
        json string)
        :param retry_count: how many retries to do in case of failure
        :return: response dict
        """

        self._detect_event_loop_change()

        if not self.query_lock:
            self.query_lock = asyncio.Lock()

        if isinstance(request, dict):
            request = json.dumps(request)
            assert isinstance(request, str)

        timeout = RaspberryPiDevice.DEFAULT_TIMEOUT

        async with self.query_lock:
            return await self._query(request, retry_count, timeout)

    async def close(self) -> None:
        """Close the connection."""
        writer = self.writer
        self.reader = self.writer = None
        if writer:
            writer.close()
            with contextlib.suppress(Exception):
                await writer.wait_closed()

    async def _query(self, request: str, retry_count: int, timeout: int) -> dict:
        """Try to query a device."""
        for retry in range(retry_count + 1):
            try:
                await self._connect(timeout)
            except ConnectionRefusedError as ex:
                await self.close()
                raise ValueError(
                    f"Unable to connect to the device: {self.host}:{self.port}: {ex}"
                ) from ex
            except OSError as ex:
                await self.close()
                if ex.errno in _NO_RETRY_ERRORS or retry >= retry_count:
                    raise ValueError(
                        f"Unable to connect to the device: {self.host}:{self.port}: {ex}"
                    ) from ex
                continue
            except Exception as ex:  # pylint: disable=broad-exception-caught
                await self.close()
                if retry >= retry_count:
                    raise ValueError(
                        f"Unable to connect to the device: {self.host}:{self.port}: {ex}"
                    ) from ex
                continue

            try:
                assert self.reader is not None
                assert self.writer is not None
                async with asyncio_timeout(timeout):
                    return await self._execute_query(request)
            except Exception as ex:  # pylint: disable=broad-exception-caught
                await self.close()
                if retry >= retry_count:
                    raise ValueError(
                        f"Unable to query the device {self.host}:{self.port}: {ex}"
                    ) from ex

        # make mypy happy, this should never be reached..
        await self.close()
        raise ValueError("Query reached somehow to unreachable")

    async def _execute_query(self, request: str) -> dict:
        """Execute a query on the device and wait for the response."""
        assert self.writer is not None
        assert self.reader is not None

        req = pickle.dumps(request)
        self.writer.write(struct.pack(">I", len(req)) + req)
        await self.writer.drain()

        packed_block_size = await self.reader.readexactly(self.BLOCK_SIZE)
        length = struct.unpack(">I", packed_block_size)[0]

        buffer = await self.reader.readexactly(length)
        response = pickle.loads(buffer)
        json_payload = json.loads(response)

        return json_payload

    def _detect_event_loop_change(self) -> None:
        """Check if this object has been reused between event loops."""
        loop = asyncio.get_running_loop()
        if not self.loop:
            self.loop = loop
        elif self.loop != loop:
            self._reset()

    async def _connect(self, timeout: int) -> None:
        """Try to connect or reconnect to the device."""
        if self.writer:
            return
        self.reader = self.writer = None

        task = asyncio.open_connection(self.host, self.port)
        async with asyncio_timeout(timeout):
            self.reader, self.writer = await task

    def _reset(self) -> None:
        """Clear any variables that should not survive between loops."""
        self.reader = self.writer = self.loop = self.query_lock = None

    def _create_request(self, target: str, cmd: str, arg: Optional[dict] = None):
        request: dict[str, Any] = {target: {cmd: arg}}

        return request

    async def _query_helper(
        self, target: str, cmd: str, arg: Optional[dict] = None
    ) -> Any:
        """Query device, return results or raise an exception.

        :param target: Target system {system, time, emeter, ..}
        :param cmd: Command to execute
        :param arg: payload dict to be send to the device
        :param child_ids: ids of child devices
        :return: Unwrapped result for the call.
        """
        request = self._create_request(target, cmd, arg)

        try:
            response = await self.query(request=request)
        except Exception as ex:
            raise ValueError(f"Communication error on {target}:{cmd}") from ex

        if target not in response:
            raise ValueError(f"No required {target} in response: {response}")

        result = response[target]
        if "err_code" in result and result["err_code"] != 0:
            raise ValueError(f"Error on {target}.{cmd}: {result}")

        if cmd not in result:
            raise ValueError(f"No command in response: {response}")
        result = result[cmd]
        if "err_code" in result and result["err_code"] != 0:
            raise ValueError(f"Error on {target} {cmd}: {result}")

        if "err_code" in result:
            del result["err_code"]

        return result
