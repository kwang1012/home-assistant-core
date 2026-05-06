"""Rpi device door api."""

import asyncio
from typing import Any

from .device import RaspberryPiDevice


class RaspberryPiLock(RaspberryPiDevice):
    """RaspberryPiLock component."""

    LOCK_SERVICE = "pi.virtual.lock"
    SET_LOCK_METHOD = "transition_lock_state"

    async def get_lock_state(self) -> None:
        """Get lock state."""
        self._state = await self._query_helper(self.LOCK_SERVICE, "get_lock_state")

    @property
    def lock_state(self) -> dict[str, Any]:
        """Query the lock state."""
        lock_state: dict[str, Any] = self.sys_info["lock_state"]
        if lock_state is None:
            raise ValueError(
                "The device has no lock_state or you have not called update()"
            )

        return lock_state

    @property
    def changed_by(self) -> str | None:
        """Last change triggered by."""
        lock_state = self.lock_state
        return lock_state.get("changed_by")

    @property
    def code_format(self) -> str | None:
        """Regex for code format or None if no code is required."""
        lock_state = self.lock_state
        return lock_state.get("code_format")

    @property
    def is_locked(self) -> bool | None:
        """Return true if the lock is locked."""
        lock_state = self.lock_state
        return lock_state.get("is_locked")

    @property
    def is_locking(self) -> bool | None:
        """Return true if the lock is locking."""
        lock_state = self.lock_state
        return lock_state.get("is_locking")

    @property
    def is_unlocking(self) -> bool | None:
        """Return true if the lock is unlocking."""
        lock_state = self.lock_state
        return lock_state.get("is_unlocking")

    @property
    def is_jammed(self) -> bool | None:
        """Return true if the lock is jammed (incomplete locking)."""
        lock_state = self.lock_state
        return lock_state.get("is_jammed")

    async def lock(self, **kwargs: Any) -> None:
        """Lock the lock."""
        _state = {"on_off": 1, **kwargs}

        return await self._query_helper(self.LOCK_SERVICE, self.SET_LOCK_METHOD, _state)

    async def unlock(self, **kwargs: Any) -> None:
        """Unlock the lock."""
        _state = {"on_off": 0, **kwargs}

        return await self._query_helper(self.LOCK_SERVICE, self.SET_LOCK_METHOD, _state)

    async def open(self, **kwargs: Any) -> None:
        """Open the door latch."""
        _state = {"open": 1, **kwargs}

        return await self._query_helper(self.LOCK_SERVICE, self.SET_LOCK_METHOD, _state)


async def main():
    """Run lock device test."""
    device = RaspberryPiLock("127.0.0.1", 9999)
    await device.update()
    await device.lock()
    await asyncio.sleep(4)
    await device.update()
    await device.unlock()
    await asyncio.sleep(4)
    await device.update()


if __name__ == "__main__":
    asyncio.run(main())
