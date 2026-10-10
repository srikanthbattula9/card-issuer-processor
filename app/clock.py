"""A clock abstraction so time-dependent logic (ACH return windows) can be
tested by advancing a simulated clock instead of sleeping or backdating
database rows. SystemClock is what the app uses at runtime; SimulatedClock
is what tests use."""
from datetime import datetime, timedelta, timezone


class Clock:
    def now(self) -> datetime:
        raise NotImplementedError


class SystemClock(Clock):
    def now(self) -> datetime:
        return datetime.now(timezone.utc)


class SimulatedClock(Clock):
    """Time only moves when told to."""

    def __init__(self, start: datetime | None = None):
        self._now = start or datetime.now(timezone.utc)

    def now(self) -> datetime:
        return self._now

    def advance(self, delta: timedelta) -> None:
        self._now += delta

    def set(self, when: datetime) -> None:
        self._now = when
