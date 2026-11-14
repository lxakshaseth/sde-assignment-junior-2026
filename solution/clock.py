"""Simulated clock and calling window calculations."""
from __future__ import annotations

import asyncio
import time
from datetime import datetime, timedelta, timezone
from typing import Optional, Union

from .models import IST, DAYS


def hm(s: str) -> tuple[int, int]:
    h, m = s.split(":")
    return int(h), int(m)


def in_window(
    t: datetime,
    start: str,
    end: str,
    days: Optional[list[str]] = None,
    grace_s: float = 0,
) -> bool:
    """True if t is within [start, end) on an allowed day, tolerating grace_s either side."""
    for probe in (t, t - timedelta(seconds=grace_s), t + timedelta(seconds=grace_s)):
        lt = probe.astimezone(IST)
        if days is not None and DAYS[lt.weekday()] not in days:
            continue
        sh, sm = hm(start)
        eh, em = hm(end)
        s = lt.replace(hour=sh, minute=sm, second=0, microsecond=0)
        e = lt.replace(hour=eh, minute=em, second=0, microsecond=0)
        if s <= lt < e:
            return True
    return False


def next_open(t: datetime, start: str, end: str, days: list[str]) -> datetime:
    """Earliest instant >= t that is inside the window on an allowed day."""
    lt = t.astimezone(IST)
    for d in range(0, 9):
        day = (lt + timedelta(days=d)).replace(second=0, microsecond=0)
        if DAYS[day.weekday()] not in days:
            continue
        sh, sm = hm(start)
        eh, em = hm(end)
        s = day.replace(hour=sh, minute=sm)
        e = day.replace(hour=eh, minute=em)
        if d == 0:
            if lt < s:
                return s
            if lt < e:
                return lt
            continue
        return s
    return lt


def next_eligible_time(
    t: datetime,
    camp_start: str,
    camp_end: str,
    camp_days: list[str],
    reg_start: str = "09:00",
    reg_end: str = "21:00",
) -> datetime:
    """
    Earliest instant >= t where BOTH the campaign window AND the regulatory window are open.
    """
    candidate = t.astimezone(IST)
    reg_days = DAYS  # regulatory window is open every day
    for _ in range(14 * 24):  # safety iteration limit across 14 days
        # Check if candidate is inside both windows
        in_camp = in_window(candidate, camp_start, camp_end, camp_days, grace_s=0)
        in_reg = in_window(candidate, reg_start, reg_end, reg_days, grace_s=0)
        if in_camp and in_reg:
            return candidate

        # If not, find next opening of each
        next_camp = next_open(candidate, camp_start, camp_end, camp_days)
        next_reg = next_open(candidate, reg_start, reg_end, reg_days)

        # Candidate jumps to the later of the openings
        adv = max(candidate + timedelta(minutes=1), next_camp, next_reg)
        if adv == candidate:
            adv = candidate + timedelta(minutes=1)
        candidate = adv

    return candidate


class SimClock:
    """Simulated clock synced with provider API or driven manually."""

    def __init__(
        self,
        sim_start: datetime,
        speed: float = 600.0,
        real_start: Optional[float] = None,
        manual: bool = False,
    ):
        self.sim_start = sim_start.astimezone(IST)
        self.speed = float(speed)
        self.real_start = real_start if real_start is not None else time.time()
        self.manual = manual
        self._manual_now: datetime = self.sim_start

    def now(self) -> datetime:
        if self.manual:
            return self._manual_now
        elapsed_real = time.time() - self.real_start
        return self.sim_start + timedelta(seconds=elapsed_real * self.speed)

    def set_sim_time(self, t: datetime):
        """For manual mode / tests."""
        self._manual_now = t.astimezone(IST)

    def advance_sim(self, seconds: float):
        """Advance simulated time by seconds."""
        if self.manual:
            self._manual_now += timedelta(seconds=seconds)
        else:
            self.sim_start += timedelta(seconds=seconds)

    def sync_from_provider(self, sim_epoch: str, real_epoch: float, speed: float):
        """Update clock sync from GET /v1/clock response."""
        self.sim_start = datetime.fromisoformat(sim_epoch).astimezone(IST)
        self.real_start = float(real_epoch)
        self.speed = float(speed)

    async def sleep_sim(self, sim_seconds: float):
        """Sleep for sim_seconds in simulated time (sim_seconds / speed in real time)."""
        if self.manual or self.speed <= 0:
            self.advance_sim(sim_seconds)
            await asyncio.sleep(0)
            return
        real_sleep = max(0.0, sim_seconds / self.speed)
        await asyncio.sleep(real_sleep)

    def real_seconds_until(self, target_sim: datetime) -> float:
        """Calculate real seconds until target simulated time."""
        current = self.now()
        diff = (target_sim.astimezone(IST) - current).total_seconds()
        if diff <= 0:
            return 0.0
        return diff / self.speed
