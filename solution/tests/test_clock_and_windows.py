"""Tests for calling windows, regulatory windows, and clock math."""
from datetime import datetime, timedelta

import pytest

from solution.clock import SimClock, in_window, next_eligible_time, next_open
from solution.models import IST


def dt(year: int, month: int, day: int, hour: int, minute: int) -> datetime:
    return datetime(year, month, day, hour, minute, tzinfo=IST)


def test_in_window_basic():
    # Thursday 12 Nov 2026, 17:30
    t = dt(2026, 11, 12, 17, 30)

    # Renewals: 10:00 - 19:00, mon-sat -> should be True
    assert in_window(t, "10:00", "19:00", ["mon", "tue", "wed", "thu", "fri", "sat"]) is True

    # Renewals at 19:01 -> False
    assert in_window(dt(2026, 11, 12, 19, 1), "10:00", "19:00", ["mon", "tue", "wed", "thu", "fri", "sat"]) is False

    # Renewals at 09:59 -> False
    assert in_window(dt(2026, 11, 12, 9, 59), "10:00", "19:00", ["mon", "tue", "wed", "thu", "fri", "sat"]) is False

    # Exact start boundary (10:00) -> True ([start, end))
    assert in_window(dt(2026, 11, 12, 10, 0), "10:00", "19:00", ["mon", "tue", "wed", "thu", "fri", "sat"]) is True

    # Exact end boundary (19:00) -> False ([start, end))
    assert in_window(dt(2026, 11, 12, 19, 0), "10:00", "19:00", ["mon", "tue", "wed", "thu", "fri", "sat"]) is False


def test_in_window_days_filter():
    # Sunday 15 Nov 2026, 14:00
    t_sun = dt(2026, 11, 15, 14, 0)

    # Renewals window does not include 'sun' -> False
    assert in_window(t_sun, "10:00", "19:00", ["mon", "tue", "wed", "thu", "fri", "sat"]) is False

    # Feedback window includes 'sun' -> True
    assert in_window(t_sun, "11:00", "20:30", ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]) is True


def test_regulatory_window():
    # Regulatory: 09:00 - 21:00 every day
    reg_days = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]

    # 08:59 -> False
    assert in_window(dt(2026, 11, 12, 8, 59), "09:00", "21:00", reg_days) is False
    # 09:00 -> True
    assert in_window(dt(2026, 11, 12, 9, 0), "09:00", "21:00", reg_days) is True
    # 20:59 -> True
    assert in_window(dt(2026, 11, 12, 20, 59), "09:00", "21:00", reg_days) is True
    # 21:00 -> False
    assert in_window(dt(2026, 11, 12, 21, 0), "09:00", "21:00", reg_days) is False


def test_next_open_same_day():
    # Thursday 12 Nov 2026, 08:30
    t = dt(2026, 11, 12, 8, 30)
    # Window opens at 10:00 today
    opened = next_open(t, "10:00", "19:00", ["thu"])
    assert opened == dt(2026, 11, 12, 10, 0)


def test_next_open_next_day():
    # Thursday 12 Nov 2026, 19:30 (after close)
    t = dt(2026, 11, 12, 19, 30)
    # Should open Friday at 10:00
    opened = next_open(t, "10:00", "19:00", ["thu", "fri"])
    assert opened == dt(2026, 11, 13, 10, 0)


def test_next_open_skipping_days():
    # Friday 13 Nov 2026, 19:30 (after close)
    t = dt(2026, 11, 13, 19, 30)
    # Zen winback: mon-fri only. Next opening should be Monday 16 Nov at 09:30
    opened = next_open(t, "09:30", "18:00", ["mon", "tue", "wed", "thu", "fri"])
    assert opened == dt(2026, 11, 16, 9, 30)


def test_next_eligible_time_combining_reg_and_campaign():
    # Thursday 12 Nov 2026, 21:30 (after both regulatory and campaign windows)
    t = dt(2026, 11, 12, 21, 30)
    # Campaign window: 11:00 - 20:30, all days
    # Regulatory: 09:00 - 21:00
    # Both open at max(11:00, 09:00) = 11:00 next day
    eligible = next_eligible_time(
        t,
        camp_start="11:00",
        camp_end="20:30",
        camp_days=["mon", "tue", "wed", "thu", "fri", "sat", "sun"],
        reg_start="09:00",
        reg_end="21:00",
    )
    assert eligible == dt(2026, 11, 13, 11, 0)


def test_sim_clock_manual():
    clock = SimClock(
        sim_start=dt(2026, 11, 12, 17, 30),
        speed=600.0,
        manual=True,
    )
    assert clock.now() == dt(2026, 11, 12, 17, 30)

    clock.advance_sim(300)
    assert clock.now() == dt(2026, 11, 12, 17, 35)

    clock.set_sim_time(dt(2026, 11, 13, 10, 0))
    assert clock.now() == dt(2026, 11, 13, 10, 0)
