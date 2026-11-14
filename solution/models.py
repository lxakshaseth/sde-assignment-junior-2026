"""Data models and constants for the Campaign Dispatcher."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from enum import Enum
from typing import Optional

IST = timezone(timedelta(hours=5, minutes=30))
DAYS = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]
TERMINAL_DISPOSITIONS = {"interested", "not_interested", "wrong_number", "dnd"}


class LeadState(str, Enum):
    PENDING = "PENDING"
    DIALING = "DIALING"
    ACTIVE = "ACTIVE"
    CALLBACK_WAIT = "CALLBACK_WAIT"
    RETRY_WAIT = "RETRY_WAIT"
    COMPLETED = "COMPLETED"
    EXHAUSTED = "EXHAUSTED"
    DND_BLOCKED = "DND_BLOCKED"


class CallState(str, Enum):
    DIALING = "DIALING"
    ACTIVE = "ACTIVE"
    ENDED = "ENDED"
    ABORTED = "ABORTED"


class FinalState(str, Enum):
    COMPLETED = "completed"
    EXHAUSTED = "exhausted"
    DND_BLOCKED = "dnd_blocked"


@dataclass
class TenantConfig:
    tenant_id: str
    max_concurrent_calls: int


@dataclass
class CampaignWindow:
    start: str
    end: str
    days: list[str]


@dataclass
class CampaignConfig:
    campaign_id: str
    tenant_id: str
    leads_file: str
    window: CampaignWindow
    max_attempts: int


@dataclass
class LeadRecord:
    campaign_id: str
    lead_id: str
    phone: str
    customer_name: Optional[str]
    row_order: int
    state: str
    attempts: int
    callback_at: Optional[datetime]
    next_retry_at: Optional[datetime]
    last_status: Optional[str]
    last_disposition: Optional[str]
    last_call_sid: Optional[str]
    final_state: Optional[str]
