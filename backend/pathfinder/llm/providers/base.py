"""Shared provider plumbing. No vendor types cross this line."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from ..gateway import Usage


@dataclass(frozen=True)
class ProviderResult:
    """A parsed completion plus its metering, in the gateway's own shapes."""
    payload: dict[str, Any]
    usage: Usage
    model: str
    provider: str
