"""5Paisa broker client — COMING-SOON PLACEHOLDER (NOT certified).

There is NO working 5Paisa adapter yet. This class exists ONLY so 5Paisa can be
a first-class registry entry (so the onboarding gallery lists it and the guided
page renders) while its AUTH stays a NotImplementedAuth placeholder — real orders
can never be placed. It subclasses the Dhan stub purely to satisfy the abstract
BrokerClient surface; none of its methods are wired to 5Paisa and it is never
instantiated for auth (registry binds it live=False with NotImplementedAuth).

Do NOT treat any inherited endpoint as a real 5Paisa endpoint. Certify a proper
adapter (5Paisa needs App Name / User ID / API Key / Encryption Key — a 4-field
schema that does NOT map to the 2-field vault) before flipping this live.
"""
from __future__ import annotations

from .dhan import DhanBroker


class FivePaisaBroker(DhanBroker):
    broker_name = "fivepaisa"
