"""KANIDA vendor-swappable market-data pipeline.

Public surface for everything downstream::

    from market_data import get_provider, RawCandle

    p = get_provider()                  # $MARKET_DATA_PROVIDER, default 'kite'
    bars = p.candles("RELIANCE", "15minute", start, end)

Nothing outside this package should import a concrete provider class — that is
what makes the vendor swap a config change (contract §1, owner decision 6).

Layout (contract §1/§2):
    types.py            normalized Instrument / RawCandle / errors / session maths
    ratelimit.py        one process-wide token bucket per vendor key
    provider.py         the Protocol, the registry, get_provider()
    kite_provider.py    Zerodha Kite historical (delay_seconds=0)
    vendor15_provider.py REST 15-minute delayed vendor (delay_seconds=900)
    fake_provider.py    deterministic fixtures for tests
    basis.py            does a symbol's history sit on two adjustment bases?
    quarantine.py       symbols the provider cannot serve, re-probed daily
    tests/              the conformance suite every provider must pass

``basis`` and ``quarantine`` are deliberately *not* re-exported here: both take
a ``MarketStore``, so importing them from the package root would drag the store
into every consumer that only wants a provider.  Import them by module.
"""
from .provider import (
    BaseProvider,
    MarketDataProvider,
    available_providers,
    clear_provider_cache,
    get_provider,
    register_provider,
)
from .ratelimit import TokenBucket, get_limiter
from .types import (
    IST,
    Instrument,
    ProviderError,
    QualityFlag,
    RateLimitError,
    RawCandle,
    TokenError,
    bars_per_session,
    session_close,
    session_grid,
    session_open,
    to_ist,
)

__version__ = "1.0.0"

__all__ = [
    "BaseProvider",
    "MarketDataProvider",
    "available_providers",
    "clear_provider_cache",
    "get_provider",
    "register_provider",
    "TokenBucket",
    "get_limiter",
    "IST",
    "Instrument",
    "ProviderError",
    "QualityFlag",
    "RateLimitError",
    "RawCandle",
    "TokenError",
    "bars_per_session",
    "session_close",
    "session_grid",
    "session_open",
    "to_ist",
    "__version__",
]
