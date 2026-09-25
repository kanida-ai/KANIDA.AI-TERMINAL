"""Strategy intents — the intake through which an EXTERNAL strategy app (the kanida-app Strategy Builder) hands
autotrade a ready-made multi-leg option basket. Autotrade, not the caller, owns execution.

Locked by construction (2026-09-24, owner decision "locked bridge, defined-risk shorts only"):
  * Additive: a new package + a new router. Nothing in the session / ladder / exit paths changes.
  * DRY RUN is the default and the only mode that works until the owner flips every live gate:
      FALCON_AUTOTRADE_ENABLED, FALCON_AUTOTRADE_OPTIONS_ENABLED, AUTOTRADE_STRATEGY_INTENTS_LIVE,
      the broker listed in AUTOTRADE_STRATEGY_INTENTS_CERTIFIED (on top of registry.is_certified),
      an unexpired OPERATOR ARM for that user + broker account, market hours, and a verified margin check.
    A live request that fails any gate is REFUSED (state 'blocked') - never silently downgraded to a dry run.
  * Defined risk only: every short leg is covered (per option type, long qty >= short qty) and every BUY group
    fills completely before any SELL group is sent.
  * Idempotent on (source, idempotency_key); a replay with a different payload is a 409.
  * Arming needs a SEPARATE secret (FALCON_OPERATOR_ARM_TOKEN) that the calling app never holds.
"""
