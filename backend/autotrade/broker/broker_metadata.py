"""Declarative per-broker ONBOARDING METADATA for the guided "add a broker" UI.

This is the single, data-driven source for everything the frontend needs to
render a broker card + a guided connect page WITHOUT a redeploy: display name,
a CSS logo chip (brand color + initial — NO external image URLs), the ordered
credential fields the user must enter, and the setup steps / docs / callback.

DESIGN: this module carries ONLY presentation + onboarding copy. The functional
facts (which adapter is live, the capability matrix, whether creds can be stored)
stay in registry.py / vault.py. `registry.list_supported()` MERGES this metadata
onto each registered broker. If a broker is registered but has no entry here, the
merge falls back to a minimal auto-generated meta (so the UI never breaks) —
adding rich onboarding copy is then a one-entry change here, no frontend redeploy.

CONTRACT (authoritative — the frontend is built against this):

  BrokerMeta = {
    broker, display_name, brand:{color,initial}, live, capabilities,
    exchanges:[...], fields:[FieldDef...], setup:{docs_url?, callback_url?,
    steps:[...], token_note?}
  }
  FieldDef = { name, label, type:"text"|"password", secret:bool, required:bool,
               placeholder?, maps_to:"api_key"|"api_secret" }

The two LIVE brokers (zerodha, rupeezy) map cleanly onto the 2-field vault
(api_key / api_secret). COMING-SOON shells (live=False) list their real field
schema so the guided page renders, but connect stays disabled and their fields
do NOT need to map to the vault yet (multi-field brokers like 5Paisa are
display-only until certified — their maps_to may be None).

Callback reality (verified against the auth providers 2026-07-05):
  * zerodha — login_url() does NOT inject redirect_uri; the Redirect URL is
    configured ON the Kite Connect app at developers.kite.trade and Kite lands
    the request_token there. So we surface that redirect as `callback_url` for
    the user to paste into their Kite app; the token is copy-pasted back here.
  * rupeezy — login_url() builds flow.rupeezy.in?applicationId=…; the redirect
    URL is configured ON the Vortex app and the `auth` param lands there for the
    user to copy-paste back. Same model: `callback_url` = the redirect to set on
    the Vortex app. Neither flow has a live server-side callback handler today,
    so both are effectively manual request-token copy-paste; the steps say so.

`callback_url` is resolved at read time from AUTOTRADE_OAUTH_REDIRECT (env) so a
deploy can point it at the real portal origin; default below is the portal path.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import List, Optional


# Default redirect the user configures on their broker app. The portal now serves
# a /power/autotrade/connect page that auto-captures the returned token.
# Verified 2026-07-05: the portal serves at www.kanida.ai (app.kanida.ai 404s;
# kanida.ai 301→www). Use the FINAL domain so the broker's OAuth redirect lands
# with no redirect hop (some brokers drop query params across a 301). Override per
# deployment via AUTOTRADE_OAUTH_REDIRECT.
_DEFAULT_REDIRECT = "https://www.kanida.ai/power/autotrade/connect"


def _redirect() -> str:
    return os.environ.get("AUTOTRADE_OAUTH_REDIRECT", _DEFAULT_REDIRECT).rstrip("/")


@dataclass(frozen=True)
class FieldDef:
    name: str
    label: str
    type: str              # "text" | "password"
    secret: bool
    required: bool
    maps_to: Optional[str]  # "api_key" | "api_secret" | None (display-only)
    placeholder: str = ""

    def to_dict(self) -> dict:
        d = {
            "name": self.name,
            "label": self.label,
            "type": self.type,
            "secret": self.secret,
            "required": self.required,
            "maps_to": self.maps_to,
        }
        if self.placeholder:
            d["placeholder"] = self.placeholder
        return d


@dataclass(frozen=True)
class BrokerMetadata:
    display_name: str
    color: str
    initial: str
    exchanges: List[str]
    fields: List[FieldDef]
    steps: List[str]
    docs_url: Optional[str] = None
    # If None, callback_url is resolved from the env redirect at read time; if
    # explicitly set to "" the broker has genuinely no redirect and it's omitted.
    callback_url: Optional[str] = None
    token_note: Optional[str] = None

    def to_dict(self, *, live: bool, capabilities: dict) -> dict:
        setup: dict = {"steps": list(self.steps)}
        if self.docs_url:
            setup["docs_url"] = self.docs_url
        cb = _redirect() if self.callback_url is None else self.callback_url
        if cb:
            setup["callback_url"] = cb
        if self.token_note:
            setup["token_note"] = self.token_note
        return {
            "brand": {"color": self.color, "initial": self.initial},
            "display_name": self.display_name,
            "exchanges": list(self.exchanges),
            "fields": [f.to_dict() for f in self.fields],
            "setup": setup,
        }


# ── LIVE brokers — real credential schemas from their auth providers ──────────

_ZERODHA = BrokerMetadata(
    display_name="Zerodha (Kite Connect)",
    color="#387ED1",
    initial="Z",
    exchanges=["NSE", "NFO"],
    fields=[
        FieldDef(
            name="api_key", label="API Key", type="text",
            secret=False, required=True, maps_to="api_key",
            placeholder="Kite Connect API Key",
        ),
        FieldDef(
            name="api_secret", label="API Secret", type="password",
            secret=True, required=True, maps_to="api_secret",
            placeholder="Kite Connect API Secret",
        ),
    ],
    docs_url="https://developers.kite.trade",
    # Redirect is configured ON the Kite app; Kite lands request_token there for
    # copy-paste (no live server handler) — surface it so the user sets it.
    callback_url=None,  # resolved from env redirect at read time
    token_note="Kite tokens expire ~06:00 IST daily — reconnect each morning "
               "(one click).",
    steps=[
        "Create a Kite Connect app at developers.kite.trade.",
        "Set the app's Redirect URL to {callback_url}.",
        "Copy the app's API Key and API Secret into the fields here.",
        "Click Test & Connect, then complete the Kite login when prompted.",
        "Kite redirects to your Redirect URL with a request_token — paste it "
        "back here to finish connecting.",
    ],
)

_RUPEEZY = BrokerMetadata(
    display_name="Rupeezy (Vortex API)",
    color="#00B386",
    initial="R",
    exchanges=["NSE", "BSE", "NFO", "MCX"],
    fields=[
        FieldDef(
            name="api_key", label="Application ID", type="text",
            secret=False, required=True, maps_to="api_key",
            placeholder="Vortex Application ID",
        ),
        FieldDef(
            name="api_secret", label="API Secret (x-api-key)", type="password",
            secret=True, required=True, maps_to="api_secret",
            placeholder="Vortex x-api-key / API secret",
        ),
    ],
    docs_url="https://vortex.rupeezy.in/",
    callback_url=None,  # resolved from env redirect at read time
    token_note="Rupeezy sessions are validated by a live health ping — "
               "reconnect if the account shows expired.",
    steps=[
        "Create a Vortex API app in the Rupeezy developer portal "
        "(vortex.rupeezy.in) to get your Application ID and x-api-key.",
        "Set the app's Redirect URL to {callback_url}.",
        "Copy the Application ID and x-api-key into the fields here.",
        "Click Test & Connect — you'll be sent to flow.rupeezy.in to log in.",
        "Rupeezy redirects to your Redirect URL with an `auth` token — paste it "
        "back here to finish connecting.",
    ],
)


# ── COMING-SOON shells (live=False) — schema + steps so the gallery renders ───

_UPSTOX = BrokerMetadata(
    display_name="Upstox",
    color="#5A2D82",
    initial="U",
    exchanges=["NSE", "NFO"],
    fields=[
        FieldDef(name="api_key", label="API Key", type="text",
                 secret=False, required=True, maps_to="api_key",
                 placeholder="Upstox API Key"),
        FieldDef(name="api_secret", label="API Secret", type="password",
                 secret=True, required=True, maps_to="api_secret",
                 placeholder="Upstox API Secret"),
    ],
    docs_url="https://upstox.com/developer/api-documentation",
    steps=[
        "Coming soon — Upstox onboarding is not yet certified.",
        "Create an app in the Upstox developer console to get your API Key and "
        "API Secret.",
        "Set the app's Redirect URL to {callback_url}.",
    ],
)

_DHAN = BrokerMetadata(
    display_name="Dhan",
    color="#1A73E8",
    initial="D",
    exchanges=["NSE", "NFO"],
    fields=[
        FieldDef(name="client_id", label="Client ID", type="text",
                 secret=False, required=True, maps_to="api_key",
                 placeholder="Dhan Client ID"),
        FieldDef(name="api_key", label="API Key", type="text",
                 secret=False, required=True, maps_to=None,
                 placeholder="Dhan API Key"),
        FieldDef(name="api_secret", label="API Secret", type="password",
                 secret=True, required=True, maps_to="api_secret",
                 placeholder="Dhan API Secret"),
    ],
    docs_url="https://dhanhq.co/docs/",
    steps=[
        "Coming soon — Dhan onboarding is not yet certified.",
        "Generate your Client ID, API Key and API Secret in the DhanHQ "
        "developer portal.",
    ],
)

_FIVEPAISA = BrokerMetadata(
    display_name="5Paisa",
    color="#E11B22",
    initial="5",
    exchanges=["NSE", "NFO"],
    # Display-only until certified: 5Paisa needs FOUR fields and does NOT map to
    # the 2-field vault yet, so maps_to is None on every field.
    fields=[
        FieldDef(name="app_name", label="App Name", type="text",
                 secret=False, required=True, maps_to=None,
                 placeholder="5Paisa App Name"),
        FieldDef(name="user_id", label="User ID", type="text",
                 secret=False, required=True, maps_to=None,
                 placeholder="5Paisa User ID"),
        FieldDef(name="api_key", label="API Key", type="text",
                 secret=False, required=True, maps_to=None,
                 placeholder="5Paisa API Key"),
        FieldDef(name="encryption_key", label="Encryption Key", type="password",
                 secret=True, required=True, maps_to=None,
                 placeholder="5Paisa Encryption Key"),
    ],
    docs_url="https://www.5paisa.com/developerapi/overview",
    steps=[
        "Coming soon — 5Paisa onboarding is not yet certified.",
        "Register a 5Paisa API app to get your App Name, User ID, API Key and "
        "Encryption Key.",
    ],
)


_METADATA = {
    "zerodha": _ZERODHA,
    "rupeezy": _RUPEEZY,
    "upstox": _UPSTOX,
    "dhan": _DHAN,
    "fivepaisa": _FIVEPAISA,
}


def get_metadata(broker: str) -> Optional[BrokerMetadata]:
    return _METADATA.get((broker or "").lower())


def _fallback_meta(broker: str) -> BrokerMetadata:
    """Minimal auto-generated meta for a registered broker with no rich entry —
    keeps the UI from breaking (default 2-field api_key/api_secret schema)."""
    return BrokerMetadata(
        display_name=broker.capitalize(),
        color="#64748B",
        initial=(broker[:1] or "?").upper(),
        exchanges=["NSE"],
        fields=[
            FieldDef(name="api_key", label="API Key", type="text",
                     secret=False, required=True, maps_to="api_key"),
            FieldDef(name="api_secret", label="API Secret", type="password",
                     secret=True, required=True, maps_to="api_secret"),
        ],
        steps=["Enter your API Key and API Secret."],
    )


def meta_dict(broker: str, *, live: bool, capabilities: dict) -> dict:
    """Return the presentation portion of the BrokerMeta contract for `broker`
    (brand/display_name/exchanges/fields/setup). Falls back to a minimal schema
    if the broker has no rich entry. Steps have {callback_url} substituted."""
    meta = get_metadata(broker) or _fallback_meta(broker)
    d = meta.to_dict(live=live, capabilities=capabilities)
    cb = d.get("setup", {}).get("callback_url", "")
    if cb:
        d["setup"]["steps"] = [s.replace("{callback_url}", cb)
                               for s in d["setup"]["steps"]]
    return d
