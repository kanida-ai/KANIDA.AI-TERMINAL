"""
Options Agent · chain-SNAPSHOT persistence seam.

WHY THIS FILE EXISTS AT ALL (docs/agents/options_STEP0_FINDINGS.md §3): ``instruments("NFO")``
returns **live contracts only** and Kite exposes **no historical instrument master**, so a past-date
option chain can never be reconstructed after the fact — the tokens/strikes/expiries that existed on
date D are simply not discoverable on D+1. Therefore **this store IS the evidence store**: the daily
snapshot is the only historical options data we will ever own, and a day not snapshotted is a day of
evidence permanently lost.

Two consequences are baked into the API:

  1. **Snapshots are IMMUTABLE.** ``write()`` of a (date, underlying) that already exists does NOT
     silently overwrite — it returns ``status="exists"`` and the caller reports it. Overwriting a
     past snapshot would rewrite history, which is indistinguishable from fabricating it. An explicit
     ``overwrite=True`` exists for operator-driven repair only and is always reported in the status.
  2. **``read()`` never raises.** Missing key / no credentials / network error / corrupt object all
     return ``None`` so the point-in-time loader degrades to an honest "no snapshot for that date"
     instead of a 500 (and, critically, never to a live fetch — see data.load_chain).

SWAPPABLE SOURCE — the same seam as the Chart Agent's screen store (agents/chart/screener.py):

    AGENT_OPTIONS_SNAP_URI   s3://bucket/prefix   -> S3SnapshotStore   (cloud default)
    AGENT_OPTIONS_SNAP_DIR   /some/dir            -> LocalSnapshotStore (dev; default
                                                    backend/var/options_chains)

Object layout (identical key scheme on both backends so local dev and cloud are interchangeable):

    <root>/<UNDERLYING>/chain_<UNDERLYING>_<YYYY-MM-DD>.json.gz     canonical document
    <root>/<UNDERLYING>/chain_<UNDERLYING>_<YYYY-MM-DD>.parquet     OPTIONAL columnar sidecar

FORMAT JUDGEMENT CALL (flagged for review): the canonical object is **gzipped JSON**, not Parquet.
A snapshot is a *document* — meta + the archived instrument master + the per-strike quote table +
the coverage/guard accounting — and Parquet cannot hold that nesting without splitting it into
several files that could then drift out of sync (a half-written snapshot is exactly the "permanent
poison" this module exists to prevent). The columnar access Parquet was wanted for is still provided,
as a **sidecar** holding the per-strike rows only, written best-effort AFTER the canonical object and
never load-bearing: ``read()`` only ever reads the JSON. Set AGENT_OPTIONS_SNAP_PARQUET=0 to skip it.

Execution boundary: this module does local-filesystem / S3 I/O only. No broker, no orders, no shell.
"""
from __future__ import annotations

import gzip
import json
import logging
import os

log = logging.getLogger("agents.options.store")

ENV_URI = "AGENT_OPTIONS_SNAP_URI"
ENV_DIR = "AGENT_OPTIONS_SNAP_DIR"
ENV_PARQUET = "AGENT_OPTIONS_SNAP_PARQUET"

SCHEMA = "kanida.options.chain_snapshot/1"


# --------------------------------------------------------------------------------- helpers
def _default_dir() -> str:
    """backend/agents/options/store.py -> backend/var/options_chains (local dev default)."""
    backend = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    return os.path.join(backend, "var", "options_chains")


def _safe(name: str) -> str:
    """Filesystem/S3-safe token for an underlying or a date (mirrors screener._safe)."""
    return "".join(c if (c.isalnum() or c in "-_.") else "_" for c in str(name))


def _rel_key(as_of: str, underlying: str, ext: str = "json.gz") -> str:
    u = _safe(underlying).upper()
    return f"{u}/chain_{u}_{_safe(as_of)}.{ext}"


def _encode(payload: dict) -> bytes:
    return gzip.compress(json.dumps(payload, separators=(",", ":"), default=str).encode("utf-8"))


def _decode(blob: bytes) -> dict:
    return json.loads(gzip.decompress(blob).decode("utf-8"))


def _parquet_enabled() -> bool:
    return str(os.environ.get(ENV_PARQUET, "1")).strip().lower() not in ("0", "false", "no", "off")


def _rows_frame(payload: dict):
    """The per-strike rows as a DataFrame, or None when pandas/rows are unavailable. Guarded."""
    try:
        rows = payload.get("rows") or []
        if not rows:
            return None
        import pandas as pd
        df = pd.DataFrame(rows)
        # Denormalize the snapshot identity onto every row so the sidecar is self-describing when
        # scanned as a partitioned dataset (duckdb read_parquet over <root>/**/*.parquet).
        df.insert(0, "as_of", str(payload.get("as_of")))
        df.insert(1, "underlying", str(payload.get("underlying")))
        return df
    except Exception as e:  # noqa: BLE001 — the sidecar is never load-bearing
        log.debug("rows_frame failed (sidecar skipped): %s", e)
        return None


# --------------------------------------------------------------------------------- local store
class LocalSnapshotStore:
    """[BUILT] Per-(date, underlying) gzipped-JSON snapshots on the local filesystem (dev default)."""

    kind = "local"

    def __init__(self, root: str):
        self.root = root

    # -- addressing -------------------------------------------------------------------
    def _path(self, as_of: str, underlying: str, ext: str = "json.gz") -> str:
        return os.path.join(self.root, *_rel_key(as_of, underlying, ext).split("/"))

    def uri(self, as_of: str, underlying: str) -> str:
        return self._path(as_of, underlying)

    def base_uri(self) -> str:
        return self.root

    # -- io ---------------------------------------------------------------------------
    def exists(self, as_of: str, underlying: str) -> bool:
        try:
            return os.path.exists(self._path(as_of, underlying))
        except Exception as e:  # noqa: BLE001
            log.debug("LocalSnapshotStore.exists(%s/%s) err: %s", underlying, as_of, e)
            return False

    def write(self, as_of: str, underlying: str, payload: dict, overwrite: bool = False) -> dict:
        """IMMUTABLE by default. Returns a status dict; never raises."""
        path = self._path(as_of, underlying)
        try:
            if os.path.exists(path) and not overwrite:
                return {"status": "exists", "written": False, "uri": path,
                        "reason": "snapshot already archived for this (date, underlying) — "
                                  "snapshots are immutable; pass overwrite=True to repair"}
            os.makedirs(os.path.dirname(path), exist_ok=True)
            blob = _encode(payload)
            # Write to a temp name then replace, so a crashed write can never leave a half object
            # that read() would later hand to the point-in-time loader as if it were evidence.
            tmp = path + ".tmp"
            with open(tmp, "wb") as f:
                f.write(blob)
            os.replace(tmp, path)
            out = {"status": "written", "written": True, "uri": path, "bytes": len(blob),
                   "overwrote": bool(overwrite and True), "parquet": None}
            out["parquet"] = self._write_sidecar(as_of, underlying, payload)
            return out
        except Exception as e:  # noqa: BLE001
            log.warning("LocalSnapshotStore.write(%s/%s) failed: %s", underlying, as_of, e)
            return {"status": "error", "written": False, "uri": path,
                    "reason": f"{type(e).__name__}: {e}"}

    def _write_sidecar(self, as_of: str, underlying: str, payload: dict):
        if not _parquet_enabled():
            return None
        df = _rows_frame(payload)
        if df is None:
            return None
        try:
            p = self._path(as_of, underlying, "parquet")
            df.to_parquet(p, index=False)
            return p
        except Exception as e:  # noqa: BLE001 — best-effort only
            log.debug("LocalSnapshotStore sidecar(%s/%s) skipped: %s", underlying, as_of, e)
            return None

    def read(self, as_of: str, underlying: str):
        """GUARDED: missing / corrupt / unreadable -> None, never an exception."""
        path = self._path(as_of, underlying)
        try:
            if not os.path.exists(path):
                return None
            with open(path, "rb") as f:
                return _decode(f.read())
        except Exception as e:  # noqa: BLE001
            log.debug("LocalSnapshotStore.read(%s/%s) miss/err: %s", underlying, as_of, e)
            return None


# --------------------------------------------------------------------------------- s3 store
class S3SnapshotStore:
    """[BUILT] S3 snapshot store (boto3 — already in the cloud image), same key scheme as local.

    ``read()`` is GUARDED — NoSuchKey / missing credentials / network error all return None so the
    point-in-time loader reports an honest miss instead of crashing (and never falls back to a live
    fetch). ``write()`` is IMMUTABLE: a head_object hit short-circuits to status="exists"."""

    kind = "s3"

    def __init__(self, uri: str):
        # The root is ``root_uri``, NOT ``uri`` — ``uri()`` is the per-snapshot addressing METHOD and
        # an instance attribute of the same name silently shadows it (a real bug, caught by tests).
        self.root_uri = str(uri).rstrip("/")
        rest = (self.root_uri[len("s3://"):] if self.root_uri.startswith("s3://")
                else self.root_uri)
        self.bucket, _, self.prefix = rest.partition("/")

    # -- addressing -------------------------------------------------------------------
    def _key(self, as_of: str, underlying: str, ext: str = "json.gz") -> str:
        pre = (self.prefix + "/") if self.prefix else ""
        return f"{pre}{_rel_key(as_of, underlying, ext)}"

    def uri_for(self, as_of: str, underlying: str, ext: str = "json.gz") -> str:
        return f"s3://{self.bucket}/{self._key(as_of, underlying, ext)}"

    def uri(self, as_of: str, underlying: str) -> str:
        return self.uri_for(as_of, underlying)

    def base_uri(self) -> str:
        return self.root_uri

    def _client(self):
        import boto3
        return boto3.client("s3")

    # -- io ---------------------------------------------------------------------------
    def exists(self, as_of: str, underlying: str) -> bool:
        """True only if the object is PROVABLY there. Any error -> False, and write() therefore
        proceeds; S3 has no atomic create-if-absent, so this is a best-effort immutability check and
        is documented as such rather than claimed to be a lock."""
        try:
            self._client().head_object(Bucket=self.bucket, Key=self._key(as_of, underlying))
            return True
        except Exception as e:  # noqa: BLE001 — 404 / creds / network
            log.debug("S3SnapshotStore.exists(%s/%s): %s", underlying, as_of, e)
            return False

    def write(self, as_of: str, underlying: str, payload: dict, overwrite: bool = False) -> dict:
        key = self._key(as_of, underlying)
        target = self.uri_for(as_of, underlying)
        try:
            if not overwrite and self.exists(as_of, underlying):
                return {"status": "exists", "written": False, "uri": target,
                        "reason": "snapshot already archived for this (date, underlying) — "
                                  "snapshots are immutable; pass overwrite=True to repair"}
            blob = _encode(payload)
            self._client().put_object(Bucket=self.bucket, Key=key, Body=blob,
                                      ContentType="application/json",
                                      ContentEncoding="gzip")
            out = {"status": "written", "written": True, "uri": target, "bytes": len(blob),
                   "overwrote": bool(overwrite), "parquet": None}
            out["parquet"] = self._write_sidecar(as_of, underlying, payload)
            return out
        except Exception as e:  # noqa: BLE001
            log.warning("S3SnapshotStore.write(%s/%s) failed: %s", underlying, as_of, e)
            return {"status": "error", "written": False, "uri": target,
                    "reason": f"{type(e).__name__}: {e}"}

    def _write_sidecar(self, as_of: str, underlying: str, payload: dict):
        if not _parquet_enabled():
            return None
        df = _rows_frame(payload)
        if df is None:
            return None
        try:
            import io
            buf = io.BytesIO()
            df.to_parquet(buf, index=False)
            key = self._key(as_of, underlying, "parquet")
            self._client().put_object(Bucket=self.bucket, Key=key, Body=buf.getvalue())
            return self.uri_for(as_of, underlying, "parquet")
        except Exception as e:  # noqa: BLE001 — best-effort only
            log.debug("S3SnapshotStore sidecar(%s/%s) skipped: %s", underlying, as_of, e)
            return None

    def read(self, as_of: str, underlying: str):
        try:
            obj = self._client().get_object(Bucket=self.bucket, Key=self._key(as_of, underlying))
            return _decode(obj["Body"].read())
        except Exception as e:  # noqa: BLE001 — NoSuchKey / creds / network / corrupt -> honest miss
            log.debug("S3SnapshotStore.read(%s/%s) miss/err: %s", underlying, as_of, e)
            return None


# --------------------------------------------------------------------------------- factory
def _store():
    """Resolve the active snapshot store: S3 when AGENT_OPTIONS_SNAP_URI is an s3:// URI, else the
    local directory (AGENT_OPTIONS_SNAP_DIR, default backend/var/options_chains).

    Deliberately NOT cached: tests and tooling flip the env in-process, and a cached store would
    keep serving the previous backend — the same footgun agents/chart/data.py keys its caches against.
    """
    uri = os.environ.get(ENV_URI, "")
    if uri.startswith("s3://"):
        return S3SnapshotStore(uri)
    root = os.environ.get(ENV_DIR) or _default_dir()
    return LocalSnapshotStore(root)


# --------------------------------------------------------------------------------- public API
def write(as_of, underlying: str, payload: dict, overwrite: bool = False) -> dict:
    """Archive ONE (date, underlying) chain snapshot. Immutable by default.

    Returns {status: written|exists|error, written: bool, uri, bytes?, parquet?, reason?}.
    Never raises — the caller (fetch_kite.snapshot_chain) reports the status verbatim."""
    return _store().write(str(as_of)[:10], str(underlying), payload, overwrite=overwrite)


def read(as_of, underlying: str):
    """The archived snapshot dict for (date, underlying), or None. FULLY GUARDED — a missing key,
    absent credentials, a network error or a corrupt object all return None, never an exception.

    The backends guard themselves, and this outer try/except guards the RESOLUTION too (a malformed
    AGENT_OPTIONS_SNAP_URI, an unimportable boto3): the point-in-time loader must degrade to an
    honest "no snapshot", never to an exception the router would have to swallow — and never to a
    live fetch."""
    try:
        return _store().read(str(as_of)[:10], str(underlying))
    except Exception as e:  # noqa: BLE001
        log.debug("store.read(%s,%s) guarded miss: %s", underlying, as_of, e)
        return None


def exists(as_of, underlying: str) -> bool:
    try:
        return bool(_store().exists(str(as_of)[:10], str(underlying)))
    except Exception as e:  # noqa: BLE001
        log.debug("store.exists(%s,%s) guarded false: %s", underlying, as_of, e)
        return False


def uri(as_of, underlying: str) -> str:
    """Where this (date, underlying) snapshot lives on the ACTIVE backend (diagnostics)."""
    return _store().uri(str(as_of)[:10], str(underlying))


def base_uri() -> str:
    """The active store root (an s3:// prefix or a local directory) — diagnostics."""
    return _store().base_uri()


def backend_kind() -> str:
    return _store().kind
