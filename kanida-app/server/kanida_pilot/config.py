from __future__ import annotations
from dataclasses import dataclass, field
import os
from pathlib import Path
from urllib.parse import urlparse
from dotenv import load_dotenv
from cryptography.fernet import Fernet

ROOT = Path(__file__).resolve().parents[2]

@dataclass
class Settings:
    origin: str = 'http://127.0.0.1:8082'
    origins: list[str] = field(default_factory=list)
    database_url: str = ''
    environment: str = 'local'
    owner_email: str = ''
    encryption_key: str = ''
    google_client_id: str = ''
    google_client_secret: str = ''
    razorpay_key_id: str = ''
    razorpay_key_secret: str = ''
    razorpay_webhook_secret: str = ''
    razorpay_plan_id: str = ''
    kite_api_key: str = ''
    kite_api_secret: str = ''
    research_url: str = 'http://127.0.0.1:8765'
    research_directory: str = ''
    # Last-good DISPLAY cache (BACKLOG item 2a). The last successful body of each read the app RENDERS, so a
    # scanner restart shows "the last scan, taken at ..." instead of an error page. Bounded and evicted
    # oldest-first (kanida_pilot/lastgood.py); it is a file so it survives a pilot restart. Never consulted by
    # anything that decides a trade - those reads pass no `cache` flag and still fail closed.
    last_good_path: str = str(ROOT/'var'/'last_good.sqlite3')
    # Expanded pattern research (docs/pattern_research). READ-ONLY: market_scanner/ is owned by the research
    # workers. `pattern_research_run` selects which frozen run the registry is pinned to; a card whose evidence
    # comes from a different run is labelled "Incompatible historical evidence" rather than served.
    pattern_research_directory: str = str(ROOT.parent/'market_scanner'/'output'/'expanded_research')
    pattern_catalogue_path: str = str(ROOT.parent/'docs'/'pattern_research'/'IMPLEMENTED_CATALOGUE.json')
    pattern_research_run: str = '4b33a5249562631524d6'
    pattern_index_path: str = str(ROOT/'var'/'research_index.sqlite3')
    pattern_history_path: str = str(ROOT/'var'/'pattern_history.sqlite3')
    # Shared instrument catalogue (`instrument_labels`), read-only: company + sector for researched rows, so they
    # read like stored-scan rows. Same table market_scanner/data.py reads. Absent = rows fall back to the symbol.
    pattern_labels_database: str = str(ROOT.parent/'db'/'kanida.db')
    # Operator-recorded publication decisions (EVIDENCE_SERVING_CONTRACT.md §9). Absent = "unreviewed".
    evidence_release_path: str = str(ROOT/'var'/'evidence_release.json')
    pattern_index_refresh_seconds: int = 1800
    # Live detections of the researched patterns (docs/LIVE_DETECTION.md §5A): the scanner's own detection
    # ledger inside its scan cache, READ-ONLY. Must match the scanner's SCANNER_CACHE_DB. Absent, or holding
    # no `detections` table (the scanner is running the legacy pattern set), means live detection is simply
    # off: Discover keeps serving the researched history and says so.
    pattern_detection_db: str = str(ROOT.parent/'market_scanner'/'output'/'scanner.sqlite3')
    # F&O store for the Derivative tab (docs/DERIVATIVES_SPEC.md §2). READ-ONLY, opened mode=ro: the capture
    # and metrics workers in market_data/derivatives/ own every write. Absent = the tab says "No F&O data
    # captured yet" instead of failing; it is NEVER db/kanida.db.
    derivatives_database: str = str(ROOT.parent/'db'/'derivatives.db')
    # How long one aggregate over the ledger is reused. A research pass lands every few minutes at best
    # (LIVE_DETECTION.md §7: 2m26s for 1H), so a short cache costs nothing in freshness.
    pattern_detection_cache_seconds: int = 20
    live_enabled: bool = False
    live_activation: str = ''
    policy_version: str = 'private-pilot-v1'
    app_scheme: str = 'kanida'
    web_directory: str = str(ROOT/'dist')
    # PILOT_MAX_DATA_AGE_DAYS: stored market data older than this many calendar days (IST) cannot be
    # simulated or sent live. Matches DATA_STALE_DAYS in src/decision.ts.
    max_data_age_days: int = 3

    @property
    def secure(self): return self.origin.startswith('https://')
    @property
    def google_ready(self): return bool(self.google_client_id and self.google_client_secret)
    @property
    def billing_ready(self):
        return bool(self.razorpay_key_id.startswith('rzp_test_') and self.razorpay_key_secret and self.razorpay_webhook_secret and self.razorpay_plan_id)
    @property
    def kite_ready(self): return bool(self.kite_api_key and self.kite_api_secret)
    @property
    def mobile_origin(self):
        if self.environment=='local':
            for origin in self.origins:
                if urlparse(origin).hostname not in ('localhost','127.0.0.1','::1'):return origin
        return self.origin

    @classmethod
    def load(cls):
        load_dotenv(ROOT/'.env.pilot', override=False)
        data={}
        for name in cls.__dataclass_fields__:
            if name in ('origins','live_enabled'): continue
            value=os.getenv('PILOT_'+name.upper())
            if value is not None: data[name]=value
        data['live_enabled']=os.getenv('PILOT_LIVE_ENABLED','false').lower()=='true'
        settings=cls(**data)
        try: settings.max_data_age_days=int(settings.max_data_age_days)
        except (TypeError,ValueError): raise ValueError('PILOT_MAX_DATA_AGE_DAYS must be a whole number of days') from None
        if not 0<=settings.max_data_age_days<=30: raise ValueError('PILOT_MAX_DATA_AGE_DAYS must be between 0 and 30')
        try: settings.pattern_index_refresh_seconds=int(settings.pattern_index_refresh_seconds)
        except (TypeError,ValueError): raise ValueError('PILOT_PATTERN_INDEX_REFRESH_SECONDS must be a whole number of seconds') from None
        if not 60<=settings.pattern_index_refresh_seconds<=86400: raise ValueError('PILOT_PATTERN_INDEX_REFRESH_SECONDS must be between 60 and 86400')
        try: settings.pattern_detection_cache_seconds=int(settings.pattern_detection_cache_seconds)
        except (TypeError,ValueError): raise ValueError('PILOT_PATTERN_DETECTION_CACHE_SECONDS must be a whole number of seconds') from None
        if not 0<=settings.pattern_detection_cache_seconds<=3600: raise ValueError('PILOT_PATTERN_DETECTION_CACHE_SECONDS must be between 0 and 3600')
        settings.origin=settings.origin.rstrip('/')
        settings.owner_email=settings.owner_email.strip().lower()
        settings.origins=list(dict.fromkeys([settings.origin]+[s.strip().rstrip('/') for s in os.getenv('PILOT_ORIGINS','').split(',') if s.strip()]))
        if settings.environment!='local' and not settings.secure:
            raise ValueError('Hosted pilot requires an HTTPS origin')
        if settings.razorpay_key_id and not settings.razorpay_key_id.startswith('rzp_test_'):
            raise ValueError('Only Razorpay test credentials are permitted in this pilot')
        if not settings.encryption_key:
            if settings.environment!='local': raise ValueError('PILOT_ENCRYPTION_KEY is required')
            directory=ROOT/'var';directory.mkdir(exist_ok=True)
            keyfile=directory/'pilot.key'
            if not keyfile.exists():
                with keyfile.open('x') as f: f.write(Fernet.generate_key().decode())
            settings.encryption_key=keyfile.read_text().strip()
        Fernet(settings.encryption_key.encode())
        if not settings.database_url:
            if settings.environment!='local': raise ValueError('PILOT_DATABASE_URL is required')
            (ROOT/'var').mkdir(exist_ok=True)
            settings.database_url='sqlite:///'+str(ROOT/'var'/'pilot.sqlite3')
        for origin in settings.origins:
            parsed=urlparse(origin)
            if parsed.scheme not in ('http','https') or not parsed.netloc or parsed.path:
                raise ValueError('Origins must be exact HTTP(S) origins')
        return settings
