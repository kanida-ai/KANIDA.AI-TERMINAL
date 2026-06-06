from __future__ import annotations

import mimetypes
import subprocess
import sys
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from engine.config import load_config
from engine.viewer.data_service import PrototypeDataService, to_json_bytes


PACKAGE_ROOT = Path(__file__).resolve().parent
STATIC_ROOT = PACKAGE_ROOT / "static"


class ViewerHandler(BaseHTTPRequestHandler):
    service = PrototypeDataService()

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path == "/" or parsed.path == "/index.html":
            return self._serve_static("index.html")
        if parsed.path.startswith("/static/"):
            return self._serve_static(parsed.path.replace("/static/", "", 1))
        if parsed.path == "/api/health":
            return self._json({"ok": True})
        if parsed.path == "/api/summary":
            data = self.service.load_all()
            market = parse_qs(parsed.query).get("market", ["COMBINED"])[0]
            summary = data["summary"]
            snapshot_status = data.get("snapshot_status", {})
            if market.upper() != "COMBINED":
                market_summary = summary["markets"].get(market, {})
                return self._json({"market": market, "summary": market_summary, "snapshot_status": snapshot_status})
            return self._json({"market": "COMBINED", "summary": summary["combined"], "markets": summary["markets"], "snapshot_status": snapshot_status})
        if parsed.path == "/api/vision-audit":
            return self._json(self.service.load_all()["vision_audit"])
        if parsed.path == "/api/opportunities":
            market = parse_qs(parsed.query).get("market", ["COMBINED"])[0]
            rows = self.service.filter_market(self.service.load_all()["live_matches"], market)
            return self._json(rows)
        if parsed.path == "/api/outcome-opportunities":
            query = parse_qs(parsed.query)
            market = query.get("market", ["COMBINED"])[0]
            direction = query.get("direction", ["ALL"])[0]
            return self._json(self.service.outcome_opportunities(market, direction))
        if parsed.path == "/api/stocks":
            query = parse_qs(parsed.query)
            market = query.get("market", ["COMBINED"])[0]
            q = query.get("q", [""])[0].strip().upper()
            rows = self.service.filter_market(self.service.load_all()["stocks"], market)
            if q:
                rows = [r for r in rows if q in r["ticker"]]
            return self._json(rows)
        if parsed.path.startswith("/api/stocks/"):
            parts = [p for p in parsed.path.split("/") if p]
            if len(parts) < 4:
                return self._json({"error": "invalid stock path"}, status=HTTPStatus.BAD_REQUEST)
            _, _, market, ticker = parts[:4]
            query = parse_qs(parsed.query)
            timeframe = query.get("timeframe", [None])[0]
            bias = query.get("bias", [None])[0]
            return self._json(self.service.stock_detail(market, ticker, timeframe, bias))
        if parsed.path.startswith("/api/context/"):
            parts = [p for p in parsed.path.split("/") if p]
            if len(parts) < 4:
                return self._json({"error": "invalid context path"}, status=HTTPStatus.BAD_REQUEST)
            market, ticker = parts[2], parts[3]
            return self._json(self.service.stock_outcome_context(market, ticker))
        if parsed.path == "/api/snapshot-status":
            return self._json(self.service.snapshot_status())
        if parsed.path == "/api/insights":
            data = self.service.load_all()
            return self._json(
                {
                    "outcome_markdown": data["outcome_markdown"],
                    "insights_markdown": data["insights_markdown"],
                    "discovery_markdown": data["discovery_markdown"],
                }
            )
        return self._json({"error": "not found"}, status=HTTPStatus.NOT_FOUND)

    def do_POST(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path == "/api/refresh":
            result = run_refresh_jobs()
            self.service = PrototypeDataService(load_config())
            ViewerHandler.service = self.service
            return self._json(result)
        return self._json({"error": "not found"}, status=HTTPStatus.NOT_FOUND)

    def log_message(self, format: str, *args: object) -> None:  # noqa: A003
        return

    def _serve_static(self, relative_path: str) -> None:
        path = (STATIC_ROOT / relative_path).resolve()
        if not str(path).startswith(str(STATIC_ROOT.resolve())) or not path.exists():
            return self._json({"error": "static file not found"}, status=HTTPStatus.NOT_FOUND)
        content_type, _ = mimetypes.guess_type(str(path))
        body = path.read_bytes()
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", content_type or "application/octet-stream")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _json(self, payload, status: HTTPStatus = HTTPStatus.OK) -> None:
        body = to_json_bytes(payload)
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def run_refresh_jobs() -> dict[str, object]:
    commands = [
        [sys.executable, "-m", "engine.jobs.run_outcome_first"],
        [sys.executable, "-m", "engine.jobs.run_recalibration"],
        [sys.executable, "-m", "engine.jobs.run_live_match"],
    ]
    outputs = []
    for cmd in commands:
        proc = subprocess.run(
            cmd,
            cwd=str(Path(__file__).resolve().parents[2]),
            capture_output=True,
            text=True,
            check=False,
        )
        outputs.append(
            {
                "command": " ".join(cmd),
                "returncode": proc.returncode,
                "stdout_tail": proc.stdout[-1500:],
                "stderr_tail": proc.stderr[-1500:],
            }
        )
    ok = all(item["returncode"] == 0 for item in outputs)
    return {"ok": ok, "jobs": outputs}


def main(host: str = "127.0.0.1", port: int = 8765) -> None:
    server = ThreadingHTTPServer((host, port), ViewerHandler)
    print(f"KANIDA prototype viewer running at http://{host}:{port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
