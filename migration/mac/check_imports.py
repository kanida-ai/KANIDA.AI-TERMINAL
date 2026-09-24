"""Find third-party modules the code imports but a venv can't import, and pin them.

    python3 check_imports.py <venv-python> <windows-freeze.txt> <dir>... [--report]

Prints ``name==version`` from the Windows freeze for every missing module it can map
(one per line, for the installer), or with --report a human summary including the ones
it could not map.  Local packages (a top-level name that exists as a folder or .py in
the scanned tree) are never reported.
"""
import ast
import json
import os
import re
import subprocess
import sys
from pathlib import Path

# import name -> distribution name, where they differ
ALIAS = {
    "sklearn": "scikit-learn", "yaml": "PyYAML", "dateutil": "python-dateutil", "bs4": "beautifulsoup4",
    "PIL": "Pillow", "cv2": "opencv-python", "dotenv": "python-dotenv", "jwt": "PyJWT",
    "kiteconnect": "kiteconnect", "telegram": "python-telegram-bot", "Crypto": "pycryptodome",
    "google": "google-api-python-client", "googleapiclient": "google-api-python-client",
    "multipart": "python-multipart", "jose": "python-jose", "magic": "python-magic",
    "talib": "TA-Lib", "pandas_ta": "pandas-ta", "docx": "python-docx", "pptx": "python-pptx",
    "websocket": "websocket-client", "socketio": "python-socketio", "serial": "pyserial",
    "attr": "attrs", "OpenSSL": "pyOpenSSL", "nacl": "PyNaCl", "zmq": "pyzmq", "win32api": None,
    "win32con": None, "win32com": None, "winreg": None, "msvcrt": None, "_winapi": None,
}
SKIP_DIRS = {"node_modules", ".venv", ".pilot-venv", "venv", "__pycache__", ".git", "output", "outputs",
             "research_outputs", "archive", "tests", "worktrees"}


def norm(s: str) -> str:
    return re.sub(r"[-_.]+", "-", s).lower()


def scan(dirs):
    names, local = set(), set()
    for d in dirs:
        root = Path(d)
        if not root.exists():
            continue
        for dp, dn, fn in os.walk(root):
            dn[:] = [x for x in dn if x not in SKIP_DIRS]
            for x in dn:
                local.add(x)
            for f in fn:
                if not f.endswith(".py"):
                    continue
                local.add(f[:-3])
                try:
                    tree = ast.parse(Path(dp, f).read_text(encoding="utf-8", errors="ignore"))
                except SyntaxError:
                    continue
                for node in ast.walk(tree):
                    if isinstance(node, ast.Import):
                        names.update(a.name.split(".")[0] for a in node.names)
                    elif isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
                        names.add(node.module.split(".")[0])
    return names - local


def main() -> int:
    report = "--report" in sys.argv
    args = [a for a in sys.argv[1:] if a != "--report"]
    py, freeze, dirs = args[0], args[1], args[2:]
    wanted = sorted(n for n in scan(dirs) if ALIAS.get(n, "x") is not None)
    probe = ("import importlib.util,json,sys;"
             "print(json.dumps([n for n in sys.argv[1:] if importlib.util.find_spec(n) is None]))")
    missing = json.loads(subprocess.run([py, "-c", probe, *wanted], capture_output=True, text=True).stdout or "[]")
    pins = {}
    for line in Path(freeze).read_text(encoding="utf-8-sig", errors="ignore").splitlines():
        if "==" in line:
            n, v = line.strip().split("==", 1)
            pins[norm(n)] = f"{n}=={v}"
    mapped, unmapped = [], []
    for m in missing:
        pin = pins.get(norm(ALIAS.get(m) or m))
        (mapped if pin else unmapped).append(pin or m)
    if report:
        print(f"   imports scanned: {len(wanted)}   missing now: {len(missing)}")
        for m in unmapped:
            print(f"   ? {m}  (imported by the code, not in the Windows freeze — may be dead code or stdlib-on-Windows)")
        for m in mapped:
            print(f"   ! {m}  still missing after install")
    else:
        print("\n".join(mapped))
    return 0


if __name__ == "__main__":
    sys.exit(main())
