"""Rewrite the Windows locations the move changed, inside config/.env/markdown files.

    python3 fix_paths.py [--write] FILE...

Only known prefixes are rewritten (to the ~/Kanida layout in config.sh); any other
``C:\\`` left behind is listed, never guessed at.
"""
import re
import sys
from pathlib import Path

H = Path.home()
K = H / "Kanida"
PREFIXES = [  # longest first
    (r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine", K / "engine"),
    (r"C:\Users\SPS\Documents\Kanida_Falcon", K / "Kanida_Falcon"),
    (r"C:\Users\SPS\Desktop\KANIDA.AI_TERMINAL", K / "KANIDA.AI_TERMINAL"),
    (r"C:\Users\SPS\Desktop\_kanida_deploy", K / "_kanida_deploy"),
    (r"C:\ProgramData\ms-playwright", H / "Library" / "Caches" / "ms-playwright"),
    (r"C:\Users\SPS\.cloudflared", H / ".cloudflared"),
    (r"C:\Users\SPS\anaconda3\python.exe", K / "engine" / ".venv" / "bin" / "python"),
]


def variants(win: str):
    yield win
    yield win.replace("\\", "\\\\")
    yield win.replace("\\", "/")


def fix(text: str) -> str:
    for win, mac in PREFIXES:
        for v in variants(win):
            # take the rest of the path too, and turn its separators around
            pat = re.compile(re.escape(v) + r"((?:[\\/]{1,2}[^\s\"'`,;]*)?)", re.IGNORECASE)
            text = pat.sub(lambda m: str(mac) + re.sub(r"[\\/]{1,2}", "/", m.group(1)), text)
    return text


def main() -> int:
    write = "--write" in sys.argv
    for name in [a for a in sys.argv[1:] if a != "--write"]:
        p = Path(name)
        try:
            old = p.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError):
            continue
        new = fix(old)
        if new != old:
            print(f"   {'fixed' if write else 'would fix'}: {p}")
            if write:
                p.write_text(new, encoding="utf-8")
        for i, line in enumerate(new.splitlines(), 1):
            if re.search(r"\b[A-Z]:[\\/]", line):
                print(f"   CHECK {p}:{i}: still has a Windows path")
    return 0


if __name__ == "__main__":
    sys.exit(main())
