"""Build the bounded gross-price pilot cache, without executing any detector."""
import argparse
import json
import os
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'server'))
from kanida_pilot.pattern_history import build_cache
from kanida_pilot.research_store import ResearchStore


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--symbols', nargs='*', help='Default: all frozen symbols; resumes completed stocks.')
    parser.add_argument('--workers', type=int, default=2)
    parser.add_argument('--cache', help='Separate output cache; never inside source research directory.')
    project = Path(__file__).resolve().parents[2]
    parser.add_argument('--research-directory', default=os.environ.get('PILOT_PATTERN_RESEARCH_DIRECTORY', str(project / 'market_scanner/output/expanded_research')))
    parser.add_argument('--run', default=os.environ.get('PILOT_PATTERN_RESEARCH_RUN', '4b33a5249562631524d6'))
    parser.add_argument('--catalogue', default=os.environ.get('PILOT_PATTERN_CATALOGUE_PATH', str(project / 'docs/pattern_research/IMPLEMENTED_CATALOGUE.json')))
    args = parser.parse_args()
    store = ResearchStore(args.research_directory, args.run, args.catalogue,
                          project / 'kanida-app/var/evidence_release.json')
    cache = args.cache or str(Path(__file__).resolve().parents[1] / 'var' / 'pattern_history.sqlite3')
    try:
        result = build_cache(store, cache, args.symbols,
            progress=lambda symbol, n: print(json.dumps(dict(symbol=symbol, completed=n)), flush=True),
            workers=args.workers)
        print(json.dumps({k: result[k] for k in ('version', 'run', 'cached_symbols', 'universe_count')}), flush=True)
    finally:
        store.close()


if __name__ == '__main__':
    main()
