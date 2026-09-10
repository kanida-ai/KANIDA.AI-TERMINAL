"""
Associative recall — "have I explored something like this before?"

**Recall is never evidence.** Every row points back to an authoritative row; when the
loop wants to know what a past experiment found, it reads the outcome, not the vector.
Recall only decides *where to look*.

The embedding is a deterministic hashed bag-of-tokens, not a learned one. P1 has no
embedding provider and a fake API call would be worse than an honest simple one; this
is enough to answer the novelty question over a corpus of tens of experiments, it runs
offline, and it is reproducible. `embed_model` records exactly what it is, so nobody
later mistakes it for a semantic embedding.
"""
from __future__ import annotations

import hashlib
import math
import re
from typing import Iterable, Sequence

EMBED_MODEL = "pathfinder.hashed_bow@1 (deterministic, not semantic)"
DIM = 256
TOKEN_RE = re.compile(r"[a-z0-9_]+")


def embed(text: str) -> list[float]:
    vec = [0.0] * DIM
    for tok in TOKEN_RE.findall(text.lower()):
        h = int(hashlib.blake2b(tok.encode(), digest_size=8).hexdigest(), 16)
        vec[h % DIM] += 1.0
    norm = math.sqrt(sum(v * v for v in vec)) or 1.0
    return [v / norm for v in vec]


def cosine(a: Sequence[float], b: Sequence[float]) -> float:
    return sum(x * y for x, y in zip(a, b))


def most_similar(
    query: str, corpus: Iterable[tuple[str, str, Sequence[float]]], *, top_k: int = 3
) -> list[tuple[str, str, float]]:
    """`corpus` is `(source_table, source_id, embedding)`. Returns pointers, never content."""
    q = embed(query)
    scored = [(t, i, cosine(q, e)) for t, i, e in corpus]
    scored.sort(key=lambda r: -r[2])
    return scored[:top_k]


#: Two hypotheses this similar are the same idea wearing a different hat.
NOVELTY_THRESHOLD = 0.92
