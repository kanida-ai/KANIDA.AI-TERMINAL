"""Unit tests for idempotency_key generation."""
from falcon.trade.services.order_executor import idempotency_key


def test_key_under_kite_tag_limit():
    """Kite tag limit is 20 chars."""
    k = idempotency_key("btc_2026_05_06_a3f9c2", "HFCL", "ENTRY")
    assert len(k) <= 20
    assert k.startswith("f-")


def test_key_format_components():
    k = idempotency_key("btc_2026_05_06_a3f9c2", "HFCL", "ENTRY")
    parts = k.split("-")
    assert parts[0] == "f"
    assert parts[-1] == "e"   # ENTRY → e


def test_long_symbol_truncated():
    k = idempotency_key("btc_2026_05_06_a3f9c2", "VERYLONGSYMBOLNAME", "ENTRY")
    assert len(k) <= 20


def test_dashes_and_amp_in_symbol_stripped():
    """Some Indian symbols have dashes / ampersands (e.g. M&M-EQ); stripped to fit."""
    k = idempotency_key("btc_2026_05_06_a3f9c2", "M&M-EQ", "ENTRY")
    # No leftover & or - other than the structural separators (positions 1, batch_end, sym_end)
    structural = k.split("-")
    assert "&" not in k
    # Symbol part shouldn't contain dashes:
    assert "-" not in structural[2]


def test_all_roles_produce_distinct_keys_same_batch():
    e = idempotency_key("btc_2026_05_06_a3f9c2", "HFCL", "ENTRY")
    s = idempotency_key("btc_2026_05_06_a3f9c2", "HFCL", "STOP")
    k = idempotency_key("btc_2026_05_06_a3f9c2", "HFCL", "SMOKE")
    x = idempotency_key("btc_2026_05_06_a3f9c2", "HFCL", "EXIT")
    assert len({e, s, k, x}) == 4    # all distinct
    assert e.endswith("-e")
    assert s.endswith("-s")
    assert k.endswith("-k")          # SMOKE → k (NOT s — would collide with STOP)
    assert x.endswith("-x")


def test_different_batches_produce_different_keys():
    a = idempotency_key("btc_2026_05_06_aaa", "HFCL", "ENTRY")
    b = idempotency_key("btc_2026_05_06_bbb", "HFCL", "ENTRY")
    assert a != b


def test_smoke_batch_suffix_does_not_alias_main_stop():
    """In practice smoke batches use a _smk suffix (different batch_id);
    even if role chars collided, batch_short would differ. Belt-and-suspenders."""
    smoke_key = idempotency_key("btc_2026_05_06_aaa_smk", "HFCL", "SMOKE")
    stop_key  = idempotency_key("btc_2026_05_06_aaa",     "HFCL", "STOP")
    assert smoke_key != stop_key


def test_lowercase_invariant():
    k = idempotency_key("BTC_2026_05_06_A3F9C2", "hfcl", "entry")
    assert k == k.lower()


def test_unknown_role_marked_with_question():
    k = idempotency_key("btc_2026_05_06_aaa", "HFCL", "BOGUS")
    assert k.endswith("-?")
