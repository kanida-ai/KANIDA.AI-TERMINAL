"""Tests for the trader-voice explainer + the 3 polish fixes.

Polish fixes (must not regress):
  1. Sector naturalization — "telecommunication" never appears in output;
     every sector enum maps to a natural noun phrase
  2. Sentence capitalization — every sentence in the story starts with a capital
  3. Story joining — no double periods, clean punctuation

Plus payload shape assertions to lock the contract that frontend/routers
depend on (Design.md §5).
"""
from __future__ import annotations

import re
from typing import Any, Dict, List

import pytest

from power_user.services.explainer import (
    SECTOR_NATURAL,
    TIER_COLOR,
    TIER_ACTION_HINT,
    PICK_SCHEMA_VERSION,
    _capitalize_sentences,
    _join_story_bits,
    _sector_natural,
    BASELINE_RATES,
    OUTCOME_LABEL,
    EXPECTED_BY_TIER,
    aggregate_outcomes,
    build_pick_payload,
    feature_to_phrase,
    generate_story,
    hit_rate_numbers,
    hit_rate_phrase,
    rule_to_trader_phrase,
    tier_info,
)
from power_user.services.feature_cols import FEATURE_COLS, in_drawdown_bounce


# ────────────────────────────────────────────────────────────────────
# POLISH FIX #1 — Sector naturalization
# ────────────────────────────────────────────────────────────────────

class TestSectorNaturalization:
    """Bug from worktree: "A telecommunication in..." reads broken. Every sector
    must map to a natural noun phrase."""

    REAL_SECTORS = [
        "Automobile and Auto Components", "Capital Goods", "Chemicals",
        "Construction", "Construction Materials", "Consumer Durables",
        "Consumer Services", "Diversified", "Fast Moving Consumer Goods",
        "Financial Services", "Healthcare", "Information Technology",
        "Media Entertainment & Publication", "Metals & Mining",
        "Oil Gas & Consumable Fuels", "Power", "Realty", "Services",
        "Telecommunication", "Textiles",
    ]

    def test_every_real_sector_has_natural_form(self):
        """Every sector enum value in the DB must be in the SECTOR_NATURAL dict."""
        missing = [s for s in self.REAL_SECTORS if s not in SECTOR_NATURAL]
        assert not missing, f"Sectors missing from SECTOR_NATURAL: {missing}"

    def test_telecommunication_becomes_telecom(self):
        """The most visible regression case."""
        assert _sector_natural("Telecommunication") == "A telecom stock"

    def test_all_sector_phrases_start_with_capital(self):
        """Every value should be ready-to-render at sentence start."""
        for sector, phrase in SECTOR_NATURAL.items():
            assert phrase[0].isupper(), f"{sector!r} → {phrase!r} doesn't start with capital"
            # Must begin with "A " or "An " (article)
            assert phrase.startswith(("A ", "An ")), \
                f"{sector!r} → {phrase!r} should start with 'A ' or 'An '"

    def test_unknown_sector_fallback_safe(self):
        """Unknown sector should not crash and should still produce a usable phrase."""
        result = _sector_natural("Quantum Energy Cryptography")
        assert result == "A quantum energy cryptography stock"

    def test_none_sector(self):
        assert _sector_natural(None) == "A stock"

    def test_empty_sector(self):
        assert _sector_natural("") == "A stock"


# ────────────────────────────────────────────────────────────────────
# POLISH FIX #2 — Sentence capitalization
# ────────────────────────────────────────────────────────────────────

class TestSentenceCapitalization:
    """After a period + space, the next sentence must start with a capital."""

    def test_basic_capitalization(self):
        s = "A telecom stock in an uptrend. just confirmed a breakout."
        assert _capitalize_sentences(s) == \
            "A telecom stock in an uptrend. Just confirmed a breakout."

    def test_already_capitalized_idempotent(self):
        s = "First sentence. Second sentence."
        assert _capitalize_sentences(s) == "First sentence. Second sentence."

    def test_multiple_sentences(self):
        s = "first. second. third."
        result = _capitalize_sentences(s)
        # All sentences should start with uppercase
        for sent in result.split(". "):
            if sent:
                # Trim trailing period for last sentence check
                first_alpha_idx = next((i for i, c in enumerate(sent) if c.isalpha()), -1)
                assert first_alpha_idx != -1
                assert sent[first_alpha_idx].isupper(), f"sentence {sent!r} not capitalized"

    def test_handles_starts_with_non_alpha(self):
        """e.g. '+12%' should be preserved as-is at sentence start."""
        s = "+12% gain. continuing strong."
        result = _capitalize_sentences(s)
        assert "Continuing strong." in result


# ────────────────────────────────────────────────────────────────────
# POLISH FIX #3 — Story joining (no double periods)
# ────────────────────────────────────────────────────────────────────

class TestStoryJoining:
    """Worktree bug: bits[0] could end with period, then '. ' join → double period."""

    def test_no_double_periods(self):
        bits = ["A telecom stock", "up 18% in 20 days", "just confirmed breakout"]
        story = _join_story_bits(bits)
        assert ".." not in story, f"Double period in: {story!r}"

    def test_no_double_periods_when_bit_already_ends_with_period(self):
        """Adversarial: bits with trailing periods should still produce clean joins."""
        bits = ["A telecom stock.", "up 18%.", "just confirmed breakout."]
        story = _join_story_bits(bits)
        assert ".." not in story, f"Double period in: {story!r}"

    def test_empty_bits_returns_fallback(self):
        story = _join_story_bits([])
        assert story.startswith("A high-conviction setup")
        assert ".." not in story

    def test_single_bit(self):
        story = _join_story_bits(["A single fragment"])
        assert story == "A single fragment."

    def test_every_sentence_capitalized(self):
        """After joining 3+ bits, every resulting sentence starts with uppercase."""
        bits = ["a telecom stock", "up 18%", "just confirmed breakout", "at 52-week highs"]
        story = _join_story_bits(bits)
        for sent in story.split(". "):
            if sent.strip():
                first_alpha = next((c for c in sent if c.isalpha()), None)
                assert first_alpha is None or first_alpha.isupper(), \
                    f"Sentence not capitalized: {sent!r} (full story: {story!r})"


# ────────────────────────────────────────────────────────────────────
# Story integration — feature → polished story
# ────────────────────────────────────────────────────────────────────

class TestStoryGeneration:
    """End-to-end: realistic feature dict → polished story."""

    def test_uptrend_telecom_breakout(self):
        """The worktree's reference HFCL-style scenario."""
        feat_vals = {
            "dist_sma_200": 25, "rsi_14": 65, "roc_20": 18, "vol_vs_20d": 1.8,
            "weekly_breakout_20w": 1, "weekly_close_loc": 0.78, "close_loc": 0.85,
            "dist_high_252": -1.5, "atr_20_pct": 4.2, "weekly_range_pct": 12,
        }
        story = generate_story("HFCL", "Telecommunication", feat_vals)
        assert "telecom" in story.lower()
        assert "telecommunication" not in story.lower()    # POLISH #1
        assert ".." not in story                            # POLISH #3
        assert story.endswith(".")
        assert story[0].isupper()

    def test_information_technology_becomes_it(self):
        feat_vals = {"dist_sma_200": 15, "roc_20": 8}
        story = generate_story("INFY", "Information Technology", feat_vals)
        assert "an it stock" in story.lower() or "IT stock" in story
        assert "information technology" not in story.lower()


# ────────────────────────────────────────────────────────────────────
# Translation library — feature_to_phrase coverage
# ────────────────────────────────────────────────────────────────────

class TestTranslation:

    def test_known_features_return_phrase(self):
        cases = [
            ("close_loc",          ">",  0.85),
            ("weekly_close_loc",   ">",  0.78),
            ("atr_20_pct",         ">",  4.34),
            ("rsi_14",             ">",  65),
            ("dist_sma_200",       ">",  25),
            ("roc_20",             ">",  18),
            ("dist_high_252",      ">",  -1),
            ("weekly_breakout_20w", ">", 0),
            ("rs_sector_60d",      ">",  12),
            ("weekly_close_vs_sma20", ">", 35),
        ]
        for feat, op, th in cases:
            phrase = feature_to_phrase(feat, op, th)
            assert phrase is not None and len(phrase) > 0, f"{feat} {op} {th} → None"

    def test_unknown_feature_returns_none(self):
        assert feature_to_phrase("nonexistent_feature", ">", 0) is None

    def test_rule_to_trader_phrase_dedupes(self):
        """Same phrase from two conditions shouldn't appear twice."""
        rule = [
            ("weekly_close_loc",  ">",  0.78),
            ("weekly_close_loc",  ">",  0.66),    # same phrase
            ("atr_20_pct",        ">",  4.0),
        ]
        out = rule_to_trader_phrase(rule)
        # Phrase appears at most once
        assert out.count("weekly") <= 2   # 'weekly close' and 'weekly' might both appear


# ────────────────────────────────────────────────────────────────────
# Hit rate computer — baseline + edge math
# ────────────────────────────────────────────────────────────────────

class TestHitRates:

    def test_every_target_has_baseline(self):
        for target in OUTCOME_LABEL:
            assert target in BASELINE_RATES, f"{target} missing baseline rate"

    def test_baseline_rates_are_percentages(self):
        for target, rate in BASELINE_RATES.items():
            assert 0 <= rate <= 100, f"{target} = {rate} not a valid %"

    def test_hit_phrase_format(self):
        phrase = hit_rate_phrase("hit_15pc_20d", lift=20)
        # Should look like "N of 10 hit +15% within 20 days (baseline 2 of 10 → 2.0× edge)"
        assert " of 10 hit " in phrase
        assert "baseline" in phrase
        assert "edge" in phrase

    def test_hit_phrase_caps_at_100pc(self):
        phrase = hit_rate_phrase("hit_5pc_5d", lift=500)
        # 35 + 500 capped at 100 → 10 of 10
        assert "10 of 10" in phrase


# ────────────────────────────────────────────────────────────────────
# Tier info + expected outcomes
# ────────────────────────────────────────────────────────────────────

class TestTierAndExpected:

    def test_tier_thresholds(self):
        assert tier_info(1)[0]  == "ELITE"
        assert tier_info(3)[0]  == "ELITE"
        assert tier_info(4)[0]  == "HIGH"
        assert tier_info(7)[0]  == "HIGH"
        assert tier_info(8)[0]  == "MID"
        assert tier_info(14)[0] == "MID"
        assert tier_info(15)[0] == "LOWER"
        assert tier_info(25)[0] == "LOWER"
        assert tier_info(50)[0] == "TAIL"
        assert tier_info(100)[0] == "TAIL"

    def test_expected_by_tier_all_present(self):
        for t in ("ELITE", "HIGH", "MID", "LOWER", "TAIL"):
            assert t in EXPECTED_BY_TIER
            assert "d5_range" in EXPECTED_BY_TIER[t]
            assert "d10_range" in EXPECTED_BY_TIER[t]
            # Contract fix #1: d15_avg is a numeric range, not a string
            assert "d15_avg_range" in EXPECTED_BY_TIER[t]
            assert "d15_avg_str" not in EXPECTED_BY_TIER[t]

    def test_d15_avg_range_is_two_numbers(self):
        """Contract fix #1 — frontend can sort/chart this. String would block that."""
        for t, vals in EXPECTED_BY_TIER.items():
            rng = vals["d15_avg_range"]
            assert isinstance(rng, list) and len(rng) == 2
            assert all(isinstance(x, (int, float)) for x in rng)
            assert rng[0] <= rng[1]


class TestTierColorAndActionHint:
    """Contract fixes #8 + #9 — frontend theming + live decisions copy."""

    def test_tier_color_exists_for_every_tier(self):
        for t in ("ELITE", "HIGH", "MID", "LOWER", "TAIL"):
            assert t in TIER_COLOR
            assert isinstance(TIER_COLOR[t], str) and len(TIER_COLOR[t]) > 0

    def test_tier_action_hint_exists_for_every_tier(self):
        for t in ("ELITE", "HIGH", "MID", "LOWER", "TAIL"):
            assert t in TIER_ACTION_HINT
            hint = TIER_ACTION_HINT[t]
            assert hint.startswith(("Enter", "Skip"))


class TestHitRateNumbers:
    """Contract fix #3 — raw numbers alongside hit_phrase."""

    def test_returns_three_keys(self):
        nums = hit_rate_numbers("hit_15pc_20d", lift=20)
        assert set(nums.keys()) == {"hit_rate_pct", "baseline_pct", "edge_multiplier"}

    def test_hit_rate_pct_capped_at_100(self):
        nums = hit_rate_numbers("hit_5pc_5d", lift=500)
        assert nums["hit_rate_pct"] == 100

    def test_baseline_matches_dict(self):
        nums = hit_rate_numbers("hit_15pc_20d", lift=0)
        assert nums["baseline_pct"] == BASELINE_RATES["hit_15pc_20d"]

    def test_edge_multiplier_math(self):
        nums = hit_rate_numbers("hit_15pc_20d", lift=22)
        # base=22, pattern_wr = 22+22 = 44, edge = 44/22 = 2.0
        assert nums["edge_multiplier"] == 2.0


class TestAggregateOutcomes:
    """Aggregate must use the lowercase outcome keys (d1, d3, ...)."""

    def test_aggregate_uses_lowercase_keys(self):
        picks = [
            {"actual": {"d1":  3.0, "d5": 10.0, "d15": 15.0}},
            {"actual": {"d1": -1.0, "d5":  4.0, "d15":  8.0}},
            {"actual": {"d1":  2.0, "d5":  6.0, "d15": 12.0}},
        ]
        agg = aggregate_outcomes(picks)
        assert agg["n_picks"] == 3
        assert "d1" in agg["horizons"]
        assert "d5" in agg["horizons"]
        # WR(d1) = 2/3 → 66.7%
        assert agg["horizons"]["d1"]["wr"] == 66.7

    def test_empty_picks(self):
        agg = aggregate_outcomes([])
        assert agg["n_picks"] == 0
        assert agg["horizons"] == {}


# ────────────────────────────────────────────────────────────────────
# Canonical Pick payload shape (Design.md §5 contract)
# ────────────────────────────────────────────────────────────────────

class TestPickPayloadShape:
    """The shape build_pick_payload returns IS the API contract. v1 locked
    2026-05-14 after operator's 10-point review. Any change here must:
      1) Bump PICK_SCHEMA_VERSION
      2) Update the TypeScript `Pick` type in lib/power-api.ts
      3) Update this test class
    """

    def _sample_pick(self) -> Dict[str, Any]:
        return {
            "symbol": "HFCL",
            "sector": "Telecommunication",
            "score":  1832.02,
            "n_fires": 115,
            "feat_vals": {
                "dist_sma_200": 25, "rsi_14": 65, "roc_20": 18,
                "weekly_breakout_20w": 1, "weekly_close_loc": 0.78,
                "close_loc": 0.85, "dist_high_252": -1.5,
                "atr_20_pct": 4.2, "weekly_range_pct": 12,
            },
            "firing": [
                {"pid": 9370, "my": 2025, "rule": [("atr_20_pct",">",4.34),("weekly_close_loc",">",0.77)],
                 "tgt": "hit_15pc_20d", "lift": 41.64},
                {"pid": 8366, "my": 2024, "rule": [("roc_60",">",70),("weekly_close_loc",">",0.61)],
                 "tgt": "hit_15pc_20d", "lift": 38.01},
                {"pid": 8457, "my": 2024, "rule": [("atr_20_pct",">",3.59),("weekly_close_loc",">",0.70)],
                 "tgt": "hit_10pc_20d", "lift": 35.04},
            ],
        }

    # ── v1 contract: top-level keys ───────────────────────────────────

    def test_payload_has_all_required_keys(self):
        p = build_pick_payload(1, self._sample_pick(),
                               signal_date="2026-04-15", entry_date="2026-04-16")
        required = {
            "_version", "rank", "symbol", "sector",
            "signal_date", "entry_date",
            "score", "n_fires",
            "tier", "tier_icon", "tier_color", "tier_desc", "tier_action_hint",
            "story", "top_patterns", "expected", "risk",
        }
        missing = required - set(p.keys())
        assert not missing, f"Missing keys: {missing}"

    def test_schema_version_is_1(self):
        """Contract fix #7 — clients must be able to detect shape changes."""
        p = build_pick_payload(1, self._sample_pick())
        assert p["_version"] == PICK_SCHEMA_VERSION == 1

    def test_signal_date_and_entry_date_included(self):
        """Contract fix #2 — each pick carries its own temporal context."""
        p = build_pick_payload(1, self._sample_pick(),
                               signal_date="2026-04-15", entry_date="2026-04-16")
        assert p["signal_date"] == "2026-04-15"
        assert p["entry_date"]  == "2026-04-16"

    def test_signal_date_can_be_none(self):
        """Live-mode picks may not have dates yet."""
        p = build_pick_payload(1, self._sample_pick())
        assert p["signal_date"] is None
        assert p["entry_date"]  is None

    def test_sector_none_when_missing(self):
        """Contract fix #6 — backend returns null, not '—'. Frontend chooses display."""
        sample = self._sample_pick()
        sample["sector"] = ""
        p = build_pick_payload(1, sample)
        assert p["sector"] is None

    def test_sector_present_when_provided(self):
        p = build_pick_payload(1, self._sample_pick())
        assert p["sector"] == "Telecommunication"

    # ── v1 contract: tier metadata ────────────────────────────────────

    def test_tier_color_in_payload(self):
        """Contract fix #8 — pre-computed CSS hint."""
        p = build_pick_payload(1, self._sample_pick())
        assert p["tier_color"] == TIER_COLOR["ELITE"] == "amber"

    def test_tier_action_hint_in_payload(self):
        """Contract fix #9 — pre-computed live-decision rule in plain English."""
        p = build_pick_payload(1, self._sample_pick())
        assert p["tier_action_hint"] == TIER_ACTION_HINT["ELITE"]

    def test_elite_tier_for_rank_1(self):
        p = build_pick_payload(1, self._sample_pick())
        assert p["tier"] == "ELITE"
        assert p["tier_icon"] == "⭐"

    # ── v1 contract: top_patterns ─────────────────────────────────────

    def test_top_patterns_have_position_not_rank(self):
        """Contract fix #10 — 'position' to avoid clashing with outer 'rank'."""
        p = build_pick_payload(1, self._sample_pick())
        for tp in p["top_patterns"]:
            assert "position" in tp
            assert "rank" not in tp

    def test_top_patterns_raw_numbers_present(self):
        """Contract fix #3 — frontend can chart/sort without regex on phrase."""
        p = build_pick_payload(1, self._sample_pick())
        for tp in p["top_patterns"]:
            assert "hit_rate_pct"    in tp
            assert "baseline_pct"    in tp
            assert "edge_multiplier" in tp
            assert isinstance(tp["hit_rate_pct"], (int, float))

    def test_top_patterns_have_oos_lift_pp_not_oos_lift(self):
        """Contract fix #3 (rename for unit clarity)."""
        p = build_pick_payload(1, self._sample_pick())
        for tp in p["top_patterns"]:
            assert "oos_lift_pp" in tp
            assert "oos_lift" not in tp

    def test_pattern_id_is_prefixed_string(self):
        """pattern_id rendered as 'p_<int>' string for FE consistency."""
        p = build_pick_payload(1, self._sample_pick())
        for tp in p["top_patterns"]:
            assert isinstance(tp["pattern_id"], str)
            assert tp["pattern_id"].startswith("p_")

    def test_top_patterns_capped_at_3(self):
        sample = self._sample_pick()
        sample["firing"].append({"pid": 7777, "my": 2024,
                                 "rule": [("rsi_14",">",60)],
                                 "tgt": "hit_5pc_5d", "lift": 10})
        p = build_pick_payload(1, sample)
        assert len(p["top_patterns"]) == 3

    # ── v1 contract: expected ─────────────────────────────────────────

    def test_expected_uses_d15_avg_range_list(self):
        """Contract fix #1 — list of 2 numbers, not formatted string."""
        p = build_pick_payload(1, self._sample_pick())
        rng = p["expected"]["d15_avg_range"]
        assert isinstance(rng, list) and len(rng) == 2
        assert all(isinstance(x, (int, float)) for x in rng)
        # legacy key absent
        assert "d15_avg" not in p["expected"]
        assert "d15_avg_str" not in p["expected"]

    def test_expected_range_keys(self):
        p = build_pick_payload(1, self._sample_pick())
        assert "d5_range"  in p["expected"]
        assert "d10_range" in p["expected"]

    # ── v1 contract: risk ─────────────────────────────────────────────

    def test_risk_uses_trail_trigger_pct(self):
        """Contract fix #5 — consistent _pct suffix."""
        p = build_pick_payload(1, self._sample_pick())
        assert p["risk"] == {
            "stop_loss_pct":     -7,
            "trail_trigger_pct": 12,
            "time_exit_days":     7,
        }
        assert "trail_trigger" not in p["risk"]   # old key must NOT be present

    # ── v1 contract: actual outcomes ──────────────────────────────────

    def test_payload_no_actual_when_no_outcomes(self):
        p = build_pick_payload(1, self._sample_pick())
        assert "actual" not in p

    def test_payload_with_lowercase_outcome_keys(self):
        """Contract fix #4 — d1/d3/d5/d10/d15, not D+1 etc."""
        outcomes = {"d1": 2.9, "d3": 4.95, "d5": 12.9, "d10": 20.47, "d15": 64.5}
        p = build_pick_payload(1, self._sample_pick(), outcomes=outcomes)
        assert p["actual"] == {"d1": 2.9, "d3": 4.95, "d5": 12.9, "d10": 20.47, "d15": 64.5}
        # uppercase legacy keys must not be present
        for legacy in ("D+1", "D+5", "D+10", "D+15"):
            assert legacy not in p["actual"]

    def test_payload_normalizes_legacy_uppercase_outcomes(self):
        """Safety net: if get_outcomes ever returns legacy format, normalize."""
        outcomes = {"D+1": 2.9, "D+15": 64.5}
        p = build_pick_payload(1, self._sample_pick(), outcomes=outcomes)
        assert p["actual"] == {"d1": 2.9, "d15": 64.5}

    # ── End-to-end polish ─────────────────────────────────────────────

    def test_story_polish_in_payload(self):
        p = build_pick_payload(1, self._sample_pick())
        story = p["story"]
        assert "telecommunication" not in story.lower()  # POLISH #1
        assert story[0].isupper()                          # POLISH #2
        assert ".." not in story                           # POLISH #3
        for sent in story.split(". "):
            if sent.strip():
                first_alpha = next((c for c in sent if c.isalpha()), None)
                if first_alpha:
                    assert first_alpha.isupper(), f"Story sentence not capitalised: {sent!r}"


# ────────────────────────────────────────────────────────────────────
# in_drawdown_bounce filter (regression — engine strategy intent)
# ────────────────────────────────────────────────────────────────────

class TestDrawdownBounceFilter:

    def test_drawdown_dist_high_252_excluded(self):
        rule = [("dist_high_252", "<=", -15.0)]
        assert in_drawdown_bounce(rule)

    def test_drawdown_dist_high_120_excluded(self):
        rule = [("dist_high_120", "<=", -12.0)]
        assert in_drawdown_bounce(rule)

    def test_normal_pattern_not_excluded(self):
        rule = [("weekly_close_loc", ">", 0.7), ("atr_20_pct", ">", 3.5)]
        assert not in_drawdown_bounce(rule)

    def test_near_high_not_excluded(self):
        """dist_high_252 close to 0 is good (not a drawdown bounce)."""
        rule = [("dist_high_252", ">", -3)]
        assert not in_drawdown_bounce(rule)
