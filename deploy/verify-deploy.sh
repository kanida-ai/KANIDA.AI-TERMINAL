#!/usr/bin/env bash
# ════════════════════════════════════════════════════════════════════════
#  verify-deploy.sh — end-to-end production sanity check
# ════════════════════════════════════════════════════════════════════════
#
#  Hits every critical surface after the kanida.ai + api.kanida.ai
#  deploy is wired up.  Run from anywhere; it's all curl + grep.
#
#  Pass criteria:
#    • api.kanida.ai responds 200
#    • Lists 6 personas
#    • kanida.ai/power responds 200 (not 401 Basic Auth)
#    • All 6 persona detail pages render
#    • Featured-replays surface live
#
# ════════════════════════════════════════════════════════════════════════
set -u
FAIL=0
pass() { echo "  ✓ $1"; }
fail() { echo "  ✗ $1" >&2; FAIL=$((FAIL+1)); }

echo "═══ Backend tunnel: api.kanida.ai ═══"
code=$(curl -s -o /dev/null -w "%{http_code}" "https://api.kanida.ai/api/power/personas")
[ "$code" = "200" ] && pass "/api/power/personas → 200" || fail "/api/power/personas → $code (expected 200)"

count=$(curl -s "https://api.kanida.ai/api/power/personas" 2>/dev/null | grep -oE '"id"' | wc -l)
[ "$count" -ge 6 ] && pass "personas listed: $count (≥6 expected)" || fail "personas listed: $count (need ≥6)"

for slug in daily-trader patient-trader weekly-trader monthly-trader btst-trader falcon-top-10; do
    code=$(curl -s -o /dev/null -w "%{http_code}" "https://api.kanida.ai/api/power/portfolios/$slug")
    [ "$code" = "200" ] && pass "portfolios/$slug → 200" || fail "portfolios/$slug → $code"
done

featured=$(curl -s "https://api.kanida.ai/api/power/replay/featured" 2>/dev/null | grep -oE '"replay_date"' | wc -l)
[ "$featured" -ge 3 ] && pass "featured replays: $featured" || fail "featured replays: $featured (need ≥3)"

echo ""
echo "═══ Frontend: kanida.ai ═══"
code=$(curl -s -o /dev/null -w "%{http_code}" "https://kanida.ai/power")
[ "$code" = "200" ] && pass "kanida.ai/power → 200" || fail "kanida.ai/power → $code"
[ "$code" = "401" ] && echo "      Hint: Vercel Basic-Auth deployment protection still on. Turn it off." >&2

# Verify the Next app contains the v3 hero markers
body=$(curl -s "https://kanida.ai/power")
echo "$body" | grep -q "KANIDA.AI"        && pass "hero H1 'KANIDA.AI' present"     || fail "hero H1 missing"
echo "$body" | grep -q "AI co-trading"    && pass "hero H2 'AI co-trading' present" || fail "hero H2 missing"
echo "$body" | grep -q "Every pick answers 3 things" && pass "3-pillar section present" || fail "3-pillar section missing"

for slug in daily-trader falcon-top-10 monthly-trader; do
    code=$(curl -s -o /dev/null -w "%{http_code}" "https://kanida.ai/power/portfolios/$slug")
    [ "$code" = "200" ] && pass "kanida.ai/power/portfolios/$slug → 200" || fail "kanida.ai/power/portfolios/$slug → $code"
done

echo ""
echo "═══ DNS / TLS ═══"
nameservers=$(dig +short NS kanida.ai 2>/dev/null | head -2)
echo "$nameservers" | grep -q "cloudflare" && pass "DNS on Cloudflare: $(echo $nameservers | tr '\n' ' ')" \
                                          || fail "DNS not yet on Cloudflare: $nameservers"

apicname=$(dig +short api.kanida.ai 2>/dev/null | head -1)
echo "$apicname" | grep -qE "cfargotunnel|cloudflare" && pass "api.kanida.ai → tunnel ($apicname)" \
                                                      || fail "api.kanida.ai DNS wrong: $apicname"

# TLS cert
sni=$(echo | openssl s_client -connect api.kanida.ai:443 -servername api.kanida.ai 2>/dev/null | openssl x509 -noout -issuer 2>/dev/null)
echo "$sni" | grep -q "Google\|Cloudflare\|Let's Encrypt" && pass "TLS cert: $sni" || fail "TLS cert issue: $sni"

echo ""
echo "═══ Summary ═══"
if [ $FAIL -eq 0 ]; then
    echo "ALL CHECKS PASSED ✓  — kanida.ai/power is production-live."
    exit 0
else
    echo "$FAIL check(s) failed. See ✗ lines above." >&2
    exit 1
fi
