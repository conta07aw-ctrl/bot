#!/bin/bash
# Polymarket latency + geoblock test
#
# Purpose: compare VPS locations for direct (unproxied) access to the
# Polymarket CLOB. Produces two numbers that matter:
#
#   1. Latency distribution to the three endpoints we actually use
#      (GET /time, GET /book, POST /order). Multiple samples so we can
#      spot jitter and cold-socket hits.
#
#   2. Whether the write endpoint (POST /order) is reachable from this
#      host's IP at all, or whether it's blocked by Polymarket's
#      Cloudflare geo/ASN filter. A 400 or 401 means we got to the app
#      (IP allowed, request was just invalid). A 403 or a Cloudflare
#      challenge page means the IP is on the blocklist.
#
# Usage:
#   bash scripts/test-poly-latency.sh             # direct, no proxy
#   HTTPS_PROXY=http://user:pass@host:port \
#     bash scripts/test-poly-latency.sh           # through proxy
#
# Run on BOTH the current NYC3 VPS and the Mexico droplet, compare the
# two reports side by side.

set -u

CLOB_BASE="https://clob.polymarket.com"
GAMMA_BASE="https://gamma-api.polymarket.com"
SAMPLES=10

# Colors (only if stdout is a tty)
if [ -t 1 ]; then
  BOLD=$'\033[1m'; RED=$'\033[31m'; GRN=$'\033[32m'; YEL=$'\033[33m'
  BLU=$'\033[34m'; RST=$'\033[0m'
else
  BOLD=""; RED=""; GRN=""; YEL=""; BLU=""; RST=""
fi

PROXY_NOTE=""
if [ -n "${HTTPS_PROXY:-}" ]; then
  PROXY_NOTE=" (via proxy: ${HTTPS_PROXY//:\/\/*@/://***@})"
fi

echo ""
echo "${BOLD}=== Polymarket latency + geoblock test ===${RST}${PROXY_NOTE}"
echo ""

# --- who are we? ---
echo "${BOLD}1. Egress IP + geo${RST}"
EGRESS=$(curl -s --max-time 5 https://ifconfig.io 2>/dev/null || echo "unknown")
GEO=$(curl -s --max-time 5 "https://ipapi.co/${EGRESS}/json/" 2>/dev/null || echo "{}")
CC=$(echo "$GEO" | grep -o '"country_code":"[^"]*"' | cut -d'"' -f4)
CITY=$(echo "$GEO" | grep -o '"city":"[^"]*"' | cut -d'"' -f4)
ORG=$(echo "$GEO" | grep -o '"org":"[^"]*"' | cut -d'"' -f4)
echo "   egress IP : ${EGRESS}"
echo "   location  : ${CITY:-?}, ${CC:-?}"
echo "   ASN/org   : ${ORG:-?}"
echo ""

# --- TCP round-trip ---
echo "${BOLD}2. Network RTT (ping to clob.polymarket.com)${RST}"
PING_HOST=$(echo "$CLOB_BASE" | sed 's|https\?://||' | cut -d/ -f1)
if command -v ping >/dev/null 2>&1; then
  ping -c 4 -W 2 "$PING_HOST" 2>/dev/null | tail -2 || echo "   (ping failed or not allowed)"
else
  echo "   (ping not installed)"
fi
echo ""

# --- Latency samples helper ---
sample() {
  local label="$1"; shift
  local method="$1"; shift
  local url="$1"; shift
  local extra=("$@")

  local total_ms=0
  local samples=()
  local http_codes=()
  local fails=0

  for i in $(seq 1 $SAMPLES); do
    local out
    out=$(curl -sS --max-time 10 \
      -o /dev/null \
      -w "%{time_total} %{http_code}" \
      -X "$method" \
      "${extra[@]}" \
      "$url" 2>&1 || echo "0 000")
    local tsec=$(echo "$out" | awk '{print $1}')
    local code=$(echo "$out" | awk '{print $2}')
    local tms=$(awk -v t="$tsec" 'BEGIN{printf "%.0f", t*1000}')
    if [ "$code" = "000" ] || [ "$code" = "" ]; then
      fails=$((fails+1))
      samples+=("FAIL")
    else
      samples+=("${tms}")
      total_ms=$((total_ms + tms))
    fi
    http_codes+=("$code")
  done

  # stats
  local ok=$((SAMPLES - fails))
  local avg=0
  local min=99999
  local max=0
  for s in "${samples[@]}"; do
    [ "$s" = "FAIL" ] && continue
    [ "$s" -lt "$min" ] && min=$s
    [ "$s" -gt "$max" ] && max=$s
  done
  [ "$ok" -gt 0 ] && avg=$((total_ms / ok))

  printf "   %-28s" "$label"
  printf "%3d/%d ok  " "$ok" "$SAMPLES"
  if [ "$ok" -gt 0 ]; then
    printf "min=${GRN}%4dms${RST}  avg=${BOLD}%4dms${RST}  max=${YEL}%4dms${RST}  " "$min" "$avg" "$max"
  else
    printf "${RED}all failed${RST}                                       "
  fi
  # unique HTTP codes
  local codes_uniq=$(printf "%s\n" "${http_codes[@]}" | sort -u | tr '\n' ',' | sed 's/,$//')
  printf "codes=[${codes_uniq}]\n"
}

# --- Read endpoints (never blocked, just latency) ---
echo "${BOLD}3. READ endpoints (latency only — reads are never geoblocked)${RST}"
sample "GET /time"          "GET" "${CLOB_BASE}/time"
sample "GET /markets"       "GET" "${CLOB_BASE}/markets?next_cursor=MA=="
sample "GET /book?token_id" "GET" "${CLOB_BASE}/book?token_id=123"
sample "Gamma /markets"     "GET" "${GAMMA_BASE}/markets?limit=1"
echo ""

# --- Write endpoint (this is the one that matters) ---
echo "${BOLD}4. WRITE endpoint (POST /order) — geoblock detection${RST}"
echo "   sending bogus POST — we expect 400/401 if IP allowed, 403 if blocked"
echo ""

RESP_BODY=$(mktemp)
RESP_HEADERS=$(mktemp)
trap "rm -f $RESP_BODY $RESP_HEADERS" EXIT

start=$(date +%s%N 2>/dev/null || date +%s)
HTTP_CODE=$(curl -sS --max-time 15 \
  -o "$RESP_BODY" \
  -D "$RESP_HEADERS" \
  -w "%{http_code}" \
  -X POST \
  -H "Content-Type: application/json" \
  -H "User-Agent: sarb-latency-test/1.0" \
  -d '{"test":"latency-probe"}' \
  "${CLOB_BASE}/order" 2>&1 || echo "000")
end=$(date +%s%N 2>/dev/null || date +%s)

if [ ${#start} -gt 10 ]; then
  elapsed_ms=$(( (end - start) / 1000000 ))
else
  elapsed_ms=$(( (end - start) * 1000 ))
fi

echo "   HTTP code : ${HTTP_CODE}"
echo "   elapsed   : ${elapsed_ms}ms"
echo ""
echo "   response headers (first 10):"
head -10 "$RESP_HEADERS" 2>/dev/null | sed 's/^/     /' || echo "     (empty)"
echo ""
echo "   response body (first 500 chars):"
head -c 500 "$RESP_BODY" 2>/dev/null | sed 's/^/     /' || echo "     (empty)"
echo ""
echo ""

# --- Verdict ---
echo "${BOLD}5. Verdict${RST}"
case "$HTTP_CODE" in
  400|401|422)
    echo "   ${GRN}✓ IP ALLOWED${RST} — app-layer rejection (bad payload) — the IP passed the geo/ASN filter"
    echo "   ${GRN}→ This host can talk to Polymarket /order directly, no proxy needed${RST}"
    ;;
  403)
    if grep -qi "cloudflare\|attention required\|blocked" "$RESP_BODY" 2>/dev/null; then
      echo "   ${RED}✗ BLOCKED by Cloudflare${RST} — IP is on Polymarket's geo/ASN blocklist"
      echo "   ${RED}→ This host needs a residential proxy to hit /order${RST}"
    else
      echo "   ${RED}✗ 403 FORBIDDEN${RST} — likely geo/ASN block (but no Cloudflare marker)"
      echo "   ${RED}→ Needs proxy${RST}"
    fi
    ;;
  000|"")
    echo "   ${RED}✗ connection failed${RST} — network unreachable or timeout"
    ;;
  2*)
    echo "   ${YEL}⚠ 2xx response to invalid payload — unexpected, investigate manually${RST}"
    ;;
  *)
    echo "   ${YEL}⚠ unexpected HTTP ${HTTP_CODE}${RST} — inspect the body above"
    ;;
esac
echo ""
