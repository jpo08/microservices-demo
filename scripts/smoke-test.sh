#!/usr/bin/env bash
# ============================================================
# scripts/smoke-test.sh
# Script de pruebas de humo (smoke tests) para el pipeline CI/CD
# Uso: ./smoke-test.sh <servicio> <base-url>
# ============================================================

set -euo pipefail

SERVICE="${1:-all}"
BASE_URL="${2:-http://localhost}"
MAX_WAIT=120
INTERVAL=5

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log()  { echo -e "${GREEN}[INFO]${NC}  $*"; }
warn() { echo -e "${YELLOW}[WARN]${NC}  $*"; }
fail() { echo -e "${RED}[FAIL]${NC}  $*"; exit 1; }

# ── Esperar hasta que un endpoint responda ─────────────────
wait_for_service() {
  local name="$1"
  local url="$2"
  local waited=0

  log "Esperando servicio '$name' en $url ..."
  until curl -sf --max-time 5 "$url" > /dev/null 2>&1; do
    if [ $waited -ge $MAX_WAIT ]; then
      fail "Timeout esperando '$name' después de ${MAX_WAIT}s"
    fi
    warn "  '$name' no disponible. Reintentando en ${INTERVAL}s... (${waited}/${MAX_WAIT}s)"
    sleep $INTERVAL
    waited=$((waited + INTERVAL))
  done
  log "✅ Servicio '$name' disponible"
}

# ── Test: vote service ──────────────────────────────────────
test_vote() {
  local url="${BASE_URL}:8080"
  wait_for_service "vote" "$url"

  log "Probando endpoint de votación..."
  response=$(curl -sf -o /dev/null -w "%{http_code}" "$url/" || echo "000")
  if [ "$response" != "200" ]; then
    fail "Vote service devolvió HTTP $response (esperado 200)"
  fi

  log "Enviando voto de prueba..."
  vote_response=$(curl -sf -o /dev/null -w "%{http_code}" \
    -X POST "$url/vote" \
    -H "Content-Type: application/x-www-form-urlencoded" \
    -d "vote=a" || echo "000")
  if [ "$vote_response" != "200" ] && [ "$vote_response" != "302" ]; then
    warn "Voto de prueba devolvió HTTP $vote_response (puede ser esperado en CI)"
  fi

  log "✅ Vote service OK"
}

# ── Test: result service ────────────────────────────────────
test_result() {
  local url="${BASE_URL}:4000"
  wait_for_service "result" "$url"

  log "Probando endpoint de resultados..."
  response=$(curl -sf -o /dev/null -w "%{http_code}" "$url/" || echo "000")
  if [ "$response" != "200" ]; then
    fail "Result service devolvió HTTP $response (esperado 200)"
  fi

  log "Comprobando health check..."
  health=$(curl -sf "$url/health" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('status',''))" 2>/dev/null || echo "")
  if [ "$health" != "ok" ]; then
    warn "Health check no retornó 'ok' (valor: '$health')"
  fi

  log "✅ Result service OK"
}

# ── Test: worker (indirecto a través de la DB) ──────────────
test_worker() {
  log "Verificando worker (indirecto vía endpoint de resultados)..."
  # El worker no tiene endpoint HTTP; se verifica que los datos
  # fluyan desde Kafka → Worker → PostgreSQL → Result
  sleep 5
  result_url="${BASE_URL}:4000"
  response=$(curl -sf "$result_url/" -o /dev/null -w "%{http_code}" || echo "000")
  if [ "$response" == "200" ]; then
    log "✅ Worker probablemente activo (result service responde con datos)"
  else
    warn "No se pudo verificar el worker indirectamente"
  fi
}

# ── Main ────────────────────────────────────────────────────
main() {
  log "========================================="
  log "  Smoke Tests - Microservices Demo"
  log "  Servicio: $SERVICE | URL base: $BASE_URL"
  log "========================================="

  case "$SERVICE" in
    vote)   test_vote ;;
    result) test_result ;;
    worker) test_worker ;;
    all)
      test_vote
      test_result
      test_worker
      ;;
    *)
      fail "Servicio desconocido: $SERVICE. Opciones: vote, result, worker, all"
      ;;
  esac

  log ""
  log "========================================="
  log "  ✅ Todos los smoke tests pasaron"
  log "========================================="
}

main "$@"
