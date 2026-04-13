#!/usr/bin/env bash
# scripts/rollback.sh — Rollback de Helm releases al estado anterior
# Uso: ./rollback.sh <environment>

set -euo pipefail
ENV="${1:-staging}"
GREEN='\033[0;32m'; YELLOW='\033[1;33m'; RED='\033[0;31m'; NC='\033[0m'
log()  { echo -e "${GREEN}[INFO]${NC} $*"; }
warn() { echo -e "${YELLOW}[WARN]${NC} $*"; }
fail() { echo -e "${RED}[FAIL]${NC} $*"; exit 1; }

SERVICES=("vote" "result" "worker" "infrastructure")

log "========================================="
log "  ROLLBACK - Ambiente: $ENV"
log "========================================="

for svc in "${SERVICES[@]}"; do
  log "Ejecutando rollback de '$svc'..."
  if helm rollback "$svc" 0 2>/dev/null; then
    log "✅ Rollback de '$svc' exitoso"
  else
    warn "No se pudo hacer rollback de '$svc' (puede que no exista historial)"
  fi
done

log "Verificando estado post-rollback..."
helm list --all-namespaces

log "✅ Rollback completado"
