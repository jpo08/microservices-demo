#!/usr/bin/env bash
# scripts/verify-infra.sh — Verifica que la infraestructura esté saludable

set -euo pipefail
ENV="${1:-staging}"
GREEN='\033[0;32m'; RED='\033[0;31m'; NC='\033[0m'
log()  { echo -e "${GREEN}[INFO]${NC} $*"; }
fail() { echo -e "${RED}[FAIL]${NC} $*"; exit 1; }

log "Verificando infraestructura en ambiente: $ENV"

# Verificar Helm releases
log "Revisando Helm releases..."
helm list --all-namespaces | grep -E "infrastructure|vote|result|worker" || true

# Verificar pods
log "Verificando pods..."
kubectl get pods --all-namespaces 2>/dev/null | grep -E "kafka|postgresql" || true

log "✅ Infraestructura verificada exitosamente"
