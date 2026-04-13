#!/usr/bin/env bash
# scripts/wait-for-kafka.sh — Espera hasta que Kafka esté listo
# Uso: ./wait-for-kafka.sh localhost:9092

set -euo pipefail
KAFKA_ADDR="${1:-localhost:9092}"
MAX_WAIT=120
INTERVAL=5
waited=0

echo "[WAIT] Esperando que Kafka esté disponible en $KAFKA_ADDR ..."

until nc -z $(echo $KAFKA_ADDR | tr ':' ' ') 2>/dev/null; do
  if [ $waited -ge $MAX_WAIT ]; then
    echo "[TIMEOUT] Kafka no disponible después de ${MAX_WAIT}s"
    exit 1
  fi
  echo "[WAIT] Kafka no listo, reintentando en ${INTERVAL}s... (${waited}/${MAX_WAIT}s)"
  sleep $INTERVAL
  waited=$((waited + INTERVAL))
done

echo "[OK] Kafka disponible en $KAFKA_ADDR"
