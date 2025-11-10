#!/usr/bin/env bash
set -euo pipefail
START_MM="${START_MM:?}"
END_MM="${END_MM:?}"
BODIES="${BODIES:-1,2,3,15376}"
Q_ARG="${Q_ARG:-}"
mkdir -p data/landing logs
IFS=',' read -r -a BODY_LIST <<< "$BODIES"
while IFS=',' read -r FROM TO PART; do
  for B in "${BODY_LIST[@]}"; do
    DATE_FROM="$FROM" \
    DATE_TO="$TO" \
    BODY_ARG="$B" \
    Q_ARG="$Q_ARG" \
    OUTPUT_PATH="data/landing/${PART}_${B}.jsonl" \
    LOG_FILE="logs/${PART}_${B}.log" \
    docker compose -f docker/docker-compose.yml up --build --abort-on-container-exit --exit-code-from crawler crawler
  done
done < <(scripts/month_span.py "$START_MM" "$END_MM")
