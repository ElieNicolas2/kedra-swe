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
    export DATE_FROM="$FROM"
    export DATE_TO="$TO"
    export BODY_ARG="$B"
    export Q_ARG="$Q_ARG"
    export OUTPUT_PATH="data/landing/${PART}_${B}.jsonl"
    export LOG_FILE="logs/${PART}_${B}.log"
    scripts/run_crawl_local.sh
  done
done < <(scripts/month_span.py "$START_MM" "$END_MM")
