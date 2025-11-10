#!/usr/bin/env bash
set -euo pipefail
DATE_FROM="${DATE_FROM:?}"
DATE_TO="${DATE_TO:?}"
OUTPUT_PATH="${OUTPUT_PATH:-data/landing/out.jsonl}"
LOG_FILE="${LOG_FILE:-logs/crawl.log}"
BODY_ARG="${BODY_ARG:-}"
Q_ARG="${Q_ARG:-}"
ARGS=(-a "date_from=$DATE_FROM" -a "date_to=$DATE_TO")
if [ -n "$BODY_ARG" ]; then ARGS+=(-a "body=$BODY_ARG"); fi
if [ -n "$Q_ARG" ]; then ARGS+=(-a "q=$Q_ARG"); fi
scrapy crawl search "${ARGS[@]}" -O "$OUTPUT_PATH" -s "LOG_FILE=$LOG_FILE"
