# KEDRA-SWE — Workplace Relations Decisions Crawler

Scrapes decisions from https://www.workplacerelations.ie/en/search/ with optional filtering by body, date window, and keyword. Downloads HTML pages and attached PDFs/DOC/DOCX files, writes structured JSON Lines, and upserts metadata into MongoDB.

## Features
- Search by date window (D/M/YYYY) and optional body filter
- Optional keyword `q` with auto-quoting for multi-word queries
- Pagination until exhaustion
- Detail-page pass to collect official attachments
- Deterministic, sanitized identifiers (ADJ-xxxxx, IR-SC-xxxxx, etc.)
- ISO date normalization and month partitioning
- MongoDB upsert with `first_seen` / `updated_at`
- Dockerized runner with Compose, volumes for data and logs

## Bodies
- 1 — Equality Tribunal
- 2 — Employment Appeals Tribunal
- 3 — Labour Court
- 15376 — Workplace Relations Commission

## Repo layout
```
.
├── crawler/            # Scrapy project (spider, items, pipelines)
├── data/               # Downloaded files and feeds (mounted volume)
├── logs/               # Logs and optional metrics (mounted volume)
├── docker/
│   ├── Dockerfile
│   └── docker-compose.yml
├── .dockerignore
├── .gitignore
├── scrapy.cfg
└── requirements.txt
```

## Quickstart (local venv)
```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
export MONGO_URI="mongodb://localhost:27017"
export MONGO_DB="kedra"
export MONGO_COLLECTION="decisions"

scrapy crawl search \
  -a date_from=1/10/2025 \
  -a date_to=15/10/2025 \
  -a body=15376 \
  -a q="minimum notice" \
  -O data/landing/out.jsonl \
  -s LOG_FILE=logs/crawl.log
```

## Docker
Build and run with Compose (Dockerfile and compose live in `docker/`, build context is repo root):
```bash
docker compose -f docker/docker-compose.yml up --build crawler
```

Override at runtime:
```bash
DATE_FROM="1/10/2025" DATE_TO="15/10/2025" BODY_ARG="15376" Q_ARG='minimum notice' \
docker compose -f docker/docker-compose.yml up --build crawler
```

## Environment variables
- `MONGO_URI` (default `mongodb://mongo:27017` in Compose)
- `MONGO_DB` (default `kedra`)
- `MONGO_COLLECTION` (default `decisions`)
- `DATE_FROM`, `DATE_TO` (required by entry command)
- `BODY_ARG` optional (comma-separated IDs)
- `Q_ARG` optional
- `OUTPUT_PATH` default `data/landing/out.jsonl`
- `LOG_FILE` default `logs/crawl.log`

## Output fields (JSONL)
- `identifier` — normalized ID (e.g., `ADJ-00054873`)
- `title`, `description`
- `decision_date_raw`, `decision_date` (ISO if parsed), `partition_date` (`YYYY-MM`)
- `body` — provided filter or `all`
- `source_url`, `detail_url`
- `file_urls` — HTML detail + attachments
- `files` (Scrapy Files pipeline entries)
- `stored_files` — `url`, `stored_file_path`, `file_hash`, `filesize_bytes`, `mime`
- `content_types` — `["html", "pdf", ...]`
- `first_seen`, `updated_at` (Mongo)

## Known constraints
- The site expects day-first dates.
- If search returns zero items in a page, pagination stops.
- If `decision_date_raw` is missing/unusual, we keep the raw and fall back to the window month for `partition_date`.
