FROM python:3.9-slim

ENV PYTHONUNBUFFERED=1
ENV PIP_NO_CACHE_DIR=1

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libxml2-dev \
    libxslt1-dev \
    zlib1g-dev \
    libffi-dev \
    libssl-dev \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app
ENV PYTHONPATH=/app

COPY requirements_min.txt requirements.txt 
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV SCRAPY_SETTINGS_MODULE=crawler.settings

VOLUME ["/app/data", "/app/logs", "/app/.scrapy"]

CMD ["bash", "-lc", "scrapy crawl search -a date_from=${DATE_FROM} -a date_to=${DATE_TO} ${BODY_ARG:+-a body=${BODY_ARG}} ${Q_ARG:+-a q='${Q_ARG}'} -O ${OUTPUT_PATH:-data/landing/out.jsonl} -s LOG_FILE=${LOG_FILE:-logs/crawl.log}"]
