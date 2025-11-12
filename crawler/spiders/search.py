import os
import re
from urllib.parse import urlencode, urlparse

import scrapy
from crawler.items import CrawlerItem
from crawler.utility import to_iso_date, normalize_identifier, unique_preserve, guess_identifier, prepare_search_query 
from typing import Optional
from datetime import datetime, timezone

BODY_MAP = {
    1: "Equality Tribunal",
    2: "Employment Appeals Tribunal",
    3: "Labour Court",
    15376: "Workplace Relations Commission",
}

class SearchSpider(scrapy.Spider):
    name = "search"
    allowed_domains = ["www.workplacerelations.ie"]

    def __init__(self, date_from=None, date_to=None, body=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.date_from = date_from
        self.date_to = date_to

        if body:
            try:
                self.body_ids = [int(x.strip()) for x in str(body).split(",") if x.strip()]
            except ValueError:
                self.logger.warning("Invalid body list %r; defaulting to all.", body)
                self.body_ids = list(BODY_MAP.keys())
        else:
            self.body_ids = list(BODY_MAP.keys())

    def add_args(self, body_id: Optional[int], date_from: Optional[str], date_to: Optional[str], page: int, q: Optional[str] = None):
        params = {"decisions": "1"}
        if date_from:
            params["from"] = date_from
        if date_to:
            params["to"] = date_to
        if body_id:
            params["body"] = body_id
        if page and page > 1:
            params["pageNumber"] = str(page)
        if q:
            params["q"] = q
        return "https://www.workplacerelations.ie/en/search/?" + urlencode(params)

    def start_requests(self):
        for body_id in self.body_ids:
            body_name = BODY_MAP.get(body_id, str(body_id))
            url = self.add_args(body_id=body_id, date_from=self.date_from, date_to=self.date_to, page=1, q=None)
            yield scrapy.Request(
                url,
                callback=self.parse,
                cb_kwargs={
                    "date_from": self.date_from,
                    "date_to": self.date_to,
                    "body_id": body_id,
                    "body_name": body_name,
                    "page": 1,
                    "q": None,
                },
            )

    def parse(self, response, body_id=None, body_name=None, date_from=None, date_to=None, page=1, q=None, **kwargs):
        items_sel = response.css("li.each-item")
        count = len(items_sel)
        self.logger.info("Found %d items on page %s for body %s", count, page, body_name or "ALL")
        if count == 0:
            return

        for s in items_sel:
            title_a = s.css("h3 a::attr(href)").get() or s.css("a::attr(href)").get()
            title_txt = (s.css("h3 a::text").get() or s.css("a::text").get() or "").strip()
            date_txt = (s.css("time::text, .date::text").get() or "").strip()
            desc_txt = (s.css(".summary::text, .teaser::text, p::text").get() or "").strip()
            detail_url = response.urljoin(title_a) if title_a else None

            ident = normalize_identifier(detail_url or "", title_txt)
            if not ident or ident == "NOID":
                g = guess_identifier(f"{title_txt} {detail_url or ''}")
                ident = g if g else ident
            if not ident or ident == "NOID":
                seg = ""
                if detail_url:
                    seg = os.path.basename(urlparse(detail_url).path)
                    seg = re.sub(r"\.(html?|pdf|docx?)$", "", seg, flags=re.IGNORECASE)
                    seg = re.sub(r"[^A-Za-z0-9\-]+", "-", seg).strip("-")
                ident = (seg or "NOID").upper()
            identifier = ident.replace("/", "-").replace("\\", "-").strip()

            decision_date, part_yyyy_mm = to_iso_date(date_txt)

            initial_files = []
            for a in s.css("a::attr(href)").getall():
                href = response.urljoin(a)
                low = href.lower()
                if low.endswith((".pdf", ".doc", ".docx")):
                    initial_files.append(href)
            if not initial_files and detail_url:
                initial_files = [detail_url]

            item = CrawlerItem()

            if not identifier or identifier.upper() == "NOID":
                if not initial_files and not detail_url:
                    self.logger.warning("Dropping: missing identifier and no files (%s)", response.url)
                    self.crawler.stats.inc_value("search/dropped_no_id_no_files")
                    return
                identifier = ("NOID-" + str(abs(hash(detail_url or title_txt)))).upper()

            item["identifier"] = identifier

            item["title"] = (title_txt or "").strip() or identifier
            item["description"] = (desc_txt or "").strip()
            item["decision_date_raw"] = (date_txt or "").strip()

            if decision_date:
                item["decision_date"] = decision_date
                item["partition_date"] = part_yyyy_mm
            else:
                try:
                    d, m, y = (date_from or "").split("/")
                    item["partition_date"] = f"{y}-{int(m):02d}"
                except Exception:
                    item["partition_date"] = datetime.now(timezone.utc).strftime("%Y-%m")

            if body_id is None or body_name is None:
                self.logger.warning("Missing body info for %s; defaulting to unknown", identifier)
                self.crawler.stats.inc_value("search/missing_body_info")
            item["body_id"] = int(body_id) if body_id is not None else -1
            item["body"]    = str(body_name) if body_name is not None else "unknown"

            item["source_url"] = detail_url or response.url
            item["detail_url"] = detail_url or ""

            if detail_url:
                yield scrapy.Request(
                    detail_url,
                    callback=self.parse_detail,
                    errback=self.on_detail_error,
                    cb_kwargs={
                        "base_item": item,
                        "seed_file_urls": unique_preserve(initial_files),
                        "date_from": date_from,
                        "date_to": date_to,
                        "body_id": body_id,
                        "body_name": body_name,
                    },
                )
            else:
                files = unique_preserve(initial_files)
                if not files:
                    self.logger.warning("Dropping %s: no detail_url and no files", item["identifier"])
                    self.crawler.stats.inc_value("search/dropped_no_files")
                    return
                item["file_urls"] = files
                yield item

        next_page = (page or 1) + 1
        next_url = self.add_args(body_id, date_from, date_to, page=next_page, q=q)
        yield scrapy.Request(
            next_url,
            callback=self.parse,
            cb_kwargs={
                "body_id": body_id,
                "body_name": body_name,
                "date_from": date_from,
                "date_to": date_to,
                "page": next_page,
                "q": q,
            },
        )

    def parse_detail(self, response, base_item, seed_file_urls, date_from, date_to, body_id, body_name, **kwargs):
        attach = []
        for href in response.css('a::attr(href)').getall():
            full = response.urljoin(href)
            low = full.lower()
            if low.endswith((".pdf", ".doc", ".docx")):
                attach.append(full)

        combined = unique_preserve(list(seed_file_urls) + attach)
        item = base_item
        item["file_urls"] = combined
        item["body_id"] = int(body_id) if body_id is not None else None
        item["body"] = str(body_name) if body_name is not None else None
        yield item  

    def on_detail_error(self, failure):
        request = failure.request
        kw = request.cb_kwargs or {}
        base_item = kw.get("base_item")
        seed_files = kw.get("seed_file_urls") or []

        self.logger.warning("Detail request failed for %s (%s). Using seed files only.",
                            base_item.get("identifier") if base_item else "UNKNOWN",
                            getattr(failure.value, "__class__", type(failure.value)).__name__)
        self.crawler.stats.inc_value("search/detail_request_failed")

        if not base_item:
            return  # we cannot do anything about it give up

        if seed_files:
            base_item["file_urls"] = unique_preserve(seed_files)
            yield base_item
        else:
            self.logger.warning("Dropping %s: detail failed and no seed files",
                                base_item.get("identifier", "UNKNOWN"))
            self.crawler.stats.inc_value("search/dropped_detail_no_files")

