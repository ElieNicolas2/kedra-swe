import os
import re
from urllib.parse import urlencode, urlparse

import scrapy
from crawler.items import CrawlerItem
from crawler.utility import to_iso_date, normalize_identifier, unique_preserve, guess_identifier, prepare_search_query
from typing import Optional

class SearchSpider(scrapy.Spider):
    name = "search"
    allowed_domains = ["www.workplacerelations.ie"]

    def add_args(self, body: Optional[str], date_from: str, date_to: str, page: int, q: Optional[str] = None):
        params = {"decisions": "1", "from": date_from, "to": date_to}
        if body:
            params["body"] = body
        if page > 1:
            params["pageNumber"] = str(page)
        if q:
            params["q"] = q
        return "https://www.workplacerelations.ie/en/search/?" + urlencode(params)

    def start_requests(self):
        body = getattr(self, "body", None)
        date_from = getattr(self, "date_from", None)
        date_to = getattr(self, "date_to", None)
        q_raw = getattr(self, "q", None)
        q = prepare_search_query(q_raw)
        if not date_from or not date_to:
            raise scrapy.exceptions.CloseSpider("Pass -a date_from=D/M/YYYY -a date_to=D/M/YYYY [ -a body=ID or CSV ]")
        url = self.add_args(body, date_from, date_to, page=1, q=q)
        yield scrapy.Request(url, callback=self.parse, cb_kwargs={"body": body, "date_from": date_from, "date_to": date_to, "page": 1, "q": q})

    def parse(self, response, body=None, date_from=None, date_to=None, page=1, q=None, **kwargs):
        items_sel = response.css("li.each-item")
        count = len(items_sel)
        self.logger.info("Found %d items on page %s for body %s", count, page, body or "ALL")
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
            item["identifier"] = identifier
            item["title"] = title_txt
            item["description"] = desc_txt
            item["decision_date_raw"] = date_txt
            if decision_date:
                item["decision_date"] = decision_date
                item["partition_date"] = part_yyyy_mm
            else:
                try:
                    d, m, y = (date_from or "").split("/")
                    item["partition_date"] = f"{y}-{int(m):02d}"
                except Exception:
                    pass
            item["body"] = str(body) if body else "all"
            item["source_url"] = detail_url
            item["detail_url"] = detail_url

            if detail_url:
                yield scrapy.Request(
                    detail_url,
                    callback=self.parse_detail,
                    cb_kwargs={
                        "base_item": item,
                        "seed_file_urls": initial_files,
                        "date_from": date_from,
                        "date_to": date_to,
                        "body": body,
                    },
                )
            else:
                item["file_urls"] = unique_preserve(initial_files)
                yield item

        next_page = page + 1
        next_url = self.add_args(body, date_from, date_to, page=next_page, q=q)
        yield scrapy.Request(next_url, callback=self.parse, cb_kwargs={"body": body, "date_from": date_from, "date_to": date_to, "page": next_page, "q": q})

    def parse_detail(self, response, base_item, seed_file_urls, date_from, date_to, body):
        attach = []
        for href in response.css('a::attr(href)').getall():
            full = response.urljoin(href)
            low = full.lower()
            if low.endswith((".pdf", ".doc", ".docx")):
                attach.append(full)
        combined = unique_preserve(list(seed_file_urls) + attach)
        item = base_item
        item["file_urls"] = combined
        return item
