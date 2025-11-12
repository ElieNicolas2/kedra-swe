# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
import os, mimetypes
from scrapy.pipelines.files import FilesPipeline
from scrapy.http import Request
from crawler.utility import safe_ext_from_ct, sha256_bytes
from datetime import datetime, timezone
# useful for handling different item types with a single interface
from itemadapter import ItemAdapter

class DecisionFilesPipeline(FilesPipeline):
    def get_media_requests(self, item, info):
        for url in item.get("file_urls", []):
            yield Request(
                url,
                meta={
                    "identifier": item.get("identifier"),
                    "body": item.get("body"),
                    "partition_date": item.get("partition_date"),
                },
                dont_filter=True,
            )

    def file_path(self, request, response=None, info=None, *, item=None):
        identifier = (request.meta.get("identifier") or "noid").strip()
        body = str(request.meta.get("body") or "unknown")
        part = request.meta.get("partition_date") or "0000-00"
        ct = response.headers.get("Content-Type", b"").decode("utf-8") if response else ""
        ext = safe_ext_from_ct(ct, request.url)
        fname = f"{identifier}{ext}"
        return os.path.join(part, body, fname).replace("\\", "/")

def file_downloaded(self, response, request, info, *, item=None):
    checksum = super().file_downloaded(response, request, info, item=item)
    rel_path = self.file_path(request, response=response, info=info, item=item)
    abs_path = self.store.path(rel_path)

    data = b""
    try:
        with open(abs_path, "rb") as f:
            data = f.read()
    except Exception:
        data = b""

    size = len(data)
    file_hash = sha256_bytes(data) if data else None

    ct_hdr = response.headers.get(b"Content-Type", b"").decode("utf-8", errors="ignore")
    guessed, _ = mimetypes.guess_type(abs_path)
    mime = ct_hdr or (guessed or "application/octet-stream")

    sf = {
        "url": response.url,
        "stored_file_path": rel_path,
        "filesize_bytes": size,
        "file_hash": file_hash,
        "mime": mime,
        "checksum": checksum,
    }
    item.setdefault("stored_files", []).append(sf)

    kind = (
        "pdf" if "pdf" in mime.lower()
        else "docx" if "officedocument.wordprocessingml.document" in mime.lower()
        else "doc" if "msword" in mime.lower()
        else "html" if "html" in mime.lower()
        else "bin"
    )
    types = set(item.get("content_types") or [])
    types.add(kind)
    item["content_types"] = list(types)

    return checksum

class MetadataPipeline:
    def process_item(self, item, spider):
        if not item.get("partition_date"):
            part = None
            dd = item.get("decision_date")
            if isinstance(dd, str) and len(dd) >= 7:
                part = dd[:7]
            item["partition_date"] = part or datetime.now(timezone.utc).strftime("%Y-%m")

        item["scraped_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds") + "Z"

        item.setdefault("content_types", [])

        return item
