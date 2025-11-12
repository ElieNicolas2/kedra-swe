import os
import re
import sys
import shutil
import hashlib
import logging
import argparse

from pathlib import Path
from typing import Dict, Any, List, Tuple, Optional
from datetime import datetime, timezone
from bs4 import BeautifulSoup, NavigableString, Comment
from bs4.element import Tag
from pymongo import MongoClient

logging.basicConfig(
    level=os.getenv("TRANSFORM_LOGLEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("transform")

MONGO_URI          = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB           = os.getenv("MONGO_DB", "kedra")
SOURCE_COLLECTION  = os.getenv("MONGO_COLLECTION", "decisions")          
CURATED_COLLECTION = os.getenv("CURATED_COLLECTION", "decisions_curated")
LANDING_DIR        = Path(os.getenv("FILES_STORE", "data/landing"))
CURATED_DIR        = Path(os.getenv("CURATED_STORE", "data/curated"))

def ensure_dir(p: Path):
    p.mkdir(parents=True, exist_ok=True)

def sha256_path(p: Path):
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

def is_html_path(p: Path):
    return p.suffix.lower() in {".html", ".htm"}

def is_binary_path(p: Path):
    return p.suffix.lower() in {".pdf", ".doc", ".docx"}

def next_unique_name(dir_: Path, base_name: str, ext: str):
    candidate = dir_ / f"{base_name}{ext}"
    i = 2
    while candidate.exists():
        candidate = dir_ / f"{base_name}-{i}{ext}"
        i += 1
    return candidate

def decide_partition(doc: Dict[str, Any]):
    dd = (doc.get("decision_date") or "")[:7]
    if re.match(r"^\d{4}-\d{2}$", dd):
        return dd
    pd = (doc.get("partition_date") or "")
    if re.match(r"^\d{4}-\d{2}$", pd):
        return pd
    return datetime.now(timezone.utc).strftime("%Y-%m")

def body_folder(doc: Dict[str, Any]):
    val = (doc.get("body") or doc.get("body_id") or "all")
    val = str(val).strip().lower()
    return re.sub(r"[^a-z0-9\-]+", "-", val)

def source_file_paths(doc: Dict[str, Any]):
    out: List[Tuple[Path, Optional[str]]] = []
    for rec in (doc.get("stored_files") or []):
        rel = rec.get("path")
        if rel:
            out.append((LANDING_DIR / rel, rec.get("content_type")))
    if not out:
        for rec in (doc.get("files") or []):
            rel = rec.get("path")
            if rel:
                out.append((LANDING_DIR / rel, None))
    return out

def clean_html(html: str):
    try:
        soup = BeautifulSoup(html, "lxml")
    except Exception as e:
        logger.warning("BeautifulSoup parse failed: %s", e)
        return html

    for el in soup.select("header, footer, nav, aside, iframe, noscript, script, style"):
        try:
            el.decompose()
        except Exception:
            pass

    for c in soup.find_all(string=lambda t: isinstance(t, Comment)):
        try:
            c.extract()
        except Exception:
            pass

    KW = {
        "breadcrumb","breadcrumbs","navbar","navigation","site-header","site-footer",
        "cookie","consent","banner","sidebar","social","share","toc","skip-link",
        "masthead","branding","advert","ad-","promo","newsletter","modal","popup",
        "utility","topbar","menubar","pager","pagination"
    }

    def looks_boiler(el):
        if not isinstance(el, Tag):
            return False
        _id = el.get("id") or ""
        _cls = el.get("class") or []
        if not isinstance(_cls, (list, tuple)):
            _cls = [_cls] if _cls else []
        attrs = f"{_id} {' '.join(map(str, _cls))}".lower()
        return any(k in attrs for k in KW)

    for el in list(soup.find_all(True)): 
        try:
            if looks_boiler(el):
                el.decompose()
        except Exception:
            continue

    main = soup.select_one("main") or soup.select_one("[role=main]") or soup.body or soup

    candidates = [n for n in main.find_all(["article", "section", "div"], recursive=False) if isinstance(n, Tag)]
    if not candidates:
        candidates = [main] if isinstance(main, Tag) else []

    def text_len(n: Tag):
        try:
            return len(n.get_text(separator=" ", strip=True))
        except Exception:
            return 0

    chosen = max(candidates, key=text_len) if candidates else main

    for tag in list(getattr(chosen, "find_all", lambda *a, **k: [])()):
        if not isinstance(tag, Tag):
            continue
        try:
            if tag.name in {"p","div","section","article"} and not tag.get_text(strip=True):
                tag.decompose()
        except Exception:
            continue

    title_txt = ""
    try:
        if soup.title and soup.title.string:
            title_txt = soup.title.string.strip()
    except Exception:
        pass

    out = BeautifulSoup("<!doctype html><html><head><meta charset='utf-8'></head><body></body></html>", "lxml")
    if title_txt:
        try:
            t = out.new_tag("title"); t.string = title_txt; out.head.append(t)
        except Exception:
            pass

    container = out.new_tag("article")
    container["data-curated"] = "true"

    for child in list(getattr(chosen, "children", [])):
        if isinstance(child, (NavigableString, Comment)):
            if isinstance(child, NavigableString) and child.strip():
                container.append(out.new_string(child))
            continue
        if not isinstance(child, Tag):
            continue
        try:
            container.append(child.extract())
        except Exception:
            continue

    out.body.append(container)

    try:
        if len(container.get_text(strip=True)) < 200:
            logger.info("Curated HTML too small; falling back to original.")
            return html
    except Exception:
        return html

    return str(out)

def curate_one(doc: Dict[str, Any], curated_root: Path):
    ident = (doc.get("identifier") or "NOID").strip().replace("/", "-").replace("\\", "-")
    part = decide_partition(doc)
    body = body_folder(doc)

    dest_dir = curated_root / part / body / ident
    ensure_dir(dest_dir)

    results: List[Dict[str, Any]] = []
    for src_path, ct in source_file_paths(doc):
        try:
            if not src_path.exists():
                logger.warning("Missing source file: %s", src_path)
                results.append({"status": "missing_source", "source": str(src_path)})
                continue

            ext = src_path.suffix.lower()
            if is_binary_path(src_path) or (ext and ext not in {".html", ".htm"}):
                target = next_unique_name(dest_dir, ident, ext or ".bin")
                shutil.copy2(src_path, target)
                h = sha256_path(target)
                results.append({
                    "status": "copied",
                    "transformed": False,
                    "new_file_path": str(target.relative_to(curated_root)),
                    "new_file_hash": h,
                    "content_type_hint": ct,
                    "ext": ext or ".bin",
                })
            else:
                try:
                    raw = src_path.read_text(encoding="utf-8", errors="ignore")
                except UnicodeDecodeError:
                    raw = src_path.read_text(errors="ignore")
                cleaned = clean_html(raw)
                target = next_unique_name(dest_dir, ident, ".html")
                target.write_text(cleaned, encoding="utf-8")
                h = sha256_path(target)
                results.append({
                    "status": "transformed",
                    "transformed": True,
                    "new_file_path": str(target.relative_to(curated_root)),
                    "new_file_hash": h,
                    "content_type_hint": "text/html",
                    "ext": ".html",
                })

        except Exception as e:
            logger.exception("Error curating %s (%s): %s", ident, src_path, e)
            results.append({"status": "error", "source": str(src_path), "error": str(e)})
    return results

def query_window(start: str, end: str):
    if start and end:
        s_mm = start[:7]
        e_mm = end[:7]
        return {
            "$or": [
                {"decision_date": {"$gte": start, "$lte": end}},
                {"partition_date": {"$gte": s_mm, "$lte": e_mm}},
            ]
        }
    return {}

def main():
    ap = argparse.ArgumentParser(description="Transform Landing Zone into curated container.")
    ap.add_argument("--start", required=True, help="YYYY-MM-DD inclusive")
    ap.add_argument("--end",   required=True, help="YYYY-MM-DD inclusive")
    args = ap.parse_args()

    ensure_dir(CURATED_DIR)

    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=6000)
        client.admin.command("ping")
    except Exception as e:
        logger.error("Mongo connection failed: %s", e)
        sys.exit(2)

    db  = client[MONGO_DB]
    src = db[SOURCE_COLLECTION]
    dst = db[CURATED_COLLECTION]

    try:
        dst.create_index([("identifier", 1), ("detail_url", 1)])
        dst.create_index([("new_files.new_file_path", 1)])
    except Exception as e:
        logger.warning("Index creation failed (continuing): %s", e)

    filt = query_window(args.start, args.end)
    try:
        total = src.count_documents(filt)
    except Exception as e:
        logger.error("count_documents failed: %s", e)
        total = -1

    logger.info("Transforming %s documents from '%s' to '%s'", total, SOURCE_COLLECTION, CURATED_COLLECTION)

    processed = ok = errs = missing = 0
    for doc in src.find(filt, no_cursor_timeout=True):
        try:
            curated = curate_one(doc, CURATED_DIR)
            errs    += sum(1 for r in curated if r.get("status") == "error")
            missing += sum(1 for r in curated if r.get("status") == "missing_source")

            update = {
                "identifier":     doc.get("identifier"),
                "detail_url":     doc.get("detail_url") or doc.get("source_url"),
                "body":           doc.get("body") or "all",
                "body_id":        doc.get("body_id"),
                "decision_date":  doc.get("decision_date"),
                "partition_date": decide_partition(doc),
                "new_files":      curated,
                "curated_at":     datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            }

            dst.update_one(
                {"identifier": update["identifier"], "detail_url": update["detail_url"]},
                {"$set": update},
                upsert=True,
            )
            ok += 1
        except Exception as e:
            errs += 1
            logger.exception("Upsert failed for %s: %s", doc.get("identifier"), e)
        processed += 1
        if processed % 200 == 0:
            logger.info("... processed=%d ok=%d errs=%d missing=%d", processed, ok, errs, missing)

    client.close()
    logger.info("Done. processed=%d ok=%d errs=%d missing=%d", processed, ok, errs, missing)

if __name__ == "__main__":
    main()
