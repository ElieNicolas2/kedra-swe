import hashlib
import os
import re
from urllib.parse import urlparse
from datetime import datetime
from typing import Optional

def sha256_bytes(b: bytes):
    h = hashlib.sha256()
    h.update(b)
    return h.hexdigest()

def safe_ext_from_ct(content_type: str, url: str):
    if content_type:
        if "pdf" in content_type:
            return ".pdf"
        if "msword" in content_type or "application/msword" in content_type:
            return ".doc"
        if "officedocument.wordprocessingml.document" in content_type:
            return ".docx"
        if "html" in content_type or "text/html" in content_type:
            return ".html"
    path = urlparse(url).path.lower()
    for ext in [".pdf", ".docx", ".doc", ".html", ".htm"]:
        if path.endswith(ext):
            return ext if ext != ".htm" else ".html"
    return ".bin"

def to_iso_date(raw: str):
    if not raw:
        return None, None
    raw = raw.strip()
    for fmt in ("%d/%m/%Y", "%d-%m-%Y"):
        try:
            dt = datetime.strptime(raw, fmt)
            return dt.strftime("%Y-%m-%d"), dt.strftime("%Y-%m")
        except ValueError:
            continue
    return None, None

ID_PATTERNS = [re.compile(r"\b(ADJ-\d{5,})\b", re.IGNORECASE), re.compile(r"\b(IR-SC-\d{5,})\b", re.IGNORECASE), re.compile(r"\b(LCR-\d{5,})\b", re.IGNORECASE), re.compile(r"\b(EET-\d{5,})\b", re.IGNORECASE), re.compile(r"\b(DEC-\d{5,})\b", re.IGNORECASE), re.compile(r"\b(WTC-[A-Z0-9\-_/]{4,})\b", re.IGNORECASE), re.compile(r"\b(EDA-[A-Z0-9\-_/]{4,})\b", re.IGNORECASE), re.compile(r"\b(UD-[A-Z0-9\-_/]{4,})\b", re.IGNORECASE), re.compile(r"\b(MN-[A-Z0-9\-_/]{4,})\b", re.IGNORECASE), re.compile(r"\b(CA-[A-Z0-9\-_/]{4,})\b", re.IGNORECASE)]

def normalize_identifier(detail_url: str, title_text: str = ""):
    hay = f"{detail_url} {title_text}".strip()
    for pat in ID_PATTERNS:
        m = pat.search(hay)
        if m:
            return m.group(1).upper()
    try:
        seg = os.path.basename(urlparse(detail_url).path)
        seg = re.sub(r"\.(html?|pdf|docx?)$", "", seg, flags=re.IGNORECASE)
        seg = re.sub(r"[^A-Za-z0-9\-]+", "-", seg).strip("-")
        if seg:
            return seg.upper()
    except Exception:
        pass
    t = (title_text or "").strip()
    if t:
        t = re.sub(r"[^A-Za-z0-9\-]+", "-", t).strip("-")
        if t:
            return t.upper()
    return "NOID"

def guess_identifier(text_or_url: str):
    if not text_or_url:
        return None
    m = re.search(r"\b(ADJ|WTC|EET|DEC|LCR|EDA|UD|MN|CA)-?[A-Z0-9\-_/]{4,}\b", text_or_url, re.IGNORECASE)
    if m:
        return m.group(0).upper()
    return None

def unique_preserve(seq):
    seen = set()
    out = []
    for x in seq:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out

def prepare_search_query(q: Optional[str]):
    if not q:
        return None
    q = q.strip()
    if not q:
        return None
    if (q.startswith('"') and q.endswith('"')) or (q.startswith("'") and q.endswith("'")):
        return q
    if re.search(r"\s", q):
        return f'"{q}"'
    return q