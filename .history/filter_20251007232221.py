#!/usr/bin/env python3
"""
Filter & Deduplicate for RL + Mechanical Ventilation Weaning
------------------------------------------------------------
Reads raw combined files from paper_list/raw/, normalizes into a single schema,
deduplicates across sources, then applies keyword filters to prioritize
Mechanical Ventilation + Weaning papers (so sepsis, etc. are dropped).

Outputs (in paper_list/normalized/):
- unified_all.jsonl / unified_all.csv           : normalized (pre-filter), de-duplicated
- screened_candidates.csv                       : pass MV+weaning filter (for PRISMA TA screening)
- excluded_non_mv_weaning.csv                   : auto-excluded (with reason flags)
- prisma_counts.json                            : simple counts you can paste into PRISMA
"""

from pathlib import Path
import json, csv, re, xml.etree.ElementTree as ET

RAW = Path("paper_list/raw")
OUT = Path("paper_list/normalized")
OUT.mkdir(parents=True, exist_ok=True)

# ------------------ tuning knobs ------------------
STRICT_REQUIRE_RL = False  # set True to also require RL keyword match in text
CASE_SENSITIVE = False     # keyword matching is case-insensitive by default

# RL keywords (already in your search, but kept here if you switch sources later)
RL_TERMS = [
    r"reinforcement learning", r"\bMDP\b", r"markov decision", r"\bQ-?learning\b",
    r"fitted q", r"policy gradient", r"actor-critic", r"offline reinforcement",
    r"deep reinforcement", r"inverse reinforcement",
]

# Mechanical ventilation (MV) and Weaning terms
MV_TERMS = [
    "mechanical ventilation", "ventilator", "ventilatory support",
    "ventilation", "ventilated", "patient-ventilator"
]
WEAN_TERMS = [
    "wean", "weaning", "extubat", "ventilator liberation",
    "spontaneous breathing trial", "sbt", "liberation"
]

# --------------------------------------------------

def read_json(path: Path):
    if not path.exists(): return None
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)

def write_jsonl(path: Path, rows):
    with path.open("w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

def write_csv(path: Path, rows, cols):
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in rows:
            rr = r.copy()
            if isinstance(rr.get("authors"), list):
                rr["authors"] = "; ".join(rr["authors"])
            w.writerow(rr)

def normalize_text(s):
    return re.sub(r"\s+", " ", s or "").strip()

def norm_title_key(title):
    t = normalize_text(title).lower()
    t = re.sub(r"[^a-z0-9]+", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t

def safe_year(text):
    if not text: return None
    m = re.search(r"(19|20)\d{2}", str(text))
    return int(m.group(0)) if m else None

def norm_record(source, _id=None, title=None, authors=None, year=None, doi=None, url=None, venue=None, extra=None):
    return {
        "source": source,
        "id": _id or "",
        "title": normalize_text(title) if title else "",
        "authors": authors or [],
        "year": int(year) if isinstance(year, int) or (isinstance(year, str) and year.isdigit()) else "",
        "doi": (doi or "").replace("https://doi.org/","").strip(),
        "url": (url or "").strip(),
        "venue": normalize_text(venue) if venue else "",
        "extra": extra or {},  # stash snippets, etc.
    }

# ------------ Normalizers per source --------------

def normalize_google_scholar():
    p = RAW / "google_scholar_all.json"
    data = read_json(p)
    if not data: return []
    recs = []
    for it in data.get("items", []):
        title = it.get("title")
        url = it.get("link")
        if not url and isinstance(it.get("resources"), list) and it["resources"]:
            url = it["resources"][0].get("link")
        pub = it.get("publication_info") or {}
        # authors
        authors = []
        if isinstance(pub.get("authors"), list):
            authors = [a.get("name") for a in pub["authors"] if isinstance(a, dict) and a.get("name")]
        else:
            summary = pub.get("summary") or ""
            first = summary.split(" - ")[0] if " - " in summary else ""
            if first:
                authors = [x.strip() for x in first.split(",") if x.strip()][:12]
        year = pub.get("year") or safe_year(pub.get("summary") or it.get("snippet"))
        venue = None
        extra = {"snippet": it.get("snippet") or "", "pub_summary": pub.get("summary") or ""}
        recs.append(norm_record("google_scholar", url or title, title, authors, year, None, url, venue, extra))
    return recs

def normalize_semantic_scholar():
    p = RAW / "semanticscholar_all.json"
    data = read_json(p)
    if not data: return []
    items = data.get("items", []) or data.get("data", [])
    recs = []
    for it in items:
        _id = it.get("paperId") or (it.get("externalIds") or {}).get("CorpusId")
        title = it.get("title")
        authors = [a.get("name") for a in (it.get("authors") or []) if isinstance(a, dict) and a.get("name")]
        year = it.get("year")
        doi = it.get("doi") or (it.get("externalIds") or {}).get("DOI")
        url = it.get("url")
        venue = it.get("venue")
        recs.append(norm_record("semantic_scholar", _id or doi or url or title, title, authors, year, doi, url, venue))
    return recs

def normalize_openalex():
    p = RAW / "openalex_all.json"
    data = read_json(p)
    if not data: return []
    items = data.get("items", []) or data.get("results", [])
    recs = []
    for it in items:
        _id = it.get("id")
        title = it.get("display_name")
        year = it.get("publication_year")
        doi = (it.get("doi") or "").replace("https://doi.org/","")
        url = ("https://doi.org/" + doi) if doi else _id
        venue = (it.get("host_venue") or {}).get("display_name")
        authors = []
        for au in (it.get("authorships") or []):
            ad = au.get("author") or {}
            if ad.get("display_name"): authors.append(ad["display_name"])
        recs.append(norm_record("openalex", _id or doi or url or title, title, authors, year, doi, url, venue))
    return recs

def normalize_pubmed():
    # We stored PMIDs only (from ESearch). Titles/DOIs require EFetch (optional future step).
    p = RAW / "pubmed_esearch_all.json"
    data = read_json(p)
    if not data: return []
    recs = []
    for pmid in data.get("pmids", []):
        recs.append(norm_record("pubmed", f"PMID:{pmid}", None, None, None, None, f"https://pubmed.ncbi.nlm.nih.gov/{pmid}/", None))
    return recs

def normalize_arxiv():
    p = RAW / "arxiv_all.json"
    data = read_json(p)
    if not data: return []
    ns = {"atom":"http://www.w3.org/2005/Atom", "arxiv":"http://arxiv.org/schemas/atom"}
    recs = []
    for page in data.get("pages", []):
        try:
            root = ET.fromstring(page.get("xml",""))
        except Exception:
            continue
        for e in root.findall("atom:entry", ns):
            _id = (e.findtext("atom:id", default="", namespaces=ns) or "").strip()
            title = (e.findtext("atom:title", default="", namespaces=ns) or "").strip().replace("\n"," ")
            authors = [a.findtext("atom:name", default="", namespaces=ns) for a in e.findall("atom:author", ns)]
            published = e.findtext("atom:published", default="", namespaces=ns) or ""
            year = safe_year(published)
            doi = None
            for d in e.findall("arxiv:doi", ns):
                if d.text: doi = d.text.strip()
            url = _id
            recs.append(norm_record("arxiv", _id or doi or title, title, authors, year, doi, url, "arXiv"))
    return recs

def normalize_scopus():
    p = RAW / "scopus_all.json"
    data = read_json(p)
    if not data: return []
    entries = data.get("entries") or (data.get("search-results", {}) or {}).get("entry") or []
    recs = []
    for it in entries:
        title = it.get("dc:title") or it.get("title")
        doi = it.get("prism:doi")
        url = it.get("prism:url")
        if not url and isinstance(it.get("link"), list) and it["link"]:
            url = it["link"][0].get("@href")
        venue = it.get("prism:publicationName")
        cover = it.get("prism:coverDate") or ""
        year = safe_year(cover)
        authors = []
        au = it.get("author")
        if isinstance(au, list):
            for a in au:
                if isinstance(a, dict):
                    nm = a.get("authname") or a.get("given-name") or a.get("surname")
                    if nm: authors.append(nm)
        _id = it.get("dc:identifier") or doi or url or title
        recs.append(norm_record("scopus", _id, title, authors, year, doi, url, venue))
    return recs

def normalize_wos():
    p = RAW / "wos_all.json"
    data = read_json(p)
    if not data: return []
    recs = []
    for page in data.get("pages", []):
        payload = page.get("payload") or {}
        records = (payload.get("Data") or {}).get("Records")
        if not isinstance(records, list): continue
        for r in records:
            _id = r.get("UID") or r.get("uid") or ""
            # Title can be in list/dict under "Title"
            title = ""
            tt = r.get("Title")
            if isinstance(tt, list) and tt:
                title = tt[0].get("Title") or ""
            elif isinstance(tt, dict):
                title = tt.get("Title") or ""
            # DOI (best-effort sniff anywhere in record)
            doi = None
            rec_str = json.dumps(r, ensure_ascii=False)
            m = re.search(r"10\.\d{4,9}/\S+\b", rec_str)
            if m: doi = m.group(0)
            year = safe_year(rec_str)
            url = f"https://www.webofscience.com/wos/woscc/full-record/{_id}" if _id else ""
            recs.append(norm_record("web_of_science", _id or doi or title, title, [], year, doi, url, ""))
    return recs

# ------------- Deduplication ---------------------

SOURCE_PRIORITY = [
    # Prefer richer/bibliographic sources when DOI/title tie
    "web_of_science", "scopus", "openalex", "semantic_scholar", "google_scholar", "arxiv", "pubmed"
]

def better_record(a, b):
    """Choose a 'better' record between a and b when keys collide."""
    # Prefer presence of DOI
    if (a.get("doi") and not b.get("doi")): return a
    if (b.get("doi") and not a.get("doi")): return b
    # Prefer longer title (heuristic for completeness)
    if len(a.get("title","")) > len(b.get("title","")): return a
    if len(b.get("title","")) > len(a.get("title","")): return b
    # Prefer source priority
    if SOURCE_PRIORITY.index(a["source"]) < SOURCE_PRIORITY.index(b["source"]): return a
    return a  # default

def deduplicate(records):
    by_doi, by_title = {}, {}
    kept = []
    for r in records:
        doi = (r.get("doi") or "").lower()
        if doi:
            if doi in by_doi:
                best = better_record(by_doi[doi], r)
                by_doi[doi] = best
            else:
                by_doi[doi] = r
            continue
        # no DOI → fallback to normalized title key (+year if present)
        key = norm_title_key(r.get("title",""))
        if not key:
            kept.append(r)
            continue
        if r.get("year"):
            key = f"{key}|{r['year']}"
        if key in by_title:
            best = better_record(by_title[key], r)
            by_title[key] = best
        else:
            by_title[key] = r
    # merge
    kept.extend(by_doi.values())
    kept.extend(by_title.values())
    return kept

# ------------- Filtering (MV + Weaning [+ RL]) -------------

def compile_terms(terms):
    flags = 0 if CASE_SENSITIVE else re.IGNORECASE
    return [re.compile(t if t.startswith(r"\b") or "[" in t or "*" in t or " " in t else re.escape(t), flags) for t in terms]

RL_RX = compile_terms(RL_TERMS)
MV_RX = compile_terms(MV_TERMS)
WEAN_RX = compile_terms(WEAN_TERMS)

def text_blob(rec):
    parts = [rec.get("title",""), rec.get("venue","")]
    extra = rec.get("extra") or {}
    # include snippet/pub_summary if present (Google Scholar)
    parts.append(extra.get("snippet",""))
    parts.append(extra.get("pub_summary",""))
    return " ".join([p for p in parts if p])

def matches_any(rx_list, text):
    return any(r.search(text) for r in rx_list)

def apply_filters(records):
    screened_in = []
    screened_out = []
    for r in records:
        blob = text_blob(r)
        has_mv = matches_any(MV_RX, blob)
        has_wean = matches_any(WEAN_RX, blob)
        has_rl = matches_any(RL_RX, blob)

        if has_mv and has_wean and (has_rl or not STRICT_REQUIRE_RL):
            r["match_mv"] = has_mv
            r["match_weaning"] = has_wean
            r["match_rl"] = has_rl
            screened_in.append(r)
        else:
            reason = []
            if not has_mv:
                reason.append("no_mv")
            if not has_wean:
                reason.append("no_weaning")
            if STRICT_REQUIRE_RL and not has_rl:
                reason.append("no_rl")
            r["auto_exclude_reason"] = ",".join(reason) if reason else "no_match"
            screened_out.append(r)

    return screened_in, screened_out

# ------------------ Main ------------------------

def main():
    # 1) normalize all sources
    all_records = []
    all_records += normalize_google_scholar()
    all_records += normalize_semantic_scholar()
    all_records += normalize_openalex()
    all_records += normalize_arxiv()
    all_records += normalize_scopus()
    all_records += normalize_wos()
    all_records += normalize_pubmed()  # PMIDs only (URLs; refine later if you fetch details)

    # 2) deduplicate
    dedup = deduplicate(all_records)

    # 3) filter (MV + Weaning [+ RL optional])
    screened_in, screened_out = apply_filters(dedup)

    # 4) write outputs
    cols = ["source","id","title","authors","year","doi","url","venue"]
    write_jsonl(OUT / "unified_all.jsonl", dedup)
    write_csv(OUT / "unified_all.csv", dedup, cols)

    write_csv(OUT / "screened_candidates.csv", screened_in, cols + ["match_mv","match_weaning","match_rl"])
    write_csv(OUT / "excluded_non_mv_weaning.csv", screened_out, cols + ["auto_exclude_reason"])

    # 5) simple PRISMA-style counts
    prisma = {
        "identified_raw_total": len(all_records),
        "after_dedup": len(dedup),
        "auto_screen_in": len(screened_in),
        "auto_screen_out": len(screened_out),
        "notes": {
            "strict_require_rl": STRICT_REQUIRE_RL,
            "mv_terms": MV_TERMS,
            "weaning_terms": WEAN_TERMS,
            "rl_terms": RL_TERMS
        }
    }
    with (OUT / "prisma_counts.json").open("w", encoding="utf-8") as f:
        json.dump(prisma, f, ensure_ascii=False, indent=2)

    print("\nDone.")
    print(f"- Normalized & de-duplicated: {OUT/'unified_all.csv'}  (n={len(dedup)})")
    print(f"- Screened candidates (MV+weaning): {OUT/'screened_candidates.csv'}  (n={len(screened_in)})")
    print(f"- Auto-excluded: {OUT/'excluded_non_mv_weaning.csv'}  (n={len(screened_out)})")
    print(f"- PRISMA counts JSON: {OUT/'prisma_counts.json'}")

if __name__ == "__main__":
    main()
