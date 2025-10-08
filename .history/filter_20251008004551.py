#!/usr/bin/env python3
"""
Filter & Deduplicate for RL + Mechanical Ventilation Weaning
------------------------------------------------------------
Stage 1 (unchanged from your pipeline):
- Read raw combined files from paper_list/raw/, normalize into a single schema,
  deduplicate across sources, then apply keyword filters to prioritize
  Mechanical Ventilation + Weaning (MV+Weaning) papers.
- Outputs:
  - paper_list/normalized/unified_all.jsonl / unified_all.csv
  - paper_list/normalized/screened_candidates.csv             (pass MV+Weaning filter)
  - paper_list/normalized/excluded_non_mv_weaning.csv         (auto-excluded with reason)
  - paper_list/normalized/prisma_counts.json                  (counts)

Stage 2 (NEW):
- Scan abstracts for papers in screened_candidates.csv
- If abstract is FOUND: keep only if abstract has BOTH RL and MV terms
- If abstract is NOT FOUND: KEEP (do not punish missing abstracts)
- Outputs:
  - paper_list/normalized/abstract_check.csv / abstract_check.jsonl  (KEPT only)
  - prisma_counts.json updated with abstract-stage counts:
    {
      "abstract_stage": {
        "checked": N,
        "kept": K,
        "dropped_by_abstract": D,
        "no_abstract": U
      },
      "auto_screen_in_after_abstract": auto_screen_in + K_adjustment (== auto_screen_in - D)
    }
"""

from pathlib import Path
import json, csv, re, time, xml.etree.ElementTree as ET
import requests  # NEW

RAW = Path("paper_list/raw")
OUT = Path("paper_list/normalized")
OUT.mkdir(parents=True, exist_ok=True)

# ------------------ tuning knobs ------------------
STRICT_REQUIRE_RL = False  # Stage 1 only: set True to ALSO require RL keyword match in title/snippet screen
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
        # Ignore any keys not listed in `cols` (e.g., 'extra', 'auto_exclude_reason', 'match_mv', etc.)
        w = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore")
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

# ------------------ Abstract fetching (Stage 2) ------------------

def abs_compile_terms(terms):
    # abstracts are case-insensitive checks
    rx = []
    for t in terms:
        if any(ch in t for ch in r"\[]()|?*+{}") or " " in t:
            rx.append(re.compile(t, re.IGNORECASE))
        else:
            rx.append(re.compile(re.escape(t), re.IGNORECASE))
    return rx

ABS_RL_RX   = abs_compile_terms(RL_TERMS)
ABS_MV_RX   = abs_compile_terms(MV_TERMS)
ABS_WEAN_RX = abs_compile_terms(WEAN_TERMS)  # computed but not used for decision

def abs_matches_any(rx_list, text):
    if not text: return False
    return any(r.search(text) for r in rx_list)

def load_csv_rows(path: Path):
    rows = []
    if not path.exists(): return rows
    with path.open("r", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            a = row.get("authors") or ""
            row["authors"] = [x.strip() for x in a.split(";") if x.strip()]
            rows.append(row)
    return rows

# PubMed EFetch: PMID -> abstract
def fetch_pubmed_abstracts(pmids, chunk=200, sleep=0.34):
    out = {}
    if not pmids: return out
    url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"
    for i in range(0, len(pmids), chunk):
        batch = pmids[i:i+chunk]
        try:
            r = requests.get(url, params={"db":"pubmed","id":",".join(batch),"retmode":"xml"}, timeout=30)
            r.raise_for_status()
            root = ET.fromstring(r.text)
            for article in root.findall(".//PubmedArticle"):
                pmid = article.findtext(".//PMID")
                abstract_texts = []
                for ab in article.findall(".//Abstract/AbstractText"):
                    part = (ab.text or "").strip()
                    label = ab.get("Label")
                    if label: part = f"{label}: {part}"
                    if part: abstract_texts.append(part)
                abstract = "\n".join(abstract_texts).strip()
                if pmid and abstract:
                    out[pmid] = abstract
        except Exception:
            pass
        time.sleep(sleep)
    return out

# Semantic Scholar batch: DOI/PMID -> abstract
def s2_batch_by_ids(ids, fields="title,abstract,year,authors,venue,url", chunk=100, sleep=0.5):
    out = {}
    if not ids: return out
    url = f"https://api.semanticscholar.org/graph/v1/paper/batch?fields={fields}"
    headers = {"Content-Type":"application/json"}
    for i in range(0, len(ids), chunk):
        batch = ids[i:i+chunk]
        body = {"ids": batch}
        try:
            r = requests.post(url, headers=headers, data=json.dumps(body), timeout=45)
            r.raise_for_status()
            arr = r.json()
            for obj in arr:
                doi  = (obj.get("externalIds") or {}).get("DOI")
                pmid = (obj.get("externalIds") or {}).get("PMID")
                key = None
                if doi: key = f"DOI:{doi}"
                elif pmid: key = f"PMID:{pmid}"
                elif obj.get("paperId"): key = f"S2:{obj['paperId']}"
                elif obj.get("url"): key = obj["url"]
                if key: out[key] = obj
        except Exception:
            pass
        time.sleep(sleep)
    return out

# OpenAlex per-id abstract reconstruction
def reconstruct_openalex_abstract(abstract_inv_idx):
    if not isinstance(abstract_inv_idx, dict):
        return ""
    maxpos = -1
    for positions in abstract_inv_idx.values():
        if positions:
            maxpos = max(maxpos, max(positions))
    words = [""] * (maxpos + 1 if maxpos >= 0 else 0)
    for word, positions in abstract_inv_idx.items():
        for pos in positions:
            if 0 <= pos < len(words):
                words[pos] = word
    return " ".join([w for w in words if w]).strip()

def fetch_openalex_abstract(openalex_id):
    if not openalex_id: return ""
    wid = openalex_id.split("/")[-1]
    url = f"https://api.openalex.org/works/{wid}"
    try:
        r = requests.get(url, params={"select":"id,doi,abstract_inverted_index"}, timeout=20)
        r.raise_for_status()
        data = r.json()
        return reconstruct_openalex_abstract(data.get("abstract_inverted_index"))
    except Exception:
        return ""

# arXiv batch by id_list
def parse_arxiv_summary(xml_text):
    ns = {"atom":"http://www.w3.org/2005/Atom"}
    try:
        root = ET.fromstring(xml_text)
        d = {}
        for e in root.findall("atom:entry", ns):
            _id = (e.findtext("atom:id", default="", namespaces=ns) or "").strip()
            summary = e.findtext("atom:summary", default="", namespaces=ns) or ""
            if _id:
                d[_id] = re.sub(r"\s+", " ", summary).strip()
        return d
    except Exception:
        return {}

def fetch_arxiv_abstracts(arxiv_ids, chunk=50, sleep=1.0):
    out = {}
    if not arxiv_ids: return out
    base = "http://export.arxiv.org/api/query"
    def norm(x):
        if "arxiv.org" in x:
            return x.rstrip("/").split("/")[-1]
        return x
    ids = [norm(x) for x in arxiv_ids]
    for i in range(0, len(ids), chunk):
        id_list = ",".join(ids[i:i+chunk])
        try:
            r = requests.get(base, params={"id_list": id_list}, timeout=30)
            r.raise_for_status()
            d = parse_arxiv_summary(r.text)
            out.update(d)
        except Exception:
            pass
        time.sleep(sleep)
    return out

def abstract_stage(screened_rows):
    """
    For each paper in screened_candidates.csv:
      - Try fetch abstract via PubMed -> S2 (DOI/PMID) -> OpenAlex -> arXiv
      - Keep if: no abstract OR (abstract has RL AND MV)
      - Drop if: abstract present AND missing RL or MV
    Returns (keepers, dropped, counts_dict)
    """
    # Collect identifiers
    pmids, dois, openalex_ids, arxiv_ids = [], [], [], []
    for r in screened_rows:
        src = (r.get("source") or "").lower()
        rid = r.get("id") or ""
        doi = (r.get("doi") or "").strip()
        url = r.get("url") or ""

        if src == "pubmed" and rid.startswith("PMID:"):
            pmids.append(rid.split("PMID:")[1])
        if doi:
            dois.append(doi)
        if "openalex.org" in rid or (src == "openalex" and rid):
            openalex_ids.append(rid)
        if src == "arxiv":
            if rid: arxiv_ids.append(rid)
            elif url: arxiv_ids.append(url)
        elif "arxiv.org/abs/" in url:
            arxiv_ids.append(url)

    pmid_to_abs = fetch_pubmed_abstracts(sorted(set(pmids)))
    s2_ids = [f"DOI:{d}" for d in sorted(set(dois))]
    missing_pmids = [p for p in set(pmids) if p not in pmid_to_abs]
    s2_ids += [f"PMID:{p}" for p in missing_pmids]
    s2_map = s2_batch_by_ids(s2_ids)

    openalex_map = {}
    for wid in list(dict.fromkeys(openalex_ids)):
        a = fetch_openalex_abstract(wid)
        if a:
            openalex_map[wid.split("/")[-1]] = a
        time.sleep(0.2)

    arxiv_map = fetch_arxiv_abstracts(list(dict.fromkeys(arxiv_ids)))

    # Evaluate
    keepers, dropped = [], []
    checked = kept = dropped_cnt = noabs = 0

    for r in screened_rows:
        checked += 1
        abstract = ""
        src = (r.get("source") or "").lower()
        rid = r.get("id") or ""
        doi = (r.get("doi") or "").strip()
        url = r.get("url") or ""

        if src == "pubmed" and rid.startswith("PMID:"):
            pmid = rid.split("PMID:")[1]
            abstract = pmid_to_abs.get(pmid, "")

        if not abstract and doi:
            obj = s2_map.get(f"DOI:{doi}")
            if obj and obj.get("abstract"):
                abstract = obj["abstract"]

        if not abstract and src == "pubmed" and rid.startswith("PMID:"):
            obj = s2_map.get(f"PMID:{rid.split('PMID:')[1]}")
            if obj and obj.get("abstract"):
                abstract = obj["abstract"]

        if not abstract and (src == "openalex" or "openalex.org" in rid):
            key = rid.split("/")[-1]
            abstract = openalex_map.get(key, "")

        if not abstract and (src == "arxiv" or "arxiv.org/abs/" in url):
            abstract = arxiv_map.get(url, "") or arxiv_map.get(rid, "")

        abs_clean = re.sub(r"\s+", " ", abstract or "").strip()

        mv   = abs_matches_any(ABS_MV_RX,   abs_clean)
        rl   = abs_matches_any(ABS_RL_RX,   abs_clean)
        wean = abs_matches_any(ABS_WEAN_RX, abs_clean)  # computed but not required

        if not abs_clean:
            # No abstract found -> KEEP
            r2 = dict(r)
            r2["abstract"] = ""
            r2["match_mv_abs"] = False
            r2["match_wean_abs"] = False
            r2["match_rl_abs"] = False
            r2["decision"] = "keep_no_abstract"
            keepers.append(r2)
            kept += 1
            noabs += 1
        else:
            if mv and rl:
                # Abstract has BOTH RL & MV -> KEEP
                r2 = dict(r)
                r2["abstract"] = abs_clean
                r2["match_mv_abs"] = mv
                r2["match_wean_abs"] = wean
                r2["match_rl_abs"] = rl
                r2["decision"] = "keep"
                keepers.append(r2)
                kept += 1
            else:
                # Abstract present but missing RL or MV -> DROP
                r2 = dict(r)
                r2["abstract"] = abs_clean
                r2["match_mv_abs"] = mv
                r2["match_wean_abs"] = wean
                r2["match_rl_abs"] = rl
                r2["decision"] = "drop_by_abstract"
                dropped.append(r2)
                dropped_cnt += 1

    counts = {
        "checked": checked,
        "kept": kept,
        "dropped_by_abstract": dropped_cnt,
        "no_abstract": noabs
    }
    return keepers, dropped, counts

# ------------------ Main ------------------------

def main():
    # -------- Stage 1: normalize -> dedup -> keyword screen --------
    all_records = []
    all_records += normalize_google_scholar()
    all_records += normalize_semantic_scholar()
    all_records += normalize_openalex()
    all_records += normalize_arxiv()
    all_records += normalize_scopus()
    all_records += normalize_wos()
    all_records += normalize_pubmed()  # PMIDs only (URLs; refine later if you fetch details)

    dedup = deduplicate(all_records)
    screened_in, screened_out = apply_filters(dedup)

    cols = ["source","id","title","authors","year","doi","url","venue"]
    write_jsonl(OUT / "unified_all.jsonl", dedup)
    write_csv(OUT / "unified_all.csv", dedup, cols)

    write_csv(OUT / "screened_candidates.csv", screened_in, cols + ["match_mv","match_weaning","match_rl"])
    write_csv(OUT / "excluded_non_mv_weaning.csv", screened_out, cols + ["auto_exclude_reason"])

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

    # -------- Stage 2: abstract scan on screened_candidates --------
    if screened_in:
        kept, dropped, abs_counts = abstract_stage(screened_in)

        # Save KEPT ONLY as abstract_check.*
        abs_cols = ["source","id","title","authors","year","doi","url","venue",
                    "abstract","match_mv_abs","match_wean_abs","match_rl_abs","decision"]
        write_csv(OUT / "abstract_check.csv", kept, abs_cols)
        write_jsonl(OUT / "abstract_check.jsonl", kept)

        # Update PRISMA counts
        prisma["abstract_stage"] = abs_counts
        prisma["auto_screen_in_after_abstract"] = prisma["auto_screen_in"] - abs_counts["dropped_by_abstract"]
    else:
        # No screened_in -> write empty abstract files and keep counts neutral
        write_csv(OUT / "abstract_check.csv", [], ["source","id","title","authors","year","doi","url","venue",
                                                   "abstract","match_mv_abs","match_wean_abs","match_rl_abs","decision"])
        write_jsonl(OUT / "abstract_check.jsonl", [])
        prisma["abstract_stage"] = {"checked": 0, "kept": 0, "dropped_by_abstract": 0, "no_abstract": 0}
        prisma["auto_screen_in_after_abstract"] = 0

    # Persist PRISMA
    with (OUT / "prisma_counts.json").open("w", encoding="utf-8") as f:
        json.dump(prisma, f, ensure_ascii=False, indent=2)

    # Logs
    print("\nStage 1 complete.")
    print(f"- Normalized & de-duplicated: {OUT/'unified_all.csv'}  (n={len(dedup)})")
    print(f"- Screened candidates (MV+weaning): {OUT/'screened_candidates.csv'}  (n={len(screened_in)})")
    print(f"- Auto-excluded: {OUT/'excluded_non_mv_weaning.csv'}  (n={len(screened_out)})")
    print("Stage 2 abstract scan:")
    print(f"- abstract_check.csv written with KEPT items only.")

    print(f"- PRISMA counts JSON: {OUT/'prisma_counts.json'}")

if __name__ == "__main__":
    main()
