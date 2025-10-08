#!/usr/bin/env python3
import json, csv, re
from pathlib import Path
import xml.etree.ElementTree as ET

ROOT = Path("paper_list")
OUT = ROOT / "normalized"
OUT.mkdir(parents=True, exist_ok=True)

def write_jsonl(path: Path, rows):
    with path.open("w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

def write_csv(path: Path, rows):
    cols = ["source","id","title","authors","year","doi","url","venue"]
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in rows:
            out = r.copy()
            out["authors"] = "; ".join(out.get("authors") or [])
            w.writerow(out)

def norm_record(source, _id=None, title=None, authors=None, year=None, doi=None, url=None, venue=None):
    return {
        "source": source,
        "id": _id or "",
        "title": title or "",
        "authors": authors or [],
        "year": int(year) if isinstance(year, int) or (isinstance(year, str) and year.isdigit()) else "",
        "doi": (doi or "").strip(),
        "url": (url or "").strip(),
        "venue": venue or "",
    }

def safe_year(text):
    if not text: return None
    m = re.search(r"(19|20)\d{2}", str(text))
    return int(m.group(0)) if m else None

# ---------------- Google Scholar ----------------
def normalize_google_scholar():
    p = ROOT / "google_scholar_all.json"
    if not p.exists(): return []
    data = json.loads(p.read_text(encoding="utf-8"))
    recs = []
    for it in data.get("items", []):
        title = it.get("title")
        url = it.get("link") or (it.get("resources") or [{}])[0].get("link")
        pub = it.get("publication_info") or {}
        year = pub.get("year") or safe_year(pub.get("summary") or it.get("snippet"))
        # authors: structured list or parse summary "A Author, B Author - Journal, 2023 - ..."
        authors = []
        if isinstance(pub.get("authors"), list):
            authors = [a.get("name") for a in pub["authors"] if isinstance(a, dict) and a.get("name")]
        else:
            summary = (pub.get("summary") or "")
            if " - " in summary:
                first = summary.split(" - ")[0]
                authors = [x.strip() for x in first.split(",") if x.strip()][:12]
        recs.append(norm_record("google_scholar", url or title, title, authors, year, None, url, None))
    return recs

# --------------- Semantic Scholar ---------------
def normalize_semantic_scholar():
    p = ROOT / "semanticscholar_all.json"
    if not p.exists(): return []
    data = json.loads(p.read_text(encoding="utf-8"))
    recs = []
    for it in data.get("items", []) or data.get("data", []):
        _id = it.get("paperId") or (it.get("externalIds") or {}).get("CorpusId")
        title = it.get("title")
        authors = []
        for a in it.get("authors") or []:
            name = a.get("name") if isinstance(a, dict) else None
            if name: authors.append(name)
        year = it.get("year")
        doi = it.get("doi") or (it.get("externalIds") or {}).get("DOI")
        url = it.get("url")
        venue = it.get("venue")
        recs.append(norm_record("semantic_scholar", _id or doi or url or title, title, authors, year, doi, url, venue))
    return recs

# ------------------ OpenAlex --------------------
def normalize_openalex():
    p = ROOT / "openalex_all.json"
    if not p.exists(): return []
    data = json.loads(p.read_text(encoding="utf-8"))
    recs = []
    for it in data.get("items", []) or data.get("results", []):
        _id = it.get("id")
        title = it.get("display_name")
        year = it.get("publication_year")
        doi = (it.get("doi") or "").replace("https://doi.org/","")
        url = ("https://doi.org/" + doi) if doi else _id
        venue = (it.get("host_venue") or {}).get("display_name")
        authors = []
        for au in it.get("authorships") or []:
            ad = au.get("author") or {}
            if ad.get("display_name"): authors.append(ad["display_name"])
        recs.append(norm_record("openalex", _id or doi or url or title, title, authors, year, doi, url, venue))
    return recs

# ------------------- PubMed ---------------------
# NOTE: we saved only PMIDs from ESearch; titles/DOIs require EFetch later.
def normalize_pubmed():
    p = ROOT / "pubmed_esearch_all.json"
    if not p.exists(): return []
    data = json.loads(p.read_text(encoding="utf-8"))
    recs = []
    for pmid in data.get("pmids", []):
        recs.append(norm_record("pubmed", f"PMID:{pmid}", None, None, None, None, f"https://pubmed.ncbi.nlm.nih.gov/{pmid}/", None))
    return recs

# -------------------- arXiv ---------------------
def normalize_arxiv():
    p = ROOT / "arxiv_all.json"
    if not p.exists(): return []
    data = json.loads(p.read_text(encoding="utf-8"))
    ns = {"atom":"http://www.w3.org/2005/Atom", "arxiv":"http://arxiv.org/schemas/atom"}
    recs = []
    for page in data.get("pages", []):
        root = ET.fromstring(page.get("xml",""))
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
            venue = "arXiv"
            recs.append(norm_record("arxiv", _id or doi or title, title, authors, year, doi, url, venue))
    return recs

# -------------------- Scopus --------------------
def normalize_scopus():
    p = ROOT / "scopus_all.json"
    if not p.exists(): return []
    data = json.loads(p.read_text(encoding="utf-8"))
    entries = data.get("entries") or (data.get("search-results", {}) or {}).get("entry") or []
    recs = []
    for it in entries:
        title = it.get("dc:title") or it.get("title")
        doi = it.get("prism:doi")
        url = it.get("prism:url") or (it.get("link") or [{}])[0].get("@href") if isinstance(it.get("link"), list) else None
        venue = it.get("prism:publicationName")
        cover = it.get("prism:coverDate") or ""
        year = safe_year(cover)
        authors = []
        # Scopus author list can be nested; best-effort parse
        au = it.get("author")
        if isinstance(au, list):
            for a in au:
                if isinstance(a, dict):
                    nm = a.get("authname") or a.get("given-name") or a.get("surname")
                    if nm: authors.append(nm)
        _id = it.get("dc:identifier") or doi or url or title
        recs.append(norm_record("scopus", _id, title, authors, year, doi, url, venue))
    return recs

# ---------------- Web of Science ----------------
def normalize_wos():
    p = ROOT / "wos_all.json"
    if not p.exists(): return []
    data = json.loads(p.read_text(encoding="utf-8"))
    recs = []
    for page in data.get("pages", []):
        payload = page.get("payload") or {}
        records = (payload.get("Data") or {}).get("Records")
        if not isinstance(records, list): continue
        for r in records:
            _id = r.get("UID") or r.get("uid") or ""
            title = ""
            tt = r.get("Title")
            if isinstance(tt, list) and tt:
                title = tt[0].get("Title") or ""
            elif isinstance(tt, dict):
                title = tt.get("Title") or ""
            # DOI often lives inside "Other" or "Identifiers"
            doi = None
            for k, v in r.items():
                if isinstance(v, str) and "10." in v and "/" in v:
                    # crude DOI sniff
                    m = re.search(r"10\.\d{4,9}/\S+\b", v)
                    if m: doi = m.group(0); break
                if isinstance(v, dict):
                    for vv in v.values():
                        if isinstance(vv, str):
                            m = re.search(r"10\.\d{4,9}/\S+\b", vv)
                            if m: doi = m.group(0); break
            # year: search across record text
            year = safe_year(json.dumps(r, ensure_ascii=False))
            url = f"https://www.webofscience.com/wos/woscc/full-record/{_id}" if _id else ""
            venue = ""
            recs.append(norm_record("web_of_science", _id or doi or title, title, [], year, doi, url, venue))
    return recs

def main():
    unified = []
    # order doesn’t matter; append all
    unified += normalize_google_scholar()
    unified += normalize_semantic_scholar()
    unified += normalize_openalex()
    unified += normalize_arxiv()
    unified += normalize_scopus()
    unified += normalize_wos()
    unified += normalize_pubmed()   # PubMed last (IDs only for now)

    # write outputs
    write_jsonl(OUT / "unified_index.jsonl", unified)
    write_csv(OUT / "unified_index.csv", unified)

    # also save per-source jsonl (optional; comment out if you don’t need them)
    sources = {}
    for r in unified:
        sources.setdefault(r["source"], []).append(r)
    for s, rows in sources.items():
        write_jsonl(OUT / f"{s}.jsonl", rows)

    print(f"Done. Wrote {len(unified)} records to:")
    print(f"  - {OUT / 'unified_index.jsonl'}")
    print(f"  - {OUT / 'unified_index.csv'}")

if __name__ == "__main__":
    main()
