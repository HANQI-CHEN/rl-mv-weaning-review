#!/usr/bin/env python3
import csv, json, re, time
from pathlib import Path
from urllib.parse import urlparse
import xml.etree.ElementTree as ET
import requests

BASE = Path("paper_list/normalized")
RAW = Path("paper_list/raw")
OUT = BASE  # write next to normalized files

# --------------- load unified records (CSV or JSONL) ---------------
def load_unified():
    jl = BASE / "unified_all.jsonl"
    if jl.exists():
        rows = []
        with jl.open("r", encoding="utf-8") as f:
            for line in f:
                try:
                    rows.append(json.loads(line))
                except:
                    pass
        return rows

    csv_path = BASE / "unified_all.csv"
    rows = []
    if csv_path.exists():
        with csv_path.open("r", encoding="utf-8") as f:
            r = csv.DictReader(f)
            for row in r:
                # authors may be "a; b; c" in CSV -> list
                a = row.get("authors") or ""
                row["authors"] = [x.strip() for x in a.split(";") if x.strip()]
                rows.append(row)
        return rows

    raise SystemExit("Could not find unified_all.jsonl or unified_all.csv")

def save_jsonl(path: Path, rows):
    with path.open("w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

def save_csv(path: Path, rows):
    cols = ["source","id","title","authors","year","doi","url","venue","abstract"]
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            rr = r.copy()
            if isinstance(rr.get("authors"), list):
                rr["authors"] = "; ".join(rr["authors"])
            w.writerow(rr)

def safe_year(text):
    if not text: return None
    m = re.search(r"(19|20)\d{2}", str(text))
    return int(m.group(0)) if m else None

# ------------------- PubMed (EFetch) -------------------
def fetch_pubmed_abstracts(pmids, chunk=200, sleep=0.34):
    """Return dict PMID->abstract (string)."""
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
        except Exception as e:
            # keep going
            pass
        time.sleep(sleep)
    return out

# ------------------- Semantic Scholar (batch) -------------------
def s2_batch_by_ids(ids, fields="title,abstract,year,authors,venue,url", chunk=100, sleep=0.5):
    """ids like ['DOI:10.1000/xyz','PMID:1234'] -> dict key->object"""
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
                _id = obj.get("paperId") or obj.get("externalIds",{}).get("DOI") or obj.get("url")
                # Prefer DOI key if available to map back
                doi = (obj.get("externalIds") or {}).get("DOI")
                key = f"DOI:{doi}" if doi else (obj.get("externalIds") or {}).get("PMID") and f"PMID:{(obj.get('externalIds') or {}).get('PMID')}"
                if not key and obj.get("paperId"):
                    key = f"S2:{obj['paperId']}"
                if not key and obj.get("url"):
                    key = obj["url"]
                if key:
                    out[key] = obj
        except Exception:
            pass
        time.sleep(sleep)
    return out

# ------------------- OpenAlex (single-by-id) -------------------
def reconstruct_openalex_abstract(abstract_inv_idx):
    """Turn OpenAlex abstract_inverted_index into plain text."""
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
    """openalex_id = 'https://openalex.org/Wxxxx' or 'Wxxxx'"""
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

# ------------------- arXiv (batch by id_list) -------------------
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
    """arxiv_ids can be full abs URLs or '1905.13167v1' strings."""
    out = {}
    if not arxiv_ids: return out
    base = "http://export.arxiv.org/api/query"
    def norm(x):
        # accept http://arxiv.org/abs/xxx or raw id
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

# ------------------- driver -------------------
def main():
    recs = load_unified()

    # Collect identifiers
    pmids = []
    dois = []
    openalex_ids = []
    arxiv_ids = []

    for r in recs:
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

        # arXiv: from id or url
        if src == "arxiv":
            if rid:
                arxiv_ids.append(rid)
            elif url:
                arxiv_ids.append(url)
        elif "arxiv.org/abs/" in (url or ""):
            arxiv_ids.append(url)

    # 1) PubMed abstracts
    pmid_to_abs = fetch_pubmed_abstracts(sorted(set(pmids)))

    # 2) Semantic Scholar (batch) by DOI (and leftover PMIDs as fallback)
    s2_ids = [f"DOI:{d}" for d in sorted(set(dois))]
    # Optional: also include PMIDs that didn't return from PubMed
    missing_pmids = [p for p in set(pmids) if p not in pmid_to_abs]
    s2_ids += [f"PMID:{p}" for p in missing_pmids]
    s2_map = s2_batch_by_ids(s2_ids)

    # 3) OpenAlex abstracts (one by one; only for those still missing)
    openalex_ids = list(dict.fromkeys(openalex_ids))  # unique preserve order
    openalex_map = {}
    for wid in openalex_ids:
        a = fetch_openalex_abstract(wid)
        if a:
            openalex_map[wid.split("/")[-1]] = a
        time.sleep(0.2)

    # 4) arXiv abstracts (batch)
    arxiv_map = fetch_arxiv_abstracts(list(dict.fromkeys(arxiv_ids)))

    # Attach abstracts to records
    enriched = []
    found = missed = 0
    for r in recs:
        abstract = ""
        src = (r.get("source") or "").lower()
        rid = r.get("id") or ""
        doi = (r.get("doi") or "").strip()
        url = r.get("url") or ""

        # PubMed
        if src == "pubmed" and rid.startswith("PMID:"):
            pmid = rid.split("PMID:")[1]
            abstract = pmid_to_abs.get(pmid, "")

        # DOI via Semantic Scholar
        if not abstract and doi:
            obj = s2_map.get(f"DOI:{doi}")
            if obj and obj.get("abstract"):
                abstract = obj["abstract"]

        # PMIDs via S2 fallback
        if not abstract and src == "pubmed" and rid.startswith("PMID:"):
            obj = s2_map.get(rid) or s2_map.get(f"PMID:{rid.split('PMID:')[1]}")
            if obj and obj.get("abstract"):
                abstract = obj["abstract"]

        # OpenAlex
        if not abstract and (src == "openalex" or "openalex.org" in rid):
            key = rid.split("/")[-1]
            abstract = openalex_map.get(key, "")

        # arXiv
        if not abstract and (src == "arxiv" or "arxiv.org/abs/" in url):
            # arXiv map keyed by full abs URL or ID; normalize to URL in map
            # Our fetch stored keys as full atom <id> like 'http://arxiv.org/abs/xxxx'
            # Try url first, then rid
            abstract = arxiv_map.get(url, "") or arxiv_map.get(rid, "")

        r2 = dict(r)
        r2["abstract"] = re.sub(r"\s+", " ", abstract).strip()
        if r2["abstract"]:
            found += 1
        else:
            missed += 1
        enriched.append(r2)

    # Save
    OUT.mkdir(parents=True, exist_ok=True)
    save_jsonl(OUT / "unified_with_abstracts.jsonl", enriched)
    save_csv(OUT / "unified_with_abstracts.csv", enriched)

    print(f"\nAbstracts found for {found} records; missing for {missed}.")
    print(f"Wrote:\n  - {OUT/'unified_with_abstracts.jsonl'}\n  - {OUT/'unified_with_abstracts.csv'}")

if __name__ == "__main__":
    main()
