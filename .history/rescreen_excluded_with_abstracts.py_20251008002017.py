#!/usr/bin/env python3
"""
Rescreen excluded records using abstracts (MV + Weaning keywords)

Input :
  paper_list/normalized/excluded_non_mv_weaning.csv

Output (keepers only: rescue + no_abstract; confirmed_exclude is omitted):
  paper_list/normalized/abstract_check.csv
  paper_list/normalized/abstract_check.jsonl

Also updates paper_list/normalized/prisma_counts.json by adding:
  "abstract_rescreen": { "checked": N, "rescued": R, "no_abstract": U }
  "auto_screen_in_after_abstract": old_in + R
  "auto_screen_out_after_abstract": max(0, old_out - R)
"""

from pathlib import Path
import csv, json, re, time, xml.etree.ElementTree as ET
import requests

NORM_DIR = Path("paper_list/normalized")
EXCLUDED_CSV = NORM_DIR / "excluded_non_mv_weaning.csv"
PRISMA_JSON = NORM_DIR / "prisma_counts.json"
OUT_CHECK_CSV = NORM_DIR / "abstract_check.csv"
OUT_CHECK_JL  = NORM_DIR / "abstract_check.jsonl"

# ------------------ tuning knobs ------------------
STRICT_REQUIRE_RL = False  # set True to also require RL terms in abstract
CASE_SENSITIVE = False     # case-insensitive by default

# Term lists (align with your main filter)
RL_TERMS = [
    r"reinforcement learning", r"\bMDP\b", r"markov decision", r"\bQ-?learning\b",
    r"fitted q", r"policy gradient", r"actor-critic", r"offline reinforcement",
    r"deep reinforcement", r"inverse reinforcement",
]
MV_TERMS = [
    "mechanical ventilation", "ventilator", "ventilatory support",
    "ventilation", "ventilated", "patient-ventilator"
]
WEAN_TERMS = [
    "wean", "weaning", "extubat", "ventilator liberation",
    "spontaneous breathing trial", "sbt", "liberation"
]

# ------------------- regex helpers -------------------
def compile_terms(terms):
    flags = 0 if CASE_SENSITIVE else re.IGNORECASE
    rx = []
    for t in terms:
        # keep raw regex for patterns with anchors/specials/spaces; else escape
        if any(ch in t for ch in r"\[]()|?*+{}") or " " in t:
            rx.append(re.compile(t, flags))
        else:
            rx.append(re.compile(re.escape(t), flags))
    return rx

RL_RX   = compile_terms(RL_TERMS)
MV_RX   = compile_terms(MV_TERMS)
WEAN_RX = compile_terms(WEAN_TERMS)

def matches_any(rx_list, text):
    if not text:
        return False
    return any(r.search(text) for r in rx_list)

# ------------------- file I/O -------------------
def load_excluded_csv():
    rows = []
    if not EXCLUDED_CSV.exists():
        raise SystemExit(f"Missing {EXCLUDED_CSV}")
    with EXCLUDED_CSV.open("r", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            # normalize authors -> list
            a = row.get("authors") or ""
            row["authors"] = [x.strip() for x in a.split(";") if x.strip()]
            rows.append(row)
    return rows

def save_jsonl(path: Path, rows):
    with path.open("w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

def save_csv(path: Path, rows):
    cols = ["source","id","title","authors","year","doi","url","venue",
            "abstract","match_mv_abs","match_wean_abs","match_rl_abs","decision"]
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

# ------------------- PubMed EFetch -------------------
def fetch_pubmed_abstracts(pmids, chunk=200, sleep=0.34):
    """Return dict PMID->abstract."""
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
            # continue on errors
            pass
        time.sleep(sleep)
    return out

# ------------------- Semantic Scholar batch -------------------
def s2_batch_by_ids(ids, fields="title,abstract,year,authors,venue,url", chunk=100, sleep=0.5):
    """ids like ['DOI:10.xxxx/yyy','PMID:1234'] -> dict key->object"""
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

# ------------------- OpenAlex per-id -------------------
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

# ------------------- arXiv batch -------------------
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

# ------------------- main -------------------
def main():
    rows = load_excluded_csv()

    # collect identifiers
    pmids, dois, openalex_ids, arxiv_ids = [], [], [], []
    for r in rows:
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

    # 1) PubMed
    pmid_to_abs = fetch_pubmed_abstracts(sorted(set(pmids)))

    # 2) Semantic Scholar as fallback (by DOI, then remaining PMIDs)
    s2_ids = [f"DOI:{d}" for d in sorted(set(dois))]
    missing_pmids = [p for p in set(pmids) if p not in pmid_to_abs]
    s2_ids += [f"PMID:{p}" for p in missing_pmids]
    s2_map = s2_batch_by_ids(s2_ids)

    # 3) OpenAlex (per work id)
    openalex_map = {}
    for wid in list(dict.fromkeys(openalex_ids)):
        a = fetch_openalex_abstract(wid)
        if a:
            openalex_map[wid.split("/")[-1]] = a
        time.sleep(0.2)

    # 4) arXiv (batch)
    arxiv_map = fetch_arxiv_abstracts(list(dict.fromkeys(arxiv_ids)))

    # evaluate matches and build output rows
    out_rows = []
    rescued = confirmed = noabs = 0

    for r in rows:
        abstract = ""
        src = (r.get("source") or "").lower()
        rid = r.get("id") or ""
        doi = (r.get("doi") or "").strip()
        url = r.get("url") or ""

        # try sources in order
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
            # our arXiv map keyed by atom <id> URL or ID
            abstract = arxiv_map.get(url, "") or arxiv_map.get(rid, "")

        abs_clean = re.sub(r"\s+", " ", abstract or "").strip()

        # match on abstract only (as requested)
        mv   = matches_any(MV_RX, abs_clean)
        wean = matches_any(WEAN_RX, abs_clean)
        rl   = matches_any(RL_RX, abs_clean)

        if abs_clean:
            if mv and wean and (rl or not STRICT_REQUIRE_RL):
                decision = "rescue"  # bring back to candidates
                rescued += 1
            else:
                decision = "confirmed_exclude"
                confirmed += 1
        else:
            decision = "keep_no_abstract"
            noabs += 1

        r2 = dict(r)
        r2["abstract"] = abs_clean
        r2["match_mv_abs"] = mv
        r2["match_wean_abs"] = wean
        r2["match_rl_abs"] = rl
        r2["decision"] = decision
        out_rows.append(r2)

    # ---------------- save outputs (EXCLUDE confirmed_exclude) ----------------
    keepers = [r for r in out_rows if r.get("decision") != "confirmed_exclude"]

    save_csv(OUT_CHECK_CSV, keepers)
    save_jsonl(OUT_CHECK_JL, keepers)
    print(f"Saved keepers only (rescue + no_abstract):")
    print(f" - {OUT_CHECK_CSV}  (n={len(keepers)})")
    print(f" - {OUT_CHECK_JL}   (n={len(keepers)})")
    print(f"Removed confirmed_exclude: {confirmed}")

    # ---------------- update PRISMA counts (omit confirmed_excluded key) -----
    abstract_rescreen = {
        "checked": len(out_rows),  # all rows we evaluated
        "rescued": rescued,        # moved back in due to abstract
        "no_abstract": noabs       # kept for manual screening
        # 'confirmed_excluded' intentionally omitted
    }

    # read existing PRISMA
    prisma = {}
    if PRISMA_JSON.exists():
        try:
            prisma = json.loads(PRISMA_JSON.read_text(encoding="utf-8"))
        except Exception:
            prisma = {}

    old_in  = int(prisma.get("auto_screen_in", 0))
    old_out = int(prisma.get("auto_screen_out", 0))

    prisma["abstract_rescreen"] = abstract_rescreen
    prisma["auto_screen_in_after_abstract"]  = old_in + rescued
    prisma["auto_screen_out_after_abstract"] = max(0, old_out - rescued)

    with PRISMA_JSON.open("w", encoding="utf-8") as f:
        json.dump(prisma, f, ensure_ascii=False, indent=2)

    print("\nPRISMA updated:")
    print(json.dumps(abstract_rescreen, indent=2, ensure_ascii=False))
    print(f'auto_screen_in_after_abstract:  {prisma["auto_screen_in_after_abstract"]}')
    print(f'auto_screen_out_after_abstract: {prisma["auto_screen_out_after_abstract"]}')

if __name__ == "__main__":
    main()
