import requests, time, os, json
import xml.etree.ElementTree as ET
from pathlib import Path
from dotenv import load_dotenv
import math

load_dotenv()

# ---------- setup: ensure output folder and helper functions ----------
BASE_DIR = Path("paper_list")
RAW_DIR = BASE_DIR / "raw"
RAW_DIR.mkdir(parents=True, exist_ok=True)

def save_json(path, data):
    out_path = RAW_DIR / path
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"Saved → {out_path}")

def save_text(path, text):
    out_path = RAW_DIR / path
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(text)
    print(f"Saved → {out_path}")

# -------------------- PubMed ------------------------------------------------------------
# Expanded RL block to include more methods (DQN, PPO, etc.) and concepts (Sequential Decision, Adaptive Control)
RL = (
    # Core Concepts and Synonyms
    '("reinforcement learning"[tiab] OR "inverse reinforcement"[tiab] OR "sequential decision"[tiab] OR "adaptive control"[tiab] OR "control policy"[tiab])'
    # Core Methods/Algorithms (Q-Learning, Policy Gradient, etc.)
    ' OR ("Q-learning"[tiab] OR "fitted Q"[tiab] OR "policy gradient"[tiab] OR "value iteration"[tiab] OR "monte carlo tree search"[tiab] OR MCTS[tiab])'
    # Deep Learning Integration
    ' OR ("deep reinforcement"[tiab] OR DQN[tiab] OR "deep Q"[tiab] OR A2C[tiab] OR A3C[tiab] OR PPO[tiab] OR DDPG[tiab] OR SAC[tiab] OR TRPO[tiab])'
    # Markov Process Formalism
    ' OR ("markov decision process"[tiab] OR "markov decision"[tiab] OR MDP[tiab])'
    # Types of RL (Offline, Batch, etc.)
    ' OR ("offline reinforcement"[tiab] OR "batch reinforcement"[tiab] OR "off-policy"[tiab] OR "on-policy"[tiab])'
)
MV = '("mechanical ventilation"[tiab] OR ventilator[tiab] OR "ventilatory support"[tiab])'
WEAN = '(wean*[tiab] OR extubat*[tiab] OR "ventilator liberation"[tiab] OR "spontaneous breathing trial"[tiab] OR SBT[tiab])'
term = f"({RL}) AND ({MV}) AND ({WEAN})"
print("PubMed Search Term:\n", term) # Print the final expanded search term

try:
    url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
    base_params = {"db": "pubmed", "retmode": "json", "term": term}

    # get total count
    r = requests.get(url, params={**base_params, "retmax": 0}, timeout=30)
    r.raise_for_status()
    info = r.json()
    total = int(info["esearchresult"]["count"])
    print("PubMed count:", total)

    # fetch all PMIDs in chunks
    RETMAX = 10000
    all_pmids, pages = [], 0
    for start in range(0, total, RETMAX):
        r = requests.get(url, params={**base_params, "retstart": start, "retmax": RETMAX}, timeout=30)
        r.raise_for_status()
        data = r.json()
        pmids = data["esearchresult"].get("idlist", [])
        all_pmids.extend(pmids)
        pages += 1
        print(f"PubMed page {pages} (retstart={start}) fetched: {len(pmids)}")
        time.sleep(0.34)  # polite pacing

    save_json("pubmed_esearch_all.json", {
        "term": term,
        "pages_fetched": pages,
        "total_reported": total,
        "total_pmids_concat": len(all_pmids),
        "pmids": all_pmids
    })
except Exception as e:
    save_text("pubmed_ERROR.txt", str(e))

# -------------------- OpenAlex ------------------------------------------------------------
try:
    # Expanded OpenAlex search to include key RL acronyms for robustness
    search = ('("reinforcement learning" OR "Q-learning" OR PPO OR DQN OR "markov decision" OR MDP) '
              '("mechanical ventilation" OR ventilator OR "ventilatory support") '
              '(wean OR extubat OR "spontaneous breathing trial")')
    print("OpenAlex Search Term:\n", search)
    
    BASE = "https://api.openalex.org/works"
    PER_PAGE = 200       # OpenAlex max per page
    cursor = "*"         # cursor-based pagination
    SLEEP_SEC = 0.2

    all_items, pages = [], 0
    total_reported = None
    while True:
        r = requests.get(BASE, params={"search": search, "per-page": PER_PAGE, "cursor": cursor}, timeout=30)
        r.raise_for_status()
        data = r.json()
        if total_reported is None:
            total_reported = (data.get("meta") or {}).get("count")
            print("OpenAlex total (reported):", total_reported)
        items = data.get("results", [])
        all_items.extend(items)
        pages += 1
        nxt = (data.get("meta") or {}).get("next_cursor")
        print(f"OpenAlex page {pages} fetched: {len(items)}")
        if not nxt or not items:
            break
        cursor = nxt
        time.sleep(SLEEP_SEC)

    save_json("openalex_all.json", {
        "search": search,
        "per_page": PER_PAGE,
        "pages_fetched": pages,
        "total_reported": total_reported,
        "total_items_concat": len(all_items),
        "items": all_items
    })
except Exception as e:
    save_text("openalex_ERROR.txt", str(e))

# -------------------- Semantic Scholar --------------------
# from inside the original code: import os, math, time, json, requests
try:
    # Your original setup functions/imports are already defined above or in the original context
    
    # Expanded Semantic Scholar query (similar expansion as OpenAlex)
    query = ('("reinforcement learning" OR "Q-learning" OR PPO OR DQN OR "markov decision" OR MDP) '
             'AND ("mechanical ventilation" OR ventilator OR "ventilatory support") '
             'AND (weaning OR extubation OR "spontaneous breathing trial")')
    print("Semantic Scholar Search Term:\n", query)

    BASE = "https://api.semanticscholar.org/graph/v1/paper/search"
    FIELDS = "paperId,title,year,venue,url,externalIds,authors"
    LIMIT = 100          # max allowed per page
    MAX_PAGES = 50       # safety cap
    SLEEP_SEC = 0.6

    S2_API_KEY = os.getenv("SEMANTIC_SCHOLAR_API_KEY")
    headers = {"x-api-key": S2_API_KEY} if S2_API_KEY else {}

    # 1) Probe to get total results
    r0 = requests.get(
        BASE,
        params={"query": query, "limit": 1, "offset": 0, "fields": "paperId"},
        headers=headers,
        timeout=30
    )
    r0.raise_for_status()
    total = r0.json().get("total", 0)
    print("Semantic Scholar total (reported):", total)

    all_items = []
    pages = min(MAX_PAGES, math.ceil(total / LIMIT)) if total else 0

    # 2) Page safely (stop if API says no more data or data is empty)
    for page in range(pages):
        offset = page * LIMIT
        r = requests.get(
            BASE,
            params={"query": query, "limit": LIMIT, "offset": offset, "fields": FIELDS},
            headers=headers,
            timeout=30
        )
        if r.status_code == 400:
            print(f"S2: stopping early at offset={offset} (no more data).")
            break
        r.raise_for_status()

        data = r.json().get("data", []) or []
        if not data:
            print(f"S2: empty page at offset={offset}; stopping.")
            break

        all_items.extend(data)
        print(f"S2 page {page+1}/{pages} (offset={offset}) fetched: {len(data)}")
        time.sleep(SLEEP_SEC)

    # 3) Save a single consolidated JSON
    save_json("semanticscholar_all.json", {
        "query": query,
        "fields": FIELDS,
        "limit_per_page": LIMIT,
        "pages_attempted": pages,
        "total_reported": total,
        "total_items_concat": len(all_items),
        "items": all_items
    })

except Exception as e:
    with open(RAW_DIR / "semanticscholar_ERROR.txt", "w", encoding="utf-8") as f:
        f.write(str(e))
    print("Semantic Scholar fetch failed:", e)

# -------------------- arXiv (robust) ------------------------------------------------------------
try:
    BASE = "http://export.arxiv.org/api/query"
    # Keep original arXiv query structure, which is already quite robust for keyword searching
    QUERIES = [
        'all:"reinforcement learning" AND all:"mechanical ventilation" AND all:wean',
        'all:"reinforcement learning" AND all:"mechanical ventilation" AND all:extubat',
        'all:"reinforcement learning" AND all:"mechanical ventilation" AND all:"spontaneous breathing trial"',
        'all:"reinforcement learning" AND all:ventilator AND all:wean',
        'all:"reinforcement learning" AND all:ventilator AND all:extubat',
    ]
    FALLBACK = 'all:"reinforcement learning" AND (all:"mechanical ventilation" OR all:ventilator)'

    MAX_RESULTS = 200     # arXiv allows up to 300, but 200 is safe
    SLEEP_SEC = 3.0       # recommended: ~3s between calls
    # !! IMPORTANT: Update your email address below to comply with arXiv's policy !!
    UA = "rl-mv-weaning-review/0.1 (mailto:your_email@example.com)" 

    headers = {"User-Agent": UA}
    ns = {"opensearch": "http://a9.com/-/spec/opensearch/1.1/"}

    def fetch_query(q):
        """Fetch all pages for a given query; return list of {start, xml} pages."""
        r0 = requests.get(
            BASE,
            params={"search_query": q, "start": 0, "max_results": 1},
            headers=headers, timeout=60
        )
        r0.raise_for_status()
        root = ET.fromstring(r0.text)
        total_el = root.find("opensearch:totalResults", ns)
        total = int(total_el.text) if total_el is not None else 0
        pages = []
        if total == 0:
            print(f'arXiv query returned 0: {q}')
            return pages, total

        num_pages = math.ceil(total / MAX_RESULTS)
        for p in range(num_pages):
            start = p * MAX_RESULTS
            r = requests.get(
                BASE,
                params={"search_query": q, "start": start, "max_results": MAX_RESULTS},
                headers=headers, timeout=60
            )
            r.raise_for_status()
            pages.append({"query": q, "start": start, "xml": r.text})
            print(f"arXiv fetched: q='{q}' start={start} count<= {MAX_RESULTS}")
            time.sleep(SLEEP_SEC)
        return pages, total

    all_pages = []
    totals = []
    for q in QUERIES:
        pages, total = fetch_query(q)
        totals.append({"query": q, "total": total})
        all_pages.extend(pages)

    if all(t["total"] == 0 for t in totals):
        print("All targeted arXiv queries returned 0. Trying broader fallback…")
        pages, total = fetch_query(FALLBACK)
        totals.append({"query": FALLBACK, "total": total})
        all_pages.extend(pages)

    save_json("arxiv_all.json", {
        "queries": QUERIES,
        "fallback_query": FALLBACK,
        "max_results_per_page": MAX_RESULTS,
        "pages_fetched": len(all_pages),
        "totals_reported": totals,
        "pages": all_pages
    })
except Exception as e:
    save_text("arxiv_ERROR.txt", str(e))

# # -------------------- Scopus (Unchanged from original commented block) --------------------
# try:
#     # accept either Scopus_API_KEY or SCOPUS_API_KEY
#     Scopus_API_KEY = os.getenv("Scopus_API_KEY") or os.getenv("SCOPUS_API_KEY")
#     if not Scopus_API_KEY:
#         raise RuntimeError("SCOPUS_API_KEY / Scopus_API_KEY env var not set.")
#     INSTTOKEN = os.getenv("SCOPUS_INSTTOKEN") or os.getenv("Scopus_INSTTOKEN")  # optional

#     headers = {"X-ELS-APIKey": Scopus_API_KEY, "Accept": "application/json"}
#     if INSTTOKEN:
#         headers["X-ELS-Insttoken"] = INSTTOKEN

#     query = 'TITLE-ABS-KEY("reinforcement learning" AND ("mechanical ventilation" OR ventilator OR "ventilatory support") AND (wean* OR extubat* OR "spontaneous breathing trial" OR SBT))'
#     BASE = "https://api.elsevier.com/content/search/scopus"
#     COUNT = 25          # typical page size
#     start, pages, all_entries = 0, 0, []
#     total_reported = None

#     while True:
#         r = requests.get(BASE, headers=headers, params={"query": query, "start": start, "count": COUNT}, timeout=30)
#         if r.status_code in (401, 403):
#             raise RuntimeError(f"Scopus auth/entitlement issue: {r.status_code} {r.text[:200]}")
#         r.raise_for_status()
#         data = r.json()
#         if total_reported is None:
#             total_reported = int((data.get("search-results", {}) or {}).get("opensearch:totalResults", "0") or "0")
#             print("Scopus total (reported):", total_reported)
#         entries = (data.get("search-results", {}) or {}).get("entry", []) or []
#         all_entries.extend(entries)
#         pages += 1
#         print(f"Scopus page {pages} (start={start}) fetched: {len(entries)}")
#         if len(entries) < COUNT:
#             break
#         start += COUNT
#         time.sleep(0.5)

#     save_json("scopus_all.json", {
#         "query": query,
#         "count_per_page": COUNT,
#         "pages_fetched": pages,
#         "total_reported": total_reported,
#         "total_items_concat": len(all_entries),
#         "entries": all_entries
#     })
# except Exception as e:
#     save_text("scopus_ERROR.txt", str(e))

# # -------------------- Web of Science (Unchanged from original commented block) --------------------
# try:
#     WOS_API_KEY = os.getenv("WOS_API_KEY") or ""
#     if not WOS_API_KEY:
#         raise RuntimeError("WOS_API_KEY env var not set.")
#     usrQuery = 'TS=("reinforcement learning" AND ("mechanical ventilation" OR ventilator) AND (wean* OR extubat* OR "spontaneous breathing trial"))'

#     BASE = "https://api.clarivate.com/api/wos"
#     COUNT = 100        # often up to 100 per page
#     first, pages, all_pages = 1, 0, []
#     total_reported = None

#     while True:
#         r = requests.get(
#             BASE,
#             headers={"X-ApiKey": WOS_API_KEY},
#             params={"databaseId": "WOS", "usrQuery": usrQuery, "count": COUNT, "firstRecord": first},
#             timeout=60
#         )
#         if r.status_code in (401, 403):
#             raise RuntimeError(f"WOS auth/entitlement issue: {r.status_code} {r.text[:200]}")
#         r.raise_for_status()
#         data = r.json()
#         all_pages.append({"firstRecord": first, "payload": data})
#         pages += 1

#         # total records reported (varies by deployment)
#         if total_reported is None:
#             qres = (data.get("QueryResult") or {})
#             total_reported = qres.get("RecordsFound") or (data.get("Data") or {}).get("RecordsFound")
#             print("Web of Science total (reported):", total_reported)

#         # infer how many were returned on this page
#         returned = 0
#         recs = (data.get("Data") or {}).get("Records")
#         if isinstance(recs, list):
#             returned = len(recs)
#         print(f"WOS page {pages} (firstRecord={first}) fetched ~{returned}")
#         if returned < COUNT:
#             break
#         first += COUNT
#         time.sleep(0.5)

#     save_json("wos_all.json", {
#         "usrQuery": usrQuery,
#         "count_per_page": COUNT,
#         "pages_fetched": pages,
#         "total_reported": total_reported,
#         "pages": all_pages
#     })
# except Exception as e:
#     save_text("wos_ERROR.txt", str(e))

# -------------------- Google Scholar via SerpAPI (multi-page → single JSON) --------------------
try:
    SerpAPI_KEY = os.getenv("SerpAPI_KEY")
    if not SerpAPI_KEY:
        raise RuntimeError("SerpAPI_KEY env var not set.")

    # Expanded Google Scholar search (similar expansion as OpenAlex/S2)
    q = ('("reinforcement learning" OR "Q-learning" OR PPO OR DQN OR "markov decision" OR MDP) '
         'AND ("mechanical ventilation" OR ventilator OR "ventilatory support") '
         'AND (wean OR extubat OR "spontaneous breathing trial")')
    print("Google Scholar Search Term:\n", q)
    
    BASE = "https://serpapi.com/search.json"
    NUM_PER_PAGE = 20          # Google Scholar max is 20 per page
    PAGES = 10                  # fetch at least 5 pages
    SLEEP_SEC = 1.0            # polite pacing

    all_items = []
    pages_fetched = 0

    for page in range(PAGES):
        start = page * NUM_PER_PAGE  # 0,20,40,60,80
        params = {
            "engine": "google_scholar",
            "q": q,
            "num": NUM_PER_PAGE,
            "start": start,
            "api_key": SerpAPI_KEY,
        }
        r = requests.get(BASE, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        items = data.get("organic_results", []) or data.get("scholar_results", [])
        print(f"Google Scholar page {page+1} (start={start}) fetched: {len(items)}")
        all_items.extend(items)
        pages_fetched += 1
        time.sleep(SLEEP_SEC)

    combined = {
        "query": q,
        "num_per_page": NUM_PER_PAGE,
        "pages_fetched": pages_fetched,
        "total_items_concat": len(all_items),
        "items": all_items
    }
    save_json("google_scholar_all.json", combined)

except Exception as e:
    save_text("google_scholar_ERROR.txt", str(e))

# -------------------- done (no further processing) --------------------
print("\nDone. Raw outputs saved in ./paper_list/")