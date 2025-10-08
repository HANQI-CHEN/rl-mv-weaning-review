import requests, time, os, json
import xml.etree.ElementTree as ET
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# ---------- setup: ensure output folder ----------
OUT_DIR = Path("paper_list")
OUT_DIR.mkdir(parents=True, exist_ok=True)

def save_json(path, data):
    path = OUT_DIR / path
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"Saved → {path}")

def save_text(path, text):
    path = OUT_DIR / path
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)
    print(f"Saved → {path}")

# -------------------- PubMed (no key) --------------------
RL = '("reinforcement learning"[tiab] OR "inverse reinforcement"[tiab] OR "markov decision"[tiab] OR MDP[tiab] OR "Q-learning"[tiab] OR "fitted Q"[tiab] OR "policy gradient"[tiab] OR "offline reinforcement"[tiab] OR "deep reinforcement"[tiab])'
MV = '("mechanical ventilation"[tiab] OR ventilator[tiab] OR "ventilatory support"[tiab])'
WEAN = '(wean*[tiab] OR extubat*[tiab] OR "ventilator liberation"[tiab] OR "spontaneous breathing trial"[tiab] OR SBT[tiab])'
term = f"({RL}) AND ({MV}) AND ({WEAN})"

try:
    url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
    # keep your count call
    r = requests.get(url, params={"db":"pubmed","retmode":"json","retmax":"0","term":term}, timeout=30)
    r.raise_for_status()
    data = r.json()
    count = int(data["esearchresult"]["count"])
    print("PubMed count:", count)
    # save raw esearch JSON
    save_json("pubmed_esearch.json", data)
except Exception as e:
    save_text("pubmed_ERROR.txt", str(e))

# -------------------- OpenAlex (no key) --------------------
try:
    search = '"reinforcement learning" "mechanical ventilation" (wean OR extubat OR "spontaneous breathing trial")'
    r = requests.get("https://api.openalex.org/works", params={"search": search, "per-page": 1}, timeout=30)
    r.raise_for_status()
    data = r.json()
    print("OpenAlex count:", data.get("meta", {}).get("count", 0))
    # save raw first page JSON
    save_json("openalex_page1.json", data)
except Exception as e:
    save_text("openalex_ERROR.txt", str(e))

# -------------------- Semantic Scholar (no key) --------------------
try:
    query = '"reinforcement learning" "mechanical ventilation" (weaning OR extubation OR "spontaneous breathing trial")'
    r = requests.get(
        "https://api.semanticscholar.org/graph/v1/paper/search",
        params={"query": query, "limit": 1, "fields": "title"},
        timeout=30
    )
    r.raise_for_status()
    data = r.json()
    print("Semantic Scholar total:", data.get("total", 0))
    # save raw first page JSON
    save_json("semanticscholar_page1.json", data)
except Exception as e:
    save_text("semanticscholar_ERROR.txt", str(e))

# -------------------- arXiv (no key) --------------------
try:
    q = 'all:"reinforcement learning" AND all:(ventilator OR "mechanical ventilation") AND all:(weaning OR extubation OR "spontaneous breathing trial")'
    r = requests.get("http://export.arxiv.org/api/query", params={"search_query": q, "start": 0, "max_results": 1}, timeout=30)
    r.raise_for_status()
    xml_text = r.text
    # keep your count print
    root = ET.fromstring(xml_text)
    ns = {"opensearch": "http://a9.com/-/spec/opensearch/1.1/"}
    total = root.find("opensearch:totalResults", ns)
    print("arXiv total:", int(total.text) if total is not None else 0)
    # save raw atom XML
    save_text("arxiv_page1.xml", xml_text)
except Exception as e:
    save_text("arxiv_ERROR.txt", str(e))

# # -------------------- Scopus --------------------
# try:
#     Scopus_API_KEY = os.getenv("Scopus_API_KEY")
#     if not Scopus_API_KEY:
#         raise RuntimeError("SCOPUS_API_KEY env var not set.")
#     query = 'TITLE-ABS-KEY("reinforcement learning" AND ("mechanical ventilation" OR ventilator OR "ventilatory support") AND (wean* OR extubat* OR "spontaneous breathing trial" OR SBT))'
#     r = requests.get(
#         "https://api.elsevier.com/content/search/scopus",
#         headers={"X-ELS-APIKey": Scopus_API_KEY, "Accept": "application/json"},
#         params={"query": query, "count": 1},
#         timeout=30
#     )
#     r.raise_for_status()
#     data = r.json()
#     # keep your count print
#     print("Scopus totalResults:", int(data["search-results"]["opensearch:totalResults"]))
#     # save raw first page JSON
#     save_json("scopus_page1.json", data)
# except Exception as e:
#     # save the error details so you can inspect (403 etc.)
#     save_text("scopus_ERROR.txt", str(e))

# # -------------------- Web of Science --------------------
# try:
#     WOS_API_KEY = os.getenv("WOS_API_KEY") or ""
#     if not WOS_API_KEY:
#         raise RuntimeError("WOS_API_KEY env var not set.")
#     usrQuery = 'TS=("reinforcement learning" AND ("mechanical ventilation" OR ventilator) AND (wean* OR extubat* OR "spontaneous breathing trial"))'
#     r = requests.get(
#         "https://api.clarivate.com/api/wos",
#         headers={"X-ApiKey": WOS_API_KEY},
#         params={"databaseId": "WOS", "usrQuery": usrQuery, "count": 1, "firstRecord": 1},
#         timeout=30
#     )
#     r.raise_for_status()
#     data = r.json()
#     # Different deployments return different shapes; try both.
#     total = (data.get("QueryResult", {}) or {}).get("RecordsFound")
#     if total is None and "Data" in data:
#         total = data["Data"].get("RecordsFound")
#     print("Web of Science RecordsFound:", int(total) if total is not None else 0)
#     # save raw first page JSON
#     save_json("wos_page1.json", data)
# except Exception as e:
#     save_text("wos_ERROR.txt", str(e))

# -------------------- Google Scholar via SerpAPI --------------------
try:
    SerpAPI_KEY = os.getenv("SerpAPI_KEY")
    if not SerpAPI_KEY:
        raise RuntimeError("SerpAPI_KEY env var not set.")
    q = '"reinforcement learning" "mechanical ventilation" (wean OR extubat OR "spontaneous breathing trial")'
    BASE = "https://serpapi.com/search.json"
    NUM_PER_PAGE = 20          # Google Scholar max is 20 per page
    PAGES = 5                  # fetch at least 5 pages
    SLEEP_SEC = 1.0            # be polite; avoid rate limits

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
        # save raw JSON for each page separately
        save_json(f"google_scholar_page_{page+1}.json", data)
        time.sleep(SLEEP_SEC)
except Exception as e:
    save_text("google_scholar_ERROR.txt", str(e))

# -------------------- (no further processing in this step) --------------------
print("\nDone. Raw outputs saved in ./paper_list/")
