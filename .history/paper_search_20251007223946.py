import requests, time, os
import xml.etree.ElementTree as ET
from dotenv import load_dotenv, find_dotenv

# PubMed (no key)
RL = '("reinforcement learning"[tiab] OR "inverse reinforcement"[tiab] OR "markov decision"[tiab] OR MDP[tiab] OR "Q-learning"[tiab] OR "fitted Q"[tiab] OR "policy gradient"[tiab] OR "offline reinforcement"[tiab] OR "deep reinforcement"[tiab])'
MV = '("mechanical ventilation"[tiab] OR ventilator[tiab] OR "ventilatory support"[tiab])'
WEAN = '(wean*[tiab] OR extubat*[tiab] OR "ventilator liberation"[tiab] OR "spontaneous breathing trial"[tiab] OR SBT[tiab])'
term = f"({RL}) AND ({MV}) AND ({WEAN})"

url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
r = requests.get(url, params={"db":"pubmed","retmode":"json","retmax":"0","term":term}, timeout=30)
r.raise_for_status()
count = int(r.json()["esearchresult"]["count"])
print("PubMed count:", count)

# OpenAlex (no key)
search = '"reinforcement learning" "mechanical ventilation" (wean OR extubat OR "spontaneous breathing trial")'
r = requests.get("https://api.openalex.org/works", params={"search": search, "per-page": 1}, timeout=30)
r.raise_for_status()
print("OpenAlex count:", r.json().get("meta", {}).get("count", 0))

# Semantic Scholar (no key)
query = '"reinforcement learning" "mechanical ventilation" (weaning OR extubation OR "spontaneous breathing trial")'
r = requests.get(
    "https://api.semanticscholar.org/graph/v1/paper/search",
    params={"query": query, "limit": 1, "fields": "title"},
    timeout=30
)
r.raise_for_status()
print("Semantic Scholar total:", r.json().get("total", 0))

# arXiv (no key)
q = 'all:"reinforcement learning" AND all:(ventilator OR "mechanical ventilation") AND all:(weaning OR extubation OR "spontaneous breathing trial")'
r = requests.get("http://export.arxiv.org/api/query", params={"search_query": q, "start": 0, "max_results": 1}, timeout=30)
r.raise_for_status()
root = ET.fromstring(r.text)
ns = {"opensearch": "http://a9.com/-/spec/opensearch/1.1/"}
total = root.find("opensearch:totalResults", ns)
print("arXiv total:", int(total.text) if total is not None else 0)


# Scopus
# Scopus_API_KEY = os.getenv("SCOPUS_API_KEY")

# query = 'TITLE-ABS-KEY("reinforcement learning" AND ("mechanical ventilation" OR ventilator OR "ventilatory support") AND (wean* OR extubat* OR "spontaneous breathing trial" OR SBT))'
# r = requests.get(
#     "https://api.elsevier.com/content/search/scopus",
#     headers={"X-ELS-APIKey": Scopus_API_KEY, "Accept": "application/json"},
#     params={"query": query, "count": 1},
#     timeout=30
# )
# r.raise_for_status()
# data = r.json()
# print("Scopus totalResults:", int(data["search-results"]["opensearch:totalResults"]))

# Web of Science

# WOS_API_KEY = ""
# usrQuery = 'TS=("reinforcement learning" AND ("mechanical ventilation" OR ventilator) AND (wean* OR extubat* OR "spontaneous breathing trial"))'
# r = requests.get(
#     "https://api.clarivate.com/api/wos",
#     headers={"X-ApiKey": WOS_API_KEY},
#     params={"databaseId": "WOS", "usrQuery": usrQuery, "count": 1, "firstRecord": 1},
#     timeout=30
# )
# r.raise_for_status()
# data = r.json()
# # Different deployments return different shapes; try both.
# total = (data.get("QueryResult", {}) or {}).get("RecordsFound")
# if total is None and "Data" in data:
#     total = data["Data"].get("RecordsFound")
# print("Web of Science RecordsFound:", int(total) if total is not None else 0)

# Google Scholar via SerpAPI

# SerpAPI_KEY = "59aacd5a287fb9ec7de2d30ae597be09dba23d7d675ddc3357083a2674c24c7f"
SerpAPI_KEY = os.getenv("SerpAPI_KEY")
print(SerpAPI_KEY)


# q = '"reinforcement learning" "mechanical ventilation" (wean OR extubat OR "spontaneous breathing trial")'

# BASE = "https://serpapi.com/search.json"
# NUM_PER_PAGE = 20          # Google Scholar max is 20 per page
# PAGES = 5                  # fetch at least 5 pages
# SLEEP_SEC = 1.0            # be polite; avoid rate limits

# all_items = []
# for page in range(PAGES):
#     start = page * NUM_PER_PAGE  # 0,20,40,60,80
#     params = {
#         "engine": "google_scholar",
#         "q": q,
#         "num": NUM_PER_PAGE,
#         "start": start,
#         "api_key": SerpAPI_KEY,
#     }
#     r = requests.get(BASE, params=params, timeout=30)
#     r.raise_for_status()
#     data = r.json()

#     # Scholar results can appear under 'organic_results' (most common)
#     # or 'scholar_results' depending on endpoint/version.
#     items = data.get("organic_results", []) or data.get("scholar_results", [])
#     print(f"Page {page+1} (start={start}) fetched: {len(items)}")
#     all_items.extend(items)

#     time.sleep(SLEEP_SEC)  # gentle pacing

# # De-duplicate by result link if available
# def key_fn(it):
#     # prefer 'link' field, fall back to title
#     return it.get("link") or it.get("title")

# uniq = {}
# for it in all_items:
#     k = key_fn(it)
#     if k:
#         uniq[k] = it
#     else:
#         # keep unmatched entries too
#         uniq[id(it)] = it

# print(f"\nTotal fetched (raw): {len(all_items)}")
# print(f"Total unique (by link/title): {len(uniq)}")

# # Optional: print a few titles
# for i, it in enumerate(list(uniq.values())[:], 1):
#     print(f"{i}. {it.get('title')}")