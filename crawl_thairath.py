import os
import sys
import time
import json
import csv
import re
from datetime import datetime, timedelta
from urllib.parse import urlparse
import requests
from bs4 import BeautifulSoup
import xml.etree.ElementTree as ET
from dotenv import load_dotenv

# Load .env
load_dotenv()

# ---------- Config ----------
SUBFILTERS = {
    'economics': lambda u: '/money/economics/thai_economics/' in u,
    'investment': lambda u: '/money/investment/' in u,
    'tech_innovation': lambda u: '/money/tech_innovation/' in u,
    'politic': lambda u: '/news/politic/' in u,
    'personal_finance': lambda u: '/money/personal_finance/' in u,
    'business_marketing': lambda u: '/money/business_marketing/' in u,
}

SECTION_PREFIX = {
    'economics': '/money/economics',
    'investment': '/money/investment',
    'tech_innovation': '/money/tech_innovation',
    'politic': '/news/politic',
    'personal_finance': '/money/personal_finance',
    'business_marketing': '/money/business_marketing',
}

# ---------- Utils ----------

def parse_args():
    args = {}
    for i, arg in enumerate(sys.argv):
        if arg.startswith('--'):
            key = arg[2:]
            val = sys.argv[i+1] if i + 1 < len(sys.argv) and not sys.argv[i+1].startswith('--') else True
            args[key] = val
    return args

def ensure_dir(path):
    os.makedirs(path, exist_ok=True)

def csv_escape(val):
    if val is None: return ''
    s = str(val).replace('\n', ' ').strip()
    return s

def normalize_date(date_str):
    if not date_str: return None
    try:
        # Basic ISO parser
        dt = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
        return dt.isoformat()
    except:
        return None

# ---------- Network ----------

def fetch_text(url, timeout=5):
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(url, timeout=timeout, headers=headers)
        response.raise_for_status()
        return response.text
    except requests.exceptions.RequestException as e:
        raise e

def fetch_with_fast_retry(url):
    try:
        return fetch_text(url, timeout=2)
    except Exception:
        time.sleep(0.2)
        return fetch_text(url, timeout=3)

# ---------- Sitemap & Logic ----------

def fetch_sitemap_urls(root_url, max_sitemaps=500):
    urls = []
    seen = set()

    def load_one(url):
        if len(seen) >= max_sitemaps: return
        try:
            xml_content = fetch_text(url, timeout=10)
            root = ET.fromstring(xml_content)
            
            # Handle Namespace
            ns = {'ns': root.tag.split('}')[0].strip('{')} if '}' in root.tag else {}
            
            # Sitemap Index
            for sitemap in root.findall('.//ns:sitemap', ns) or root.findall('.//sitemap'):
                loc = sitemap.find('ns:loc', ns).text if ns else sitemap.find('loc').text
                if loc and loc not in seen:
                    seen.add(loc)
                    load_one(loc)

            # URL Set
            for url_tag in root.findall('.//ns:url', ns) or root.findall('.//url'):
                loc = url_tag.find('ns:loc', ns).text if ns else url_tag.find('loc').text
                lastmod_tag = url_tag.find('ns:lastmod', ns) if ns else url_tag.find('lastmod')
                lastmod = None
                if lastmod_tag is not None:
                    try:
                        lastmod = datetime.fromisoformat(lastmod_tag.text.replace('Z', '+00:00'))
                    except: pass
                if loc: urls.append({'loc': loc, 'lastmod': lastmod})
        except Exception as e:
            print(f"[sitemap] fail: {url} -> {e}")

    load_one(root_url)
    return urls

def extract_article(html, url):
    soup = BeautifulSoup(html, 'html.parser')
    
    headline = ""
    og_title = soup.find("meta", property="og:title")
    if og_title: headline = og_title.get("content")
    elif soup.h1: headline = soup.h1.get_text().strip()

    date_str = ""
    pub_time = soup.find("meta", property="article:published_time")
    if pub_time: date_str = pub_time.get("content")
    
    published_iso = normalize_date(date_str)

    content = ""
    candidates = ['main', 'article', '.article-content', '.content-article']
    for sel in candidates:
        container = soup.select_one(sel)
        if container:
            ps = container.find_all('p')
            paragraphs = [p.get_text().strip() for p in ps if len(p.get_text().strip()) > 2]
            if paragraphs:
                content = "\n".join(paragraphs)
                break
    
    summary = ""
    desc = soup.find("meta", property="og:description") or soup.find("meta", attrs={"name": "description"})
    if desc: summary = desc.get("content", "").strip()

    return {
        "url": url,
        "headline": headline,
        "summary": summary,
        "content": content,
        "published_iso": published_iso or ""
    }

def which_section(url, sections_set):
    for s in sections_set:
        prefix = SECTION_PREFIX.get(s)
        sub_filter = SUBFILTERS.get(s)
        if prefix and prefix in url:
            if not sub_filter or sub_filter(url):
                return s
    return None

def extract_id_from_url(u):
    m = re.search(r'/(\d+)(?:$|[/?#])', u)
    return m.group(1) if m else ""

# ---------- Main Execution ----------

def main():
    args = parse_args()
    sitemap_url = args.get('sitemap')
    if not sitemap_url:
        print("Error: --sitemap is required")
        return

    since_days = int(args.get('since-days', 1825))
    sections_str = args.get('sections', 'economics,investment,tech_innovation,politic')
    limit = int(args.get('limit', 0))
    out_json_base = args.get('out-json', 'data/thairath_dataset.json')
    out_csv_base = args.get('out-csv', 'data/thairath_dataset.csv')

    sections = [s.strip() for s in sections_str.split(',') if s.strip()]
    section_set = set(sections)
    since_date = datetime.now() - timedelta(days=since_days)

    print(f"[*] Fetching sitemap: {sitemap_url}")
    all_urls = fetch_sitemap_urls(sitemap_url)
    
    # Filter and Group
    filtered = []
    seen_locs = set()
    for item in all_urls:
        sec = which_section(item['loc'], section_set)
        if sec and item['loc'] not in seen_locs:
            if item['lastmod'] and item['lastmod'].replace(tzinfo=None) < since_date.replace(tzinfo=None):
                continue
            filtered.append(item)
            seen_locs.add(item['loc'])

    # Logic to process the queue...
    ok, skip, fail = 0, 0, 0
    results = {s: [] for s in sections}

    for item in filtered:
        loc = item['loc']
        sec = which_section(loc, section_set)
        
        try:
            html = fetch_with_fast_retry(loc)
            meta = extract_article(html, loc)
            
            record = {
                "agency": "thairath",
                "section": sec,
                "id": extract_id_from_url(loc) or str(int(time.time())),
                "published_iso": meta['published_iso'],
                "headline": meta['headline'],
                "summary": meta['summary'],
                "content": meta['content'],
                "url": loc
            }
            
            results[sec].append(record)
            ok += 1
            print(f"✔ saved: {loc}")
            time.sleep(0.05)
            
        except Exception as e:
            print(f"✘ fail: {loc} -> {e}")
            fail += 1

    # Save outputs
    for sec, items in results.items():
        if not items: continue
            
        # Save CSV
        csv_path = out_csv_base.replace('.csv', f'_{sec}.csv')
        with open(csv_path, 'w', encoding='utf-8-sig', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=items[0].keys())
            writer.writeheader()
            writer.writerows(items)

    print(f"Done. OK: {ok}, Skip: {skip}, Fail: {fail}")

if __name__ == "__main__":
    main()