import os
import csv
import re
import time
import argparse
import requests
from bs4 import BeautifulSoup
import xml.etree.ElementTree as ET
import pandas as pd

def get_xml_root(url):
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        r = requests.get(url, timeout=15, headers=headers)
        r.raise_for_status()
        return ET.fromstring(r.content)
    except Exception as e:
        print(f"[!] Error reading {url}: {e}")
        return None

def collect_urls(index_url, target_years):
    print(f"[*] Analyzing Sitemap Index for years: {', '.join(target_years)}...")
    root = get_xml_root(index_url)
    if root is None: return []

    ns = {'n': root.tag.split('}')[0].strip('{')} if '}' in root.tag else {}
    sub_sitemaps = [loc.text for loc in root.findall('.//n:loc', ns) or root.findall('.//loc')]
    
    article_links = []
    for sub in sub_sitemaps:
        # Check if the sub-sitemap URL contains any of the selected years
        if any(year in sub for year in target_years):
            print(f"  [-] Opening archive: {sub.split('/')[-1]}")
            sub_root = get_xml_root(sub)
            
            # --- SAFETY GUARD START ---
            if sub_root is None:
                print(f"      [!] Warning: Could not load {sub}. Skipping...")
                continue
            
            links_elements = sub_root.findall('.//n:loc', ns) or sub_root.findall('.//loc')
            if not links_elements:
                continue
                
            links = [loc.text for loc in links_elements if loc.text]
            # --- SAFETY GUARD END ---

            # Filter for news and money sections, OPTIONAL (uncomment if needed)
            # filtered = [l for l in links if l and ('/news/' in l or '/money/' in l)]
            # article_links.extend(filtered)
            article_links.extend(links)
                
    return article_links

def scrape_article(url):
    try:
        res = requests.get(url, timeout=10, headers={'User-Agent': 'Mozilla/5.0'})
        if res.status_code != 200: return None
        soup = BeautifulSoup(res.text, 'html.parser')
        
        article_id = re.search(r'/(\d+)', url)
        headline = soup.find('h1').get_text().strip() if soup.find('h1') else "No Title"
        date_tag = soup.find('meta', property="article:published_time")
        paragraphs = soup.find_all('p')
        content = " ".join([p.get_text().strip() for p in paragraphs if len(p.get_text()) > 20])

        if not content:
            return None

        return {
            'id': article_id.group(1) if article_id else "",
            'date': date_tag['content'] if date_tag else "",
            'headline': headline,
            'content': content,
            'url': url
        }
    except:
        return None

def main():
    parser = argparse.ArgumentParser(description="Thairath Sitemap Crawler")
    parser.add_argument('--years', nargs='+', required=True, help="Years to filter (e.g. 2024 2025)")
    parser.add_argument('--limit', type=int, default=1000, help="Limit total articles to scrape")
    parser.add_argument('--out', type=str, default="thairath_export.csv", help="Output filename")
    
    args = parser.parse_args()
    
    root_sitemap = "https://www.thairath.co.th/sitemap.xml"
    urls = collect_urls(root_sitemap, args.years)
    print(f"[*] Found {len(urls)} URLs total. Starting crawl...")

    data_list = []
    i = 0
    while len(data_list) < args.limit and i < len(urls):
        data = scrape_article(urls[i])
        if data:
            data_list.append(data)
            print(f"  [{len(data_list)}/{args.limit}] Scraped: {data['headline'][:40]}...")
        i += 1
        time.sleep(0.05)
        
    if data_list:
        df = pd.DataFrame(data_list)
        df.to_csv(args.out, index=False, encoding='utf-8-sig')
        print(f"\n[SUCCESS] Saved to {args.out}")
    else:
        print("\n[!] No articles scraped. Check your filters and try again.")

if __name__ == "__main__":
    main()