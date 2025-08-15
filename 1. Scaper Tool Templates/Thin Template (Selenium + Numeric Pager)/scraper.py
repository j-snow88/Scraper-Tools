#!/usr/bin/env python3
"""
THIN SCRAPER TEMPLATE — Selenium + Numeric Pagination

Use when:
- Listing pages are reachable by appending ?page=N (or similar)
- You need to click through detail pages and extract a few fields
- The site is dynamic (renders with JS) or you just prefer Selenium

HOW TO ADAPT:
1) Edit the CONFIG section: START_URL, NUMERIC_PAGE_PARAM, SELECTORS, FIELDS.
2) Run with --limit 5 to test, then remove --limit for full run.
"""

import re
import sys
import time
import argparse
from dataclasses import dataclass
from typing import List, Optional, Dict, Any
from urllib.parse import urlparse, urlunparse, parse_qs, urlencode, urljoin

import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException

# ====================== CONFIG (EDIT) ======================

CONFIG: Dict[str, Any] = {
    # Listing entry URL (page 0). For numeric pagination, page 1 will be ?page=1 (0-index typical).
    "START_URL": "https://example.com/listings",

    # Numeric pagination
    "NUMERIC_PAGE_PARAM": "page",  # e.g., "page", "p", "offset" (adjust as needed)
    "NUMERIC_PAGE_START": 0,       # usually 0; some sites start at 1
    "MAX_PAGES": 200,              # safety cap

    # When listing is "ready" (any selector will do)
    "LISTING_READY_SELECTORS": [".results", ".items", ".view-content"],

    # CSS selectors that match DETAIL links on each listing page
    "LIST_LINK_SELECTORS": [
        "a[href*='/detail/']",
        "a.item-link",
    ],

    # Detail page fields (add/remove as needed)
    # Each field: a prioritized list of extractors:
    #   - {"css": "h1"} -> text of element
    #   - {"css_attr": ["a.download", "href"]} -> attribute value
    #   - {"regex": r"..."} -> regex over full page text
    "FIELDS": {
        "Title": [{"css": "h1"}, {"css": ".title"}],
        "Address": [{"css": ".address"},
                    {"regex": r"\d{2,5}\s+\w+.*?\b[A-Z]{2}\b\s*\d{5}(?:-\d{4})?"}],
        "Phone": [{"regex": r"(?:\+?1[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}"}],
        "Email": [{"regex": r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}"}],
    },

    # Output columns in Excel
    "OUTPUT_COLUMNS": ["Title", "Address", "Phone", "Email"],
}

# ====================== CORE (no edit needed) ======================

@dataclass
class Row:
    data: Dict[str, str]

PHONE_RE = re.compile(r"(?:\+?1[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}")

def _normalize_space(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())

def _set_page(url: str, param: str, page_num: Optional[int]) -> str:
    parts = urlparse(url)
    q = parse_qs(parts.query)
    if page_num is None:
        q.pop(param, None)
    else:
        q[param] = [str(page_num)]
    new_query = urlencode(q, doseq=True)
    return urlunparse((parts.scheme, parts.netloc, parts.path, parts.params, new_query, parts.fragment))

def _ensure_absolute(href: str, base_url: str) -> str:
    try:
        return urljoin(base_url, href)
    except Exception:
        return href

def setup_driver(headless: bool = True, timeout: int = 60) -> webdriver.Chrome:
    opts = Options()
    if headless:
        opts.add_argument("--headless=new")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--window-size=1400,900")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument("--log-level=3")
    drv = webdriver.Chrome(options=opts)
    drv.set_page_load_timeout(timeout)
    return drv

def wait_for(driver: webdriver.Chrome, selectors: List[str], extra: float = 0.4) -> None:
    if not selectors:
        return
    try:
        WebDriverWait(driver, 20).until(
            EC.any_of(*[EC.presence_of_element_located((By.CSS_SELECTOR, s)) for s in selectors])
        )
        time.sleep(extra)
    except TimeoutException:
        pass  # proceed anyway

def collect_links(driver: webdriver.Chrome, base_url: str, selectors: List[str]) -> List[str]:
    links = set()
    for sel in selectors:
        for a in driver.find_elements(By.CSS_SELECTOR, sel):
            href = a.get_attribute("href")
            if href:
                links.add(_ensure_absolute(href, base_url))
    return sorted(links)

def _extract_with_specs(driver: webdriver.Chrome, specs: List[Dict[str, Any]]) -> str:
    text = ""
    try:
        text = driver.find_element(By.TAG_NAME, "body").text
    except Exception:
        pass

    for spec in specs:
        if "css" in spec:
            try:
                el = driver.find_element(By.CSS_SELECTOR, spec["css"])
                val = _normalize_space(el.text)
                if val:
                    return val
            except NoSuchElementException:
                continue
        if "css_attr" in spec:
            css, attr = spec["css_attr"][0], spec["css_attr"][1]
            try:
                el = driver.find_element(By.CSS_SELECTOR, css)
                val = el.get_attribute(attr) or ""
                val = _normalize_space(val)
                if val:
                    return val
            except NoSuchElementException:
                continue
        if "regex" in spec:
            m = re.search(spec["regex"], text or "")
            if m:
                return _normalize_space(m.group(0))
    return ""

def scrape_detail(driver: webdriver.Chrome, url: str, item_delay: float, fields: Dict[str, List[Dict[str, Any]]]) -> Row:
    driver.get(url)
    WebDriverWait(driver, 30).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
    time.sleep(item_delay)
    data = {}
    for field_name, spec_list in fields.items():
        data[field_name] = _extract_with_specs(driver, spec_list)
    return Row(data=data)

def scrape_all(headless: bool,
               start_url: str,
               list_link_selectors: List[str],
               fields: Dict[str, List[Dict[str, Any]]],
               listing_ready_selectors: List[str],
               page_param: str,
               page_start: int,
               max_pages: int,
               page_delay: float,
               item_delay: float,
               limit: Optional[int]) -> List[Row]:
    driver = setup_driver(headless=headless)
    rows: List[Row] = []
    seen = set()
    try:
        # figure out base URL for relative links
        parts = urlparse(start_url)
        base_for_links = f"{parts.scheme}://{parts.netloc}"

        page_idx = page_start
        while page_idx < max_pages:
            list_url = _set_page(start_url, page_param, None if page_idx == 0 else page_idx)
            print(f"--- Listing page {page_idx} --- {list_url}")
            driver.get(list_url)
            wait_for(driver, listing_ready_selectors, extra=0.5)

            links = collect_links(driver, base_for_links, list_link_selectors)
            print(f"Found {len(links)} links.")
            if not links:
                break

            for link in links:
                if limit and len(rows) >= limit:
                    break
                if link in seen:
                    continue
                seen.add(link)
                try:
                    row = scrape_detail(driver, link, item_delay=item_delay, fields=fields)
                    rows.append(row)
                except Exception as e:
                    print(f"[WARN] Error scraping {link}: {e}", file=sys.stderr)

            if limit and len(rows) >= limit:
                break

            page_idx += 1
            time.sleep(page_delay)

    finally:
        try:
            driver.quit()
        except Exception:
            pass
    return rows

def save_to_excel(rows: List[Row], path: str, columns: List[str]) -> None:
    records = [{col: r.data.get(col, "") for col in columns} for r in rows]
    pd.DataFrame(records).to_excel(path, index=False)

def main():
    ap = argparse.ArgumentParser(description="Thin Selenium + Numeric Pager Scraper")
    ap.add_argument("--headless", action="store_true", help="Run headless Chrome")
    ap.add_argument("--out", default="results.xlsx", help="Output Excel file")
    ap.add_argument("--limit", type=int, default=None, help="Limit items (testing)")
    ap.add_argument("--page-delay", type=float, default=0.7, help="Delay after paging")
    ap.add_argument("--item-delay", type=float, default=0.4, help="Delay between item pages")
    args = ap.parse_args()

    print("Starting thin scraper...")
    rows = scrape_all(
        headless=args.headless,
        start_url=CONFIG["START_URL"],
        list_link_selectors=CONFIG["LIST_LINK_SELECTORS"],
        fields=CONFIG["FIELDS"],
        listing_ready_selectors=CONFIG["LISTING_READY_SELECTORS"],
        page_param=CONFIG["NUMERIC_PAGE_PARAM"],
        page_start=CONFIG["NUMERIC_PAGE_START"],
        max_pages=CONFIG["MAX_PAGES"],
        page_delay=args.page_delay,
        item_delay=args.item_delay,
        limit=args.limit,
    )
    save_to_excel(rows, args.out, CONFIG["OUTPUT_COLUMNS"])
    print(f"Done. Wrote {args.out} with {len(rows)} rows.")

if __name__ == "__main__":
    main()
