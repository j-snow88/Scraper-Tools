#!/usr/bin/env python3
# ICE Detention Facilities Scraper (NUMERIC_PAGER_v2 — uses ?page=N, tolerant waits)

import re
import sys
import time
import argparse
from dataclasses import dataclass
from typing import List, Optional
from urllib.parse import urlparse, urlunparse, parse_qs, urlencode

import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException

BANNER = "[ICE SCRAPER] NUMERIC_PAGER_v2 — using ?page=N (tolerant listing waits)"
START_URL = "https://www.ice.gov/detention-facilities"

STATE_RE = re.compile(
    r"\b(AL|AK|AZ|AR|CA|CO|CT|DE|FL|GA|HI|ID|IL|IN|IA|KS|KY|LA|ME|MD|MA|MI|MN|MS|MO|MT|NE|NV|NH|NJ|NM|NY|NC|ND|OH|OK|OR|PA|RI|SC|SD|TN|TX|UT|VT|VA|WA|WV|WI|WY|DC)\b"
)
PHONE_RE = re.compile(r"(?:\+?1[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}")
EMAIL_RE = re.compile(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}")

@dataclass
class FacilityRow:
    facility_name: str = ""
    field_office_name: str = ""
    facility_address: str = ""
    state: str = ""
    facility_phone: str = ""
    facility_email: str = ""
    field_office_phone: str = ""

# ---------- driver helpers

def setup_driver(headless: bool = True, timeout: int = 60) -> webdriver.Chrome:
    opts = Options()
    if headless:
        opts.add_argument("--headless=new")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--window-size=1400,900")
    opts.add_argument("--log-level=3")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    drv = webdriver.Chrome(options=opts)
    drv.set_page_load_timeout(timeout)
    return drv

def accept_cookies_if_present(driver):
    try:
        btn = WebDriverWait(driver, 5).until(
            EC.element_to_be_clickable((By.ID, "onetrust-accept-btn-handler"))
        )
        driver.execute_script("arguments[0].click();", btn)
        time.sleep(0.5)
        return
    except Exception:
        pass
    for txt in ["Accept", "I Agree", "Got it", "Aceptar", "OK"]:
        try:
            btn = driver.find_element(By.XPATH, f"//button[contains(., '{txt}')]")
            if btn.is_displayed():
                driver.execute_script("arguments[0].click();", btn)
                time.sleep(0.5)
                return
        except Exception:
            continue

def click_apply(driver) -> None:
    # Apply helps force the listing to render; if it fails we still continue.
    accept_cookies_if_present(driver)
    candidates = [
        (By.XPATH, "//button[contains(., 'Apply')]"),
        (By.CSS_SELECTOR, "#edit-submit-detention-facilities"),
        (By.XPATH, "//input[@type='submit' and contains(@value, 'Apply')]"),
    ]
    for how, sel in candidates:
        try:
            btn = WebDriverWait(driver, 8).until(EC.element_to_be_clickable((how, sel)))
            driver.execute_script("arguments[0].click();", btn)
            break
        except TimeoutException:
            continue
    # Best-effort wait; don’t fail if not found
    try:
        WebDriverWait(driver, 15).until(
            EC.any_of(
                EC.presence_of_element_located((By.CSS_SELECTOR, ".view-content")),
                EC.presence_of_element_located((By.CSS_SELECTOR, "a[href*='/detention-facility/']"))
            )
        )
    except TimeoutException:
        print("WARNING: Results container not detected after Apply (continuing).", file=sys.stderr)
    time.sleep(0.6)

# ---------- link collection

def collect_facility_links_on_current_page(driver) -> List[str]:
    links = set()
    # Try within typical containers; if none, search whole page
    containers = driver.find_elements(By.CSS_SELECTOR, ".view-detention-facilities .view-content, .view-content")
    if not containers:
        containers = [driver.find_element(By.TAG_NAME, "body")]
    patterns = [
        "a[href*='/detention-facility/']",
        "a[href*='/detention-']",
        "a[href*='/detain']",
    ]
    for c in containers:
        for css in patterns:
            for a in c.find_elements(By.CSS_SELECTOR, css):
                href = (a.get_attribute("href") or "").split("?")[0]
                if href and "/detention" in href:
                    links.add(href)
    return sorted(links)

# ---------- numeric pagination helpers

def _set_or_remove_page(url: str, page_num: Optional[int]) -> str:
    """Return url with ?page=page_num (0-index). If page_num is None, remove page param."""
    parts = urlparse(url)
    q = parse_qs(parts.query)
    if page_num is None:
        q.pop("page", None)
    else:
        q["page"] = [str(page_num)]
    new_query = urlencode(q, doseq=True)
    return urlunparse((parts.scheme, parts.netloc, parts.path, parts.params, new_query, parts.fragment))

# ---------- field extraction

def _text(el) -> str:
    try:
        return el.text.strip()
    except Exception:
        return ""

def _first_state(text: str) -> str:
    m = STATE_RE.search(text or "")
    return m.group(0) if m else ""

def _first_phone_near_label(full_text: str, labels: List[str]) -> str:
    lower = (full_text or "").lower()
    for lbl in labels:
        idx = lower.find(lbl.lower())
        if idx != -1:
            window = full_text[idx: idx + 400]
            m = PHONE_RE.search(window)
            if m:
                return m.group(0)
    m = PHONE_RE.search(full_text or "")
    return m.group(0) if m else ""

def extract_facility_page(driver, url: str) -> FacilityRow:
    driver.get(url)
    WebDriverWait(driver, 30).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
    time.sleep(0.6)

    body_text = driver.find_element(By.TAG_NAME, "body").text  # <-- fixed TAG_NAME

    try:
        facility_name = _text(driver.find_element(By.CSS_SELECTOR, "h1"))
    except NoSuchElementException:
        facility_name = ""

    field_office_name = ""
    for el in driver.find_elements(By.XPATH, "//*[contains(text(), 'Field Office')]"):
        t = _text(el)
        if not t:
            continue
        m = re.search(r"([\w\-/&.,' ]+)\s+Field Office", t)
        if m:
            field_office_name = m.group(1).strip()
            break
        if t.strip().lower().endswith("field office"):
            field_office_name = t.replace("Field Office", "").strip()
            break

    facility_address = ""
    state = ""
    blocks = driver.find_elements(By.XPATH, "//p|//li|//div")
    for b in blocks:
        txt = _text(b)
        if not txt:
            continue
        st = _first_state(txt)
        if st and len(txt) < 220 and re.search(r"\b\d{5}(?:-\d{4})?\b", txt):
            facility_address = txt.replace("\n", " ")
            state = st
            break
    if not facility_address:
        m = re.search(r"([^\n]{10,220}\b[A-Z]{2}\b\s*\d{5}(?:-\d{4})?)", body_text or "")
        if m:
            facility_address = m.group(1).replace("\n", " ").strip()
            state = _first_state(facility_address)

    facility_phone = _first_phone_near_label(body_text, ["Facility Main Phone", "Facility Phone"])
    field_office_phone = _first_phone_near_label(body_text, ["Field Office Main Phone", "Field Office Phone"])

    emails = EMAIL_RE.findall(body_text or "")
    facility_email = ""
    if emails:
        govt = [e for e in emails if e.lower().endswith("@ice.dhs.gov")]
        facility_email = govt[0] if govt else emails[0]

    return FacilityRow(
        facility_name=facility_name,
        field_office_name=field_office_name,
        facility_address=facility_address,
        state=state,
        facility_phone=facility_phone,
        facility_email=facility_email,
        field_office_phone=field_office_phone,
    )

# ---------- main loop (numeric pagination)

def scrape_all(headless: bool = True, page_delay: float = 0.8, per_facility_delay: float = 0.5,
               limit: Optional[int] = None, max_pages: int = 200) -> List[FacilityRow]:
    drv = setup_driver(headless=headless)
    rows: List[FacilityRow] = []
    seen = set()
    try:
        print(BANNER)
        drv.get(START_URL)
        WebDriverWait(drv, 30).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        click_apply(drv)

        # Base listing URL without ?page=
        base = _set_or_remove_page(drv.current_url, None)

        page_idx = 0
        while page_idx < max_pages:
            list_url = _set_or_remove_page(base, None if page_idx == 0 else page_idx)
            print(f"--- Listing page {page_idx} --- {list_url}")
            drv.get(list_url)

            # Tolerant wait: either content container or any facility link
            try:
                WebDriverWait(drv, 25).until(
                    EC.any_of(
                        EC.presence_of_element_located((By.CSS_SELECTOR, ".view-content")),
                        EC.presence_of_element_located((By.CSS_SELECTOR, "a[href*='/detention-facility/']"))
                    )
                )
            except TimeoutException:
                print("[WARN] View content not detected; attempting to collect anyway.", file=sys.stderr)

            links = collect_facility_links_on_current_page(drv)
            print(f"Found {len(links)} facility links on this page.")
            if len(links) == 0:
                break  # no more pages

            for link in links:
                if limit and len(rows) >= limit:
                    break
                if link in seen:
                    continue
                seen.add(link)
                try:
                    row = extract_facility_page(drv, link)
                    rows.append(row)
                    time.sleep(per_facility_delay)
                except Exception as e:
                    print(f"[WARN] Error scraping {link}: {e}", file=sys.stderr)

            if limit and len(rows) >= limit:
                break
            page_idx += 1
            time.sleep(page_delay)

    finally:
        try:
            drv.quit()
        except Exception:
            pass
    return rows

def save_to_excel(rows: List[FacilityRow], out_path: str) -> None:
    df = pd.DataFrame([{
        "Facility Name": r.facility_name,
        "Field Office Name": r.field_office_name,
        "Facility Address": r.facility_address,
        "State": r.state,
        "Facility Phone Number": r.facility_phone,
        "Facility Email": r.facility_email,
        "Field Office Phone Number": r.field_office_phone,
    } for r in rows])
    df.to_excel(out_path, index=False)

def main():
    ap = argparse.ArgumentParser(description="ICE Detention Facilities scraper (numeric pagination, tolerant waits)")
    ap.add_argument("--headless", action="store_true", help="Run Chrome in headless mode (recommended)")
    ap.add_argument("--out", default="ice_detention_facilities.xlsx", help="Output Excel file path")
    ap.add_argument("--limit", type=int, default=None, help="Limit number of facilities (for testing)")
    ap.add_argument("--page-delay", type=float, default=0.8, help="Delay after paging")
    ap.add_argument("--item-delay", type=float, default=0.5, help="Delay between facility pages")
    args = ap.parse_args()

    print("Starting ICE detention facilities scrape...")
    rows = scrape_all(
        headless=args.headless,
        page_delay=args.page_delay,
        per_facility_delay=args.item_delay,
        limit=args.limit
    )
    save_to_excel(rows, args.out)
    print(f"Done. Wrote {args.out} with {len(rows)} rows.")

if __name__ == "__main__":
    main()
