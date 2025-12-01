
"""
Anayat U. 
Zameen.com Scraper - Python-Based 

Usage:

python zameen_scraper.py \
  --search-url "https://www.zameen.com/Homes/Islamabad-3-1.html" \  change url for other cities 
  --max-pages 1 --max-details 5 \
  --out islamabad_dataset.csv 

"""

from __future__ import annotations

import argparse
import csv
import json
import random
import re
import sys
import time
from dataclasses import dataclass, asdict
from typing import Iterable, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------
def clean_text(s: str) -> str:
    if s is None:
        return ""
    return re.sub(r"\s+", " ", str(s)).strip()

try:
    import lxml  # noqa: F401
    _BS_PARSER = "lxml"
except Exception:
    _BS_PARSER = "html.parser"

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

LISTING_URL_REGEX = re.compile(r"^https?://(www\.)?zameen\.com/Property/|^/Property/")
PRICE_TEXT_REGEX = re.compile(
    r"(?:PKR|Rs\.?)[\s\xa0]*([\d,.]+)\s*(?:Crore|Lakh|Million|Thousand|K|M|B)?",
    re.I,
)

# ---------------------------------------------------------------------
@dataclass
class Listing:
    url: str
    title: Optional[str] = None
    price: Optional[str] = None
    price_numeric: Optional[float] = None
    currency: Optional[str] = None
    location: Optional[str] = None
    bedrooms: Optional[int] = None
    bathrooms: Optional[int] = None
    area: Optional[str] = None
    property_type: Optional[str] = None
    description: Optional[str] = None
    built_in_year: Optional[str] = None
    parking_space: Optional[str] = None
    servant_quarters: Optional[str] = None
    store_rooms: Optional[str] = None
    kitchens: Optional[str] = None
    drawing_room: Optional[str] = None
    floors: Optional[str] = None
    dinning_room: Optional[str] = None
    study_room: Optional[str] = None
    laundry_room: Optional[str] = None
    lounge_or_sitting_room: Optional[str] = None
    powder_room: Optional[str] = None
    prayer_room: Optional[str] = None
    city: Optional[str] = None  # <-- new field added

# ---------------------------------------------------------------------
def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(DEFAULT_HEADERS)
    s.request = _with_timeout(s.request, timeout=20)
    return s

def _with_timeout(func, timeout=20):
    def wrapper(method, url, **kwargs):
        if "timeout" not in kwargs:
            kwargs["timeout"] = timeout
        return func(method, url, **kwargs)
    return wrapper

def get_soup(session: requests.Session, url: str) -> BeautifulSoup:
    r = session.get(url, allow_redirects=True)
    r.raise_for_status()
    if r.status_code == 403 or ("captcha" in r.text.lower() and "cloudflare" in r.text.lower()):
        raise RuntimeError("Access blocked. Try fewer pages or add delay/VPN.")
    return BeautifulSoup(r.text, _BS_PARSER)

def normalize_url(href: str, base: Optional[str] = None) -> str:
    if href.startswith("http"):
        return href
    if href.startswith("/"):
        return "https://www.zameen.com" + href
    if base:
        return base.rsplit("/", 1)[0] + "/" + href
    return "https://www.zameen.com/" + href.lstrip("/")

def discover_listing_urls(soup: BeautifulSoup) -> List[str]:
    urls, seen = [], set()
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if LISTING_URL_REGEX.search(href):
            abs_url = normalize_url(href)
            if abs_url not in seen:
                urls.append(abs_url)
                seen.add(abs_url)
    return urls

def find_next_page(soup: BeautifulSoup, current_url: str) -> Optional[str]:
    link = soup.find("link", rel=lambda v: v and "next" in v)
    if link and link.get("href"):
        return normalize_url(link["href"], current_url)
    for a in soup.find_all("a", href=True):
        if "next" in (a.get("aria-label") or a.get_text(" ")).lower():
            return normalize_url(a["href"], current_url)
    m = re.search(r"-(\d+)\.html$", current_url)
    if m:
        n = int(m.group(1))
        return re.sub(r"-(\d+)\.html$", f"-{n+1}.html", current_url)
    return None

# ---------------------------------------------------------------------
def extract_city_from_search_url(search_url: str) -> Optional[str]:
    """
    Extract city name from a search URL like:
    https://www.zameen.com/Houses_Property/Islamabad-3-1.html "islamabad"
    """
    if not search_url:
        return None
    try:
        path = re.sub(r"[?#].*$", "", search_url)
        last = path.rstrip("/").split("/")[-1]
        last = re.sub(r"\.html?$", "", last, flags=re.I)
        city = last.split("-", 1)[0].strip()
        if city:
            return city.lower()
    except Exception:
        pass
    return None

# ---------------------------------------------------------------------
def parse_listing_detail(session: requests.Session, url: str) -> Listing:
    soup = get_soup(session, url)
    listing = Listing(url=url)

    # --- Title ---
    title_tag = soup.select_one("div.c121f914 h1.aea614fd")
    if title_tag:
        listing.title = clean_text(title_tag.get_text(" ", strip=True))

    # --- Location ---
    loc_tag = soup.select_one("div.c121f914 div.cd230541")
    if loc_tag:
        listing.location = clean_text(loc_tag.get_text(" ", strip=True))

    # --- DETAILS BLOCK (label/value pairs) ---
    details_block = soup.select_one("div._83bb17d1 ul._3dc8d08d")
    if details_block:
        for li in details_block.find_all("li"):
            label_el = li.find("span", class_="ed0db22a")
            # value may be in a span with class _2fdf7fc5, or nested div
            value_el = li.find("span", class_="_2fdf7fc5") or li.find("span", class_="_2fdf7fc5".replace(".", ""))
            label = clean_text(label_el.get_text(" ", strip=True)) if label_el else ""
            value = clean_text(value_el.get_text(" ", strip=True)) if value_el else clean_text(li.get_text(" ", strip=True))

            low = label.lower()
            if "price" in low:
                listing.price = value
                pnum, curr = _parse_price(value)
                listing.price_numeric = pnum
                listing.currency = curr
            elif "area" in low:
                listing.area = value
            elif "type" in low:
                listing.property_type = value
            elif "bed" in low:  # matches "Bedroom(s)" label
                m = re.search(r"(\d+)", value)
                if m:
                    try:
                        listing.bedrooms = int(m.group(1))
                    except Exception:
                        listing.bedrooms = None
            elif "bath" in low:  # matches "Bath(s)" label
                m = re.search(r"(\d+)", value)
                if m:
                    try:
                        listing.bathrooms = int(m.group(1))
                    except Exception:
                        listing.bathrooms = None
            else:
                # aria-label fallback on value span
                if value_el and value_el.has_attr("aria-label"):
                    alabel = value_el["aria-label"].strip().lower()
                    if "price" in alabel and not listing.price:
                        listing.price = value
                        pnum, curr = _parse_price(value)
                        listing.price_numeric = pnum
                        listing.currency = curr
                    elif "area" in alabel and not listing.area:
                        listing.area = value
                    elif ("bed" in alabel or "beds" in alabel) and listing.bedrooms is None:
                        m = re.search(r"(\d+)", value)
                        if m:
                            listing.bedrooms = int(m.group(1))
                    elif ("bath" in alabel or "baths" in alabel) and listing.bathrooms is None:
                        m = re.search(r"(\d+)", value)
                        if m:
                            listing.bathrooms = int(m.group(1))
                    elif "type" in alabel and not listing.property_type:
                        listing.property_type = value

    #  Price fallback (if not found inside details block) 
    if not listing.price:
        price_tag = soup.select_one("span._105b8a67, span._2923a568, div._2923a568, span._2fdf7fc5[aria-label='Price']")
        if price_tag:
            ptxt = clean_text(price_tag.get_text(" ", strip=True))
            listing.price = ptxt
            pnum, curr = _parse_price(ptxt)
            listing.price_numeric = pnum
            listing.currency = curr

    #  AMENITIES SECTION (search for Amenities header inside same container) 
    amenities_section = None
    for sec in soup.find_all("div", class_="_83bb17d1"):
        h3 = sec.find("h3")
        if h3 and "amenit" in h3.get_text(" ", strip=True).lower():
            amenities_section = sec
            break

    if amenities_section:
        for li in amenities_section.find_all("li"):
            txt = clean_text(li.get_text(" ", strip=True))
            low_txt = txt.lower()
            # Built year
            m = re.search(r"(\b(19|20)\d{2}\b)", txt)
            if m and not listing.built_in_year:
                listing.built_in_year = m.group(1)
            # Parking
            if "park" in low_txt or "parking" in low_txt:
                m = re.search(r"(\d+)", txt)
                listing.parking_space = m.group(1) if m else "Yes"
            # Servant quarters
            if "servant" in low_txt:
                m = re.search(r"(\d+)", txt)
                listing.servant_quarters = m.group(1) if m else "Yes"
            # Store rooms
            if "store" in low_txt:
                m = re.search(r"(\d+)", txt)
                listing.store_rooms = m.group(1) if m else "Yes"
            # Kitchens
            if "kitchen" in low_txt:
                m = re.search(r"(\d+)", txt)
                listing.kitchens = m.group(1) if m else "Yes"
            # Drawing room
            if "drawing" in low_txt:
                listing.drawing_room = "Yes"
            # Dining room
            if "dining" in low_txt:
                listing.dinning_room = "Yes"
            # Study room
            if "study" in low_txt:
                listing.study_room = "Yes"
            # Prayer room
            if "prayer" in low_txt or "masjid" in low_txt:
                listing.prayer_room = "Yes"
            # Powder room
            if "powder" in low_txt:
                listing.powder_room = "Yes"
            # Lounge / Sitting
            if "lounge" in low_txt or "sitting" in low_txt or "living" in low_txt:
                listing.lounge_or_sitting_room = "Yes"

    # description (common selectors) 
    desc_tag = soup.select_one("div._3e9c24cd, div._2a806e1f, section._3e9c24cd, div._2d2b3f3a")
    if desc_tag:
        listing.description = clean_text(desc_tag.get_text(" ", strip=True))

    return listing

# ---------------------------------------------------------------------
def _parse_price(text: str) -> Tuple[Optional[float], Optional[str]]:
    """
    Parse human price strings like:
      "PKR 4.8 Crore", "PKR 4,800,000", "Rs. 12 Lakh", "10 Crore", "PKR 1.2M"
    Returns (numeric_value_in_PKR (float) or None, currency_string or None)
    """
    if not text:
        return None, None

    txt = str(text).replace("\xa0", " ").strip()

    # detect currency presence (PKR / Rs etc.)
    currency = None
    if re.search(r"\b(pkrs?|rs\.?)\b", txt, re.I):
        currency = "PKR"

    # find first number + optional suffix (crore/lakh/million/thousand/k/m/b)
    m = re.search(r"([\d,.]+)\s*(crore|lakh|million|thousand|k\b|m\b|b\b)?", txt, re.I)
    if not m:
        # fallback try to find any plain integer (no suffix)
        m2 = re.search(r"([\d,]+)", txt)
        if not m2:
            return None, currency
        num_str = m2.group(1).replace(",", "")
        try:
            return float(num_str), currency
        except Exception:
            return None, currency

    num_str = m.group(1).replace(",", "")
    try:
        base = float(num_str)
    except Exception:
        return None, currency

    suffix = m.group(2).lower() if m.group(2) else None
    multipliers = {
        "crore": 1e7,    # 1 Crore = 10,000,000
        "lakh": 1e5,     # 1 Lakh = 100,000
        "million": 1e6,
        "thousand": 1e3,
        "k": 1e3,
        "m": 1e6,
        "b": 1e9,
    }

    if suffix and suffix in multipliers:
        value = base * multipliers[suffix]
    else:
        # no suffix - assume the number is already in PKR (e.g., 4,800,000)
        value = base

    return float(value), currency

# ---------------------------------------------------------------------
def _sleep(delay: float, jitter: float):
    time.sleep(max(0.0, delay + random.uniform(-jitter, jitter)))

def scrape(search_url: str, max_pages: int, delay: float, jitter: float,
           session: requests.Session, max_details: Optional[int] = None) -> List[Listing]:
    # extract city once from the search URL
    city = extract_city_from_search_url(search_url)

    all_urls, page_url = [], search_url
    for i in range(max_pages):
        print(f"[page {i+1}] GET {page_url}")
        soup = get_soup(session, page_url)
        urls = discover_listing_urls(soup)
        print(f"  found {len(urls)} candidate detail links")
        all_urls.extend(urls)
        next_url = find_next_page(soup, page_url)
        if not next_url:
            break
        _sleep(delay, jitter)
        page_url = next_url

    listings: List[Listing] = []
    detail_pool = all_urls[:max_details] if max_details else all_urls
    for idx, url in enumerate(detail_pool, 1):
        print(f"[detail {idx}/{len(detail_pool)}] {url}")
        try:
            listing = parse_listing_detail(session, url)
            listing.city = city
            listings.append(listing)
        except Exception as e:
            print(f"  ! error: {e}")
        _sleep(delay, jitter)

    return listings

# ---------------------------------------------------------------------
def write_csv(rows: Iterable[Listing], path: str) -> None:
    fieldnames = [
        "title", "price", "location", "City", "property type", "bedrooms",
        "bathrooms", "area", "built in year", "parking space",
        "servant quarters", "store rooms", "kitchens", "drawing room",
        "floors", "dinning room", "study room", "laundry room",
        "lounge or sitting room", "powder room", "prayer room"
    ]
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            row = asdict(r)
            out = {
                "title": row.get("title"),
                "price": row.get("price_numeric"),
                "location": row.get("location"),
                "City": row.get("city"),
                "property type": row.get("property_type"),
                "bedrooms": row.get("bedrooms"),
                "bathrooms": row.get("bathrooms"),
                "area": row.get("area"),
                "built in year": row.get("built_in_year"),
                "parking space": row.get("parking_space"),
                "servant quarters": row.get("servant_quarters"),
                "store rooms": row.get("store_rooms"),
                "kitchens": row.get("kitchens"),
                "drawing room": row.get("drawing_room"),
                "floors": row.get("floors"),
                "dinning room": row.get("dinning_room"),
                "study room": row.get("study_room"),
                "laundry room": row.get("laundry_room"),
                "lounge or sitting room": row.get("lounge_or_sitting_room"),
                "powder room": row.get("powder_room"),
                "prayer room": row.get("prayer_room"),
            }
            w.writerow(out)

# ---------------------------------------------------------------------
def main(argv: Optional[List[str]] = None) -> int:
    p = argparse.ArgumentParser(description="Scraper for Zameen.com (fixed selectors)")
    p.add_argument("--search-url", required=True)
    p.add_argument("--max-pages", type=int, default=1)
    p.add_argument("--delay", type=float, default=1.5)
    p.add_argument("--jitter", type=float, default=0.5)
    p.add_argument("--out", default="zameen_listings.csv")
    p.add_argument("--max-details", type=int, default=10)
    args = p.parse_args(argv)

    session = make_session()
    try:
        listings = scrape(
            search_url=args.search_url,
            max_pages=args.max_pages,
            delay=args.delay,
            jitter=args.jitter,
            session=session,
            max_details=args.max_details,
        )
    except Exception as e:
        print(f"fatal: {e}")
        return 2

    write_csv(listings, args.out)
    print(f"\nSaved {len(listings)} listings → {args.out}")
    return 0

if __name__ == "__main__":
    sys.exit(main())
