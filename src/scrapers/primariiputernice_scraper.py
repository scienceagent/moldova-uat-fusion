from bs4 import BeautifulSoup
import requests
from urllib.parse import urljoin
from datetime import datetime

BASE_URL = "https://primariiputernice.gov.md"

def scrape_pages():
    r = requests.get(BASE_URL, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "lxml")

    pages = []
    seen = set()

    for a in soup.select("a[href]"):
        href = a.get("href", "").strip()
        if not href:
            continue
        url = urljoin(BASE_URL, href)
        if url in seen:
            continue
        seen.add(url)
        pages.append({
            "title": a.get_text(" ", strip=True),
            "url": url
        })

    return {
        "source": BASE_URL,
        "fetched_at": datetime.utcnow().isoformat() + "Z",
        "count": len(pages),
        "pages": pages
    }