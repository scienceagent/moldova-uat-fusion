"""
Population scraper for Moldova UATs.
Tries BNS statistica.gov.md, falls back to embedded data.
"""
import json
import logging
import requests
from pathlib import Path

logger = logging.getLogger(__name__)

RAW_DIR = Path(__file__).resolve().parents[2] / "data" / "raw" / "population"
RAW_DIR.mkdir(parents=True, exist_ok=True)
FALLBACK_DIR = Path(__file__).resolve().parents[2] / "data" / "fallback"


def try_bns_download() -> dict | None:
    """Attempt to download population data from BNS."""
    urls_to_try = [
        "https://statistica.gov.md/ro/populatia-si-procesele-demografice-53.html",
        "https://statbank.statistica.md/PxWeb/api/v1/ro/30%20Statistica%20sociala/"
        "30.01%20Populatia%20si%20procesele%20demografice/"
        "POP010100.px",
    ]
    for url in urls_to_try:
        try:
            r = requests.get(url, timeout=15,
                             headers={"User-Agent": "Mozilla/5.0"})
            if r.status_code == 200:
                logger.info(f"BNS responded from {url}")
                # Would need specific parsing per page structure
                # For now, log and fall through to fallback
        except Exception as e:
            logger.debug(f"BNS URL failed ({url}): {e}")
    return None


def scrape_population() -> dict:
    """Get population data, trying live sources first, then fallback."""
    live = try_bns_download()
    if live:
        out = RAW_DIR / "population_bns.json"
        out.write_text(json.dumps(live, ensure_ascii=False, indent=2),
                       encoding="utf-8")
        return live

    logger.info("Using fallback population data (RPL 2024 estimates)")
    fb = FALLBACK_DIR / "population_fallback.json"
    if fb.exists():
        data = json.loads(fb.read_text(encoding="utf-8"))
        # Copy to raw dir too
        out = RAW_DIR / "population_data.json"
        out.write_text(json.dumps(data, ensure_ascii=False, indent=2),
                       encoding="utf-8")
        return data

    logger.error("No fallback population data found!")
    return {}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    data = scrape_population()
    print(f"Population data: {len(data)} UATs")
