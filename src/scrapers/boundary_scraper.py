"""
Boundary scraper for Moldova administrative boundaries.
Downloads ADM1 (raion-level) GeoJSON from geoBoundaries.
"""
import json
import logging
import requests
from pathlib import Path

logger = logging.getLogger(__name__)

GEOBOUNDARIES_API = "https://www.geoboundaries.org/api/current/gbOpen/MDA/ADM1/"
WFS_UAT1_URL = "https://geodata.gov.md/geoserver/cadastru_data/wfs?service=WFS&version=1.1.0&request=GetFeature&typeName=UAT1&outputFormat=application/json"
HDX_GEOJSON_URL = (
    "https://data.humdata.org/dataset/3cd53554-3ad7-4aae-b862-9bbbc6fa3bfc/"
    "resource/04240ad4-6600-4d0d-b686-42cd6c8f624c/download/"
    "mda_admin_boundaries.geojson.zip"
)

RAW_DIR = Path(__file__).resolve().parents[2] / "data" / "raw" / "boundaries"
RAW_DIR.mkdir(parents=True, exist_ok=True)


def download_geoboundaries_adm1() -> dict | None:
    """Download ADM1 GeoJSON from geoBoundaries API."""
    try:
        logger.info("Fetching geoBoundaries API metadata...")
        r = requests.get(GEOBOUNDARIES_API, timeout=30)
        r.raise_for_status()
        meta = r.json()
        gj_url = meta.get("gjDownloadURL")
        if not gj_url:
            logger.error("No gjDownloadURL in API response")
            return None

        logger.info(f"Downloading GeoJSON from {gj_url}")
        r2 = requests.get(gj_url, timeout=60)
        r2.raise_for_status()
        geojson = r2.json()

        out = RAW_DIR / "adm1_geoboundaries.geojson"
        out.write_text(json.dumps(geojson, ensure_ascii=False), encoding="utf-8")
        logger.info(f"Saved {len(geojson.get('features', []))} features to {out}")
        return geojson
    except Exception as e:
        logger.error(f"geoBoundaries download failed: {e}")
        return None


def download_hdx_adm1() -> dict | None:
    """Download ADM1 GeoJSON from HDX (ZIP)."""
    import zipfile, io
    try:
        logger.info("Downloading HDX boundaries ZIP...")
        r = requests.get(HDX_GEOJSON_URL, timeout=60)
        r.raise_for_status()
        with zipfile.ZipFile(io.BytesIO(r.content)) as zf:
            for name in zf.namelist():
                if name.endswith(".geojson"):
                    data = json.loads(zf.read(name))
                    out = RAW_DIR / "adm1_hdx.geojson"
                    out.write_text(
                        json.dumps(data, ensure_ascii=False), encoding="utf-8"
                    )
                    logger.info(f"Saved HDX GeoJSON ({len(data.get('features', []))} features)")
                    return data
        logger.warning("No .geojson file found in HDX ZIP")
        return None
    except Exception as e:
        logger.error(f"HDX download failed: {e}")
        return None


def download_uat1() -> dict | None:
    """Download UAT1 (commune) boundaries from geodata.gov.md."""
    try:
        logger.info(f"Downloading UAT1 boundaries from {WFS_UAT1_URL}")
        r = requests.get(WFS_UAT1_URL, timeout=120)
        r.raise_for_status()
        geojson = r.json()
        out = RAW_DIR / "uat1_geodata.geojson"
        out.write_text(json.dumps(geojson, ensure_ascii=False), encoding="utf-8")
        logger.info(f"Saved {len(geojson.get('features', []))} UAT1 features to {out}")
        return geojson
    except Exception as e:
        logger.error(f"UAT1 download failed: {e}")
        return None


def scrape_boundaries() -> dict:
    """Download boundaries, including UAT1 and ADM1."""
    uat1 = download_uat1()
    adm1 = download_geoboundaries_adm1()
    if not adm1:
        logger.info("Falling back to HDX for ADM1...")
        adm1 = download_hdx_adm1()
    
    return {
        "uat1": uat1,
        "adm1": adm1
    }


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    result = scrape_boundaries()
    print(f"Downloaded {len(result.get('features', []))} boundary features")
