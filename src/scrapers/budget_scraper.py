"""
Budget scraper for Moldova UAT budget execution data.
Uses CKAN API on dataset.gov.md to download XLSX reports from Ministry of Finance.
"""
import json
import logging
import re
import requests
from pathlib import Path
from datetime import datetime

logger = logging.getLogger(__name__)

CKAN_API = "https://dataset.gov.md/api/3/action"
DATASET_ID = "15969-date-privind-executarea-bugetelor-autoritatilor-publice-locale"

RAW_DIR = Path(__file__).resolve().parents[2] / "data" / "raw" / "budget"
RAW_DIR.mkdir(parents=True, exist_ok=True)


def get_latest_resource_url() -> tuple[str, str] | None:
    """Query CKAN API to find the latest XLSX resource URL."""
    try:
        url = f"{CKAN_API}/package_show?id={DATASET_ID}"
        logger.info(f"Querying CKAN: {url}")
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        data = r.json()
        if not data.get("success"):
            logger.error("CKAN API returned success=false")
            return None

        resources = data["result"].get("resources", [])
        # Filter XLSX resources, sort by position (latest = highest position)
        xlsx_res = [
            r for r in resources
            if r.get("format", "").upper() == "XLSX" or r.get("url", "").endswith(".xlsx")
        ]
        if not xlsx_res:
            logger.error("No XLSX resources found")
            return None

        # Get the one with highest position (most recent)
        latest = max(xlsx_res, key=lambda x: x.get("position", 0))
        return latest["url"], latest.get("name", "unknown")
    except Exception as e:
        logger.error(f"CKAN API query failed: {e}")
        return None


def download_xlsx(url: str, name: str) -> Path | None:
    """Download an XLSX file from the given URL."""
    try:
        safe_name = re.sub(r'[^\w\-.]', '_', name)[:80] + ".xlsx"
        out_path = RAW_DIR / safe_name
        if out_path.exists():
            logger.info(f"Using cached XLSX: {out_path}")
            return out_path

        logger.info(f"Downloading XLSX: {url}")
        r = requests.get(url, timeout=120)
        r.raise_for_status()
        out_path.write_bytes(r.content)
        logger.info(f"Saved {len(r.content)} bytes to {out_path}")
        return out_path
    except Exception as e:
        logger.error(f"XLSX download failed: {e}")
        return None


def parse_budget_xlsx(xlsx_path: Path) -> list[dict]:
    """Parse a Ministry of Finance budget XLSX into structured data."""
    import pandas as pd

    logger.info(f"Parsing XLSX: {xlsx_path}")
    results = []

    try:
        xls = pd.ExcelFile(xlsx_path, engine="openpyxl")
        sheet_names = xls.sheet_names
        logger.info(f"Found sheets: {sheet_names}")

        for sheet_name in sheet_names:
            try:
                df = pd.read_excel(xlsx_path, sheet_name=sheet_name,
                                   header=None, engine="openpyxl")
                parsed = _extract_uat_data_from_sheet(df, sheet_name)
                results.extend(parsed)
            except Exception as e:
                logger.warning(f"Failed to parse sheet '{sheet_name}': {e}")
                continue

    except Exception as e:
        logger.error(f"Failed to open XLSX: {e}")

    logger.info(f"Parsed {len(results)} UAT budget records total")
    return results


def _extract_uat_data_from_sheet(df, sheet_name: str) -> list[dict]:
    """Extract UAT budget data from a single sheet DataFrame."""
    import pandas as pd
    records = []

    # Try to find header row with UAT names
    # Common patterns: rows contain raion/UAT names in column A/B
    # and numeric budget values in subsequent columns
    for start_row in range(min(15, len(df))):
        row_vals = [str(v).strip().lower() for v in df.iloc[start_row].values if pd.notna(v)]
        row_text = " ".join(row_vals)
        if any(kw in row_text for kw in ["total", "venituri", "cheltuieli", "denumire"]):
            break
    else:
        start_row = 0

    # Look for UAT name column and numeric columns
    for idx in range(start_row + 1, len(df)):
        row = df.iloc[idx]
        name = None
        values = []

        for col_idx, val in enumerate(row):
            if pd.isna(val):
                continue
            if isinstance(val, str) and len(val) > 2 and not val.replace(" ", "").isnumeric():
                if name is None:
                    name = val.strip()
            elif isinstance(val, (int, float)):
                values.append(val)

        if name and len(values) >= 2:
            # Skip header-like rows
            name_lower = name.lower()
            if any(skip in name_lower for skip in [
                "total", "din care", "inclusiv", "anexa", "raport",
                "pag.", "pagina", "cod"
            ]):
                continue

            record = {
                "name": name,
                "sheet": sheet_name,
                "values": values[:10],  # Cap at 10 numeric columns
            }
            records.append(record)

    return records


def build_budget_json(records: list[dict]) -> dict:
    """Build structured budget JSON from parsed records."""
    budget_data = {}
    for rec in records:
        name = rec["name"]
        vals = rec["values"]
        budget_data[name] = {
            "total_revenues": vals[0] if len(vals) > 0 else 0,
            "total_expenditures": vals[1] if len(vals) > 1 else 0,
            "own_revenues": vals[2] if len(vals) > 2 else 0,
            "personnel_costs": vals[3] if len(vals) > 3 else 0,
            "functioning_costs": vals[4] if len(vals) > 4 else 0,
            "source_sheet": rec["sheet"],
        }
    return budget_data


def scrape_budget() -> dict:
    """Main entry: download latest XLSX and parse budget data."""
    result = get_latest_resource_url()
    if not result:
        logger.warning("Could not get CKAN resource URL")
        return _load_fallback()

    url, name = result
    xlsx_path = download_xlsx(url, name)
    if not xlsx_path:
        return _load_fallback()

    records = parse_budget_xlsx(xlsx_path)
    if not records:
        logger.warning("No records parsed from XLSX, using fallback")
        return _load_fallback()

    budget = build_budget_json(records)

    out = RAW_DIR / "budget_parsed.json"
    out.write_text(
        json.dumps(budget, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    logger.info(f"Saved budget data for {len(budget)} UATs to {out}")
    return budget


def _load_fallback() -> dict:
    """Load fallback budget data."""
    fallback_path = Path(__file__).resolve().parents[2] / "data" / "fallback" / "budget_fallback.json"
    if fallback_path.exists():
        return json.loads(fallback_path.read_text(encoding="utf-8"))
    logger.warning("No fallback budget data available")
    return {}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    data = scrape_budget()
    print(f"Budget data for {len(data)} UATs")
