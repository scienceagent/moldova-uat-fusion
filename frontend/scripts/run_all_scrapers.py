#!/usr/bin/env python3
"""
Script principal pentru extragerea tuturor datelor necesare.
Rulează scrapere-ii în ordinea corectă și agreghează rezultatele.
"""

import sys
import json
import time
from pathlib import Path
from datetime import datetime
import logging

# Adaugă src la PYTHONPATH
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from scrapers.population_scraper import PopulationScraper
from scrapers.boundaries_scraper import BoundariesScraper
from scrapers.budget_scraper import BudgetScraper

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def setup_directories():
    """Creează structura de directoare necesară."""
    dirs = [
        Path("data/raw/population"),
        Path("data/raw/budgets"),
        Path("data/raw/boundaries"),
        Path("data/processed"),
        Path("data/cache"),
        Path("logs")
    ]
    for d in dirs:
        d.mkdir(parents=True, exist_ok=True)
        logger.info(f"Created directory: {d}")

def run_scrapers():
    """Rulează toți scrapere-ii și returnează datele extrase."""
    
    logger.info("=" * 60)
    logger.info("STARTING DATA EXTRACTION PROCESS")
    logger.info("=" * 60)
    
    results = {
        'timestamp': datetime.now().isoformat(),
        'population': {},
        'boundaries': {},
        'budgets': {}
    }
    
    # 1. Extrage date populație
    logger.info("\n[1/3] Extracting population data...")
    try:
        pop_scraper = PopulationScraper()
        results['population'] = pop_scraper.fetch_population_data()
        logger.info(f"✓ Population data: {len(results['population'].get('uat_population', {}))} UATs")
    except Exception as e:
        logger.error(f"✗ Population extraction failed: {e}")
    
    # Pauză pentru a nu suprasolicita serverele
    time.sleep(2)
    
    # 2. Extrage date geospațiale
    logger.info("\n[2/3] Extracting boundaries data...")
    try:
        bounds_scraper = BoundariesScraper()
        results['boundaries'] = bounds_scraper.fetch_all_boundaries()
        logger.info(f"✓ Boundaries data: {len(results['boundaries'].get('features', []))} features")
    except Exception as e:
        logger.error(f"✗ Boundaries extraction failed: {e}")
    
    time.sleep(2)
    
    # 3. Extrage date bugetare
    logger.info("\n[3/3] Extracting budget data...")
    try:
        budget_scraper = BudgetScraper()
        results['budgets'] = budget_scraper.fetch_all_budgets()
        logger.info("✓ Budget data extraction completed")
    except Exception as e:
        logger.error(f"✗ Budget extraction failed: {e}")
    
    # Salvează rezumatul
    summary_path = Path("data/processed/extraction_summary.json")
    with open(summary_path, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    
    logger.info("\n" + "=" * 60)
    logger.info(f"EXTRACTION COMPLETE! Summary saved to {summary_path}")
    logger.info("=" * 60)
    
    return results

def validate_data(results: dict) -> bool:
    """Verifică dacă datele extrase sunt suficiente."""
    errors = []
    
    if not results['population'].get('uat_population'):
        errors.append("No population data extracted")
    
    if not results['boundaries'].get('features'):
        errors.append("No boundaries data extracted")
    
    if errors:
        logger.error("Data validation failed:")
        for err in errors:
            logger.error(f"  - {err}")
        return False
    
    logger.info("✓ All data validation passed")
    return True

if __name__ == "__main__":
    setup_directories()
    data = run_scrapers()
    validate_data(data)