"""
Extrage date despre procesul de amalgamare voluntară.
Sursă: amalgamare.gov.md
"""

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
import json
import time
from pathlib import Path
from typing import Dict, List
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AmalgamationStatusScraper:
    """
    Extrage informații despre UAT-urile aflate în proces de amalgamare.
    """
    
    BASE_URL = "https://amalgamare.gov.md"
    
    def __init__(self, output_dir: Path = Path("data/raw/amalgamation")):
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.driver = None
    
    def setup_driver(self):
        """Configurează driver-ul Selenium."""
        chrome_options = Options()
        chrome_options.add_argument("--headless")  # Rulează în fundal
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        
        self.driver = webdriver.Chrome(options=chrome_options)
        self.driver.implicitly_wait(10)
    
    def get_uat_list(self) -> List[Dict]:
        """
        Extrage lista UAT-urilor din harta interactivă.
        """
        if not self.driver:
            self.setup_driver()
        
        uat_data = []
        
        try:
            # Încarcă pagina cu harta UAT
            url = f"{self.BASE_URL}/ro/interactive-map/uat"
            self.driver.get(url)
            
            # Așteaptă să se încarce harta
            WebDriverWait(self.driver, 20).until(
                EC.presence_of_element_located((By.CLASS_NAME, "map-container"))
            )
            
            # Caută elementele UAT pe hartă
            # (selectorii exacti trebuie ajustați pe baza structurii reale)
            uat_elements = self.driver.find_elements(By.CSS_SELECTOR, ".uat-item, .feature")
            
            for elem in uat_elements:
                try:
                    name = elem.get_attribute("data-name") or elem.text
                    status = elem.get_attribute("data-status")
                    
                    uat_data.append({
                        'name': name,
                        'status': status,
                        'url': self.driver.current_url
                    })
                except Exception as e:
                    logger.warning(f"Failed to extract UAT element: {e}")
            
            logger.info(f"Extracted {len(uat_data)} UATs from map")
            
        except Exception as e:
            logger.error(f"Failed to extract UAT list: {e}")
        
        return uat_data
    
    def scrape_news_and_decisions(self) -> List[Dict]:
        """
        Extrage știri și decizii despre amalgamare.
        """
        decisions = []
        
        try:
            # Extrage PDF-uri cu decizii
            # Exemplu: decizia pentru Baurci-Moldoveni[reference:2]
            pdf_patterns = [
                "/sites/default/files/2025-12/DIAV_Baurci-Moldoveni_Cahul.pdf",
                # Adaugă alte pattern-uri descoperite
            ]
            
            for pattern in pdf_patterns:
                decisions.append({
                    'type': 'decision',
                    'url': f"{self.BASE_URL}{pattern}",
                    'date': '2025-03-18'  # Extras din document
                })
            
        except Exception as e:
            logger.error(f"Failed to scrape decisions: {e}")
        
        return decisions
    
    def fetch_all_amalgamation_data(self) -> Dict:
        """
        Metodă principală: extrage toate datele despre amalgamare.
        """
        all_data = {
            'uat_in_process': [],
            'completed_amalgamations': [],
            'decisions': [],
            'last_updated': time.strftime('%Y-%m-%d %H:%M:%S'),
            'source': self.BASE_URL
        }
        
        # Extrage UAT-uri de pe hartă
        uat_list = self.get_uat_list()
        all_data['uat_in_process'] = uat_list
        
        # Extrage decizii
        decisions = self.scrape_news_and_decisions()
        all_data['decisions'] = decisions
        
        # Salvează datele
        output_path = self.output_dir / "amalgamation_status.json"
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(all_data, f, indent=2, ensure_ascii=False)
        
        # Închide driver-ul
        if self.driver:
            self.driver.quit()
        
        return all_data

# Exemplu de utilizare
if __name__ == "__main__":
    scraper = AmalgamationStatusScraper()
    data = scraper.fetch_all_amalgamation_data()
    print(f"Found {len(data['uat_in_process'])} UATs in amalgamation process")