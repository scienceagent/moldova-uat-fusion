"""
Extrage datele populației pentru unitățile administrativ-teritoriale din Moldova.
Sursă principală: Biroul Național de Statistică (statistica.gov.md)
"""

import requests
import pandas as pd
from bs4 import BeautifulSoup
import json
import time
from pathlib import Path
from typing import Dict, List, Optional
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class PopulationScraper:
    """Extrage date demografice pentru toate UAT-urile din Moldova."""
    
    BASE_URL = "https://statistica.gov.md"
    
    # URL-uri cunoscute pentru date populație
    POPULATION_PAGES = [
        "/ro/rezultatele-finale-ale-recensamantului-populatiei-si-locuintelor-2024-distributi-10121_62380.html",
        "/ro/populatia-1024.html",
        "/ro/populatia-pe-universul-6981.html"
    ]
    
    def __init__(self, output_dir: Path = Path("data/raw/population")):
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
    
    def extract_tables_from_page(self, url: str) -> List[pd.DataFrame]:
        """
        Extrage toate tabelele dintr-o pagină BNS și le convertește în DataFrame-uri.
        """
        logger.info(f"Extracting from: {url}")
        
        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
        except requests.RequestException as e:
            logger.error(f"Failed to fetch {url}: {e}")
            return []
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Caută toate tabelele din pagină
        tables = soup.find_all('table')
        extracted_dfs = []
        
        for i, table in enumerate(tables):
            try:
                # Convertește tabelul HTML în DataFrame
                df = pd.read_html(str(table))[0]
                
                # Verifică dacă tabelul conține date relevante (populație pe localități)
                if self._is_population_table(df):
                    extracted_dfs.append(df)
                    logger.info(f"Found population table #{i+1} with {len(df)} rows")
                    
                    # Salvează tabelul ca CSV
                    csv_path = self.output_dir / f"population_table_{i+1}.csv"
                    df.to_csv(csv_path, index=False)
                    logger.info(f"Saved to {csv_path}")
                    
            except Exception as e:
                logger.warning(f"Failed to parse table #{i+1}: {e}")
                continue
        
        return extracted_dfs
    
    def _is_population_table(self, df: pd.DataFrame) -> bool:
        """Verifică dacă un tabel conține date populație."""
        # Caută coloane relevante
        relevant_terms = ['populație', 'populaţie', 'locuitori', 'pop.', 
                          'total', 'localitate', 'oraș', 'comună', 'sat']
        
        # Verifică numele coloanelor
        for col in df.columns:
            col_lower = str(col).lower()
            if any(term in col_lower for term in relevant_terms):
                return True
        
        # Verifică primele rânduri pentru conținut numeric
        numeric_cols = df.select_dtypes(include=['number']).columns
        if len(numeric_cols) >= 2 and len(df) > 5:
            return True
        
        return False
    
    def fetch_population_data(self) -> Dict:
        """
        Metoda principală: parcurge toate paginile și agreghează datele.
        """
        all_data = {
            'uat_population': {},
            'last_updated': time.strftime('%Y-%m-%d %H:%M:%S'),
            'source': self.BASE_URL
        }
        
        for page in self.POPULATION_PAGES:
            url = f"{self.BASE_URL}{page}"
            dfs = self.extract_tables_from_page(url)
            
            for df in dfs:
                # Încearcă să identifice coloana cu numele localității
                name_col = self._find_name_column(df)
                pop_col = self._find_population_column(df)
                
                if name_col and pop_col:
                    # Curăță datele și le adaugă în dicționar
                    for _, row in df.iterrows():
                        uat_name = self._clean_name(str(row[name_col]))
                        try:
                            population = self._extract_number(row[pop_col])
                            if uat_name and population > 0:
                                all_data['uat_population'][uat_name] = population
                        except (ValueError, TypeError):
                            continue
        
        # Salvează datele agregate
        output_path = self.output_dir / "population_data.json"
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(all_data, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Saved population data for {len(all_data['uat_population'])} UATs")
        return all_data
    
    def _find_name_column(self, df: pd.DataFrame) -> Optional[str]:
        """Identifică coloana care conține numele localităților."""
        for col in df.columns:
            col_lower = str(col).lower()
            if any(term in col_lower for term in ['localitate', 'comuna', 'oras', 
                                                   'sat', 'unitate', 'uat']):
                return col
        return df.columns[0] if len(df.columns) > 0 else None
    
    def _find_population_column(self, df: pd.DataFrame) -> Optional[str]:
        """Identifică coloana care conține datele populației."""
        for col in df.columns:
            col_lower = str(col).lower()
            if any(term in col_lower for term in ['populație', 'populaţie', 
                                                   'locuitori', 'pop.']):
                return col
        
        # Dacă nu găsește, încearcă coloanele numerice
        numeric_cols = df.select_dtypes(include=['number']).columns
        return numeric_cols[0] if len(numeric_cols) > 0 else None
    
    def _clean_name(self, name: str) -> str:
        """Curăță și normalizează numele localității."""
        import re
        # Elimină caractere speciale și spații multiple
        cleaned = re.sub(r'\s+', ' ', str(name)).strip()
        # Elimină prefixe/sufixe comune
        cleaned = cleaned.replace('comuna ', '').replace('orașul ', '')
        cleaned = cleaned.replace('satul ', '').replace('municipiul ', '')
        return cleaned
    
    def _extract_number(self, value) -> int:
        """Extrage numărul dintr-un string (ex: '1,234 locuitori' -> 1234)."""
        import re
        if pd.isna(value):
            return 0
        str_value = str(value)
        numbers = re.findall(r'[\d,]+', str_value)
        if numbers:
            return int(numbers[0].replace(',', ''))
        return 0

# Exemplu de utilizare
if __name__ == "__main__":
    scraper = PopulationScraper()
    data = scraper.fetch_population_data()
    print(f"Extracted {len(data['uat_population'])} UAT population records")