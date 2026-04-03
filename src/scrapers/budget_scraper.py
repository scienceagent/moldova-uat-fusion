"""
Extrage date bugetare pentru UAT-uri din surse oficiale.
Sursă principală: dataset.gov.md (API CKAN)
"""

import requests
import json
import pandas as pd
from pathlib import Path
from typing import Dict, List, Optional
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BudgetScraper:
    """
    Extrage date despre venituri și cheltuieli pentru fiecare UAT.
    Folosește API-ul oficial al Portalului de Date Deschise.
    """
    
    # Portalul Guvernamental de Date Deschise
    CKAN_API_URL = "https://dataset.gov.md/ro/api/3"
    
    # Seturi de date relevante pentru bugetele locale
    RELEVANT_DATASETS = [
        "5002-date-privind-executarea-bugetelor-unitatilor-administrativ-teritoriale",  # Date privind executarea bugetelor
        "18540-raport-privind-executarea-bugetului-pentru-cetateni",  # Raport executare buget
    ]
    
    def __init__(self, output_dir: Path = Path("data/raw/budgets")):
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'MoldovaUATFusion/1.0'
        })
    
    def search_datasets(self, query: str = "buget unități administrativ-teritoriale") -> List[Dict]:
        """
        Caută seturi de date relevante folosind API-ul CKAN.
        """
        search_url = f"{self.CKAN_API_URL}/action/package_search"
        params = {'q': query, 'rows': 50}
        
        try:
            response = self.session.get(search_url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            datasets = data.get('result', {}).get('results', [])
            logger.info(f"Found {len(datasets)} datasets for query '{query}'")
            return datasets
            
        except Exception as e:
            logger.error(f"Search failed: {e}")
            return []
    
    def get_dataset_resources(self, dataset_id: str) -> List[Dict]:
        """
        Obține toate resursele (fișierele) disponibile într-un set de date.
        """
        show_url = f"{self.CKAN_API_URL}/action/package_show"
        params = {'id': dataset_id}
        
        try:
            response = self.session.get(show_url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            resources = data.get('result', {}).get('resources', [])
            return resources
            
        except Exception as e:
            logger.error(f"Failed to get resources for {dataset_id}: {e}")
            return []
    
    def download_resource(self, resource: Dict) -> Optional[Path]:
        """
        Descarcă un fișier CSV/Excel dintr-o resursă.
        """
        download_url = resource.get('url')
        format_type = resource.get('format', '').lower()
        
        if not download_url:
            return None
        
        # Determină extensia
        if 'csv' in format_type or download_url.endswith('.csv'):
            ext = '.csv'
        elif 'xlsx' in format_type or download_url.endswith('.xlsx'):
            ext = '.xlsx'
        elif 'json' in format_type or download_url.endswith('.json'):
            ext = '.json'
        else:
            ext = '.csv'  # fallback
        
        file_name = resource.get('name', 'budget_data') + ext
        file_path = self.output_dir / file_name
        
        try:
            response = self.session.get(download_url, timeout=60)
            response.raise_for_status()
            
            with open(file_path, 'wb') as f:
                f.write(response.content)
            
            logger.info(f"Downloaded: {file_path}")
            return file_path
            
        except Exception as e:
            logger.error(f"Failed to download {download_url}: {e}")
            return None
    
    def parse_budget_data(self, file_path: Path) -> pd.DataFrame:
        """
        Parsează un fișier bugetar și extrage informațiile relevante.
        """
        if file_path.suffix == '.csv':
            df = pd.read_csv(file_path)
        elif file_path.suffix == '.xlsx':
            df = pd.read_excel(file_path)
        else:
            return pd.DataFrame()
        
        # Curăță și structurează datele
        # Aici logica depinde de structura reală a fișierelor
        # De obicei, conțin coloane precum: UAT, Venituri, Cheltuieli, An
        
        # Normalizează numele coloanelor
        df.columns = df.columns.str.lower().str.strip()
        
        # Caută coloane relevante
        revenue_cols = [col for col in df.columns if 'venit' in col]
        expense_cols = [col for col in df.columns if 'cheltuieli' in col or 'chelt' in col]
        uat_cols = [col for col in df.columns if 'uat' in col or 'localitate' in col or 'unitate' in col]
        
        logger.info(f"Found columns: UAT={uat_cols}, Revenue={revenue_cols}, Expenses={expense_cols}")
        
        return df
    
    def fetch_all_budgets(self) -> Dict:
        """
        Metodă principală: extrage toate datele bugetare disponibile.
        """
        all_budgets = {
            'uat_budgets': {},
            'last_updated': datetime.now().isoformat(),
            'source': self.CKAN_API_URL
        }
        
        # Caută seturi de date relevante
        datasets = self.search_datasets()
        
        for dataset in datasets[:5]:  # Limităm la primele 5 pentru demo
            dataset_id = dataset.get('id')
            dataset_name = dataset.get('title')
            logger.info(f"Processing dataset: {dataset_name}")
            
            resources = self.get_dataset_resources(dataset_id)
            
            for resource in resources:
                file_path = self.download_resource(resource)
                if file_path:
                    df = self.parse_budget_data(file_path)
                    if not df.empty:
                        # Salvează DataFrame-ul procesat
                        processed_path = self.output_dir / f"processed_{file_path.stem}.csv"
                        df.to_csv(processed_path, index=False)
        
        # Încarcă datele agregate
        # (aici se adaugă logica de agregare pe UAT-uri)
        
        output_path = self.output_dir / "budgets_aggregated.json"
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(all_budgets, f, indent=2)
        
        return all_budgets

# Exemplu de utilizare
if __name__ == "__main__":
    scraper = BudgetScraper()
    data = scraper.fetch_all_budgets()
    print(f"Budget data extracted to {scraper.output_dir}")