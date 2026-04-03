"""
Extrage date geospațiale pentru unitățile administrativ-teritoriale.
Surse: geodata.gov.md, data.humdata.org, geoportalul INSPIRE
"""

import requests
import json
import zipfile
import io
import shutil
from pathlib import Path
from typing import Optional, Dict
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BoundariesScraper:
    """
    Extrage granițele administrative pentru UAT-urile din Moldova.
    Suportă formate: GeoJSON, Shapefile, KML.
    """
    
    # Surse de date geospațiale pentru Moldova
    SOURCES = {
        'humdata': {
            'url': 'https://data.humdata.org/dataset/republic-of-moldova-administrative-boundaries',
            'description': 'Admin boundaries from OCHA'
        },
        'geodata': {
            'url': 'https://geodata.gov.md/',
            'description': 'Official geospatial portal'
        },
        'inspire': {
            'url': 'https://inspire-geoportal.ec.europa.eu/',
            'description': 'INSPIRE geoportal'
        }
    }
    
    def __init__(self, output_dir: Path = Path("data/raw/boundaries")):
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.session = requests.Session()
    
    def download_humdata_boundaries(self) -> Optional[Path]:
        """
        Descarcă granițele administrative de pe Humanitarian Data Exchange.
        """
        logger.info("Downloading boundaries from HDX...")
        
        # URL direct pentru datele Moldova (admin level 1 și 2)
        # Notă: Verifică mereu URL-ul actual pe data.humdata.org
        hdx_api_url = "https://data.humdata.org/api/3/action/package_show"
        params = {'id': 'republic-of-moldova-administrative-boundaries'}
        
        try:
            response = self.session.get(hdx_api_url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            # Extrage URL-urile resurselor
            resources = data.get('result', {}).get('resources', [])
            for resource in resources:
                if 'geojson' in resource.get('format', '').lower() or \
                   'shapefile' in resource.get('format', '').lower():
                    download_url = resource.get('url')
                    if download_url:
                        return self._download_file(download_url)
            
            # Fallback: descarcă direct de pe site-ul HDX
            # (URL-ul exact trebuie verificat)
            direct_url = "https://data.humdata.org/dataset/.../download/..."
            # return self._download_file(direct_url)
            
        except Exception as e:
            logger.error(f"Failed to download from HDX: {e}")
        
        return None
    
    def create_fallback_boundaries(self) -> Dict:
        """
        Creează granițe simplificate ca fallback.
        Folosește centroizii raioanelor și buffer-e pentru demo.
        """
        logger.info("Creating fallback boundaries...")
        
        # Date aproximative pentru principalele raioane
        # (în producție, acestea trebuie înlocuite cu date reale)
        fallback_data = {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "properties": {
                        "name": "Chișinău",
                        "type": "municipiu",
                        "population": 700000
                    },
                    "geometry": {
                        "type": "Point",
                        "coordinates": [28.8575, 47.0105]
                    }
                },
                {
                    "type": "Feature",
                    "properties": {
                        "name": "Bălți",
                        "type": "municipiu",
                        "population": 150000
                    },
                    "geometry": {
                        "type": "Point",
                        "coordinates": [27.9292, 47.7618]
                    }
                },
                # Adaugă toate cele 32 de raioane + Găgăuzia + Transnistria
                # Datele complete sunt disponibile în repository-ul oficial
            ]
        }
        
        output_path = self.output_dir / "boundaries_fallback.geojson"
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(fallback_data, f, indent=2)
        
        logger.warning(f"Created fallback boundaries at {output_path}")
        return fallback_data
    
    def _download_file(self, url: str) -> Optional[Path]:
        """Descarcă un fișier și îl salvează local."""
        try:
            response = self.session.get(url, stream=True, timeout=60)
            response.raise_for_status()
            
            # Determină extensia din URL sau Content-Type
            content_type = response.headers.get('content-type', '')
            if 'zip' in content_type or url.endswith('.zip'):
                ext = '.zip'
            elif 'geojson' in content_type or url.endswith('.geojson'):
                ext = '.geojson'
            else:
                ext = '.shp'
            
            file_path = self.output_dir / f"boundaries{ext}"
            
            with open(file_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            
            logger.info(f"Downloaded: {file_path}")
            
            # Dacă e ZIP, extrage conținutul
            if ext == '.zip':
                self._extract_zip(file_path)
            
            return file_path
            
        except Exception as e:
            logger.error(f"Download failed: {e}")
            return None
    
    def _extract_zip(self, zip_path: Path):
        """Extrage fișierele dintr-un arhivă ZIP."""
        extract_dir = self.output_dir / "extracted"
        extract_dir.mkdir(exist_ok=True)
        
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(extract_dir)
        
        logger.info(f"Extracted to {extract_dir}")
        
        # Caută fișiere Shapefile
        shp_files = list(extract_dir.glob("*.shp"))
        if shp_files:
            logger.info(f"Found shapefile: {shp_files[0]}")
    
    def fetch_all_boundaries(self) -> Dict:
        """
        Metodă principală: încearcă toate sursele și returnează datele geospațiale.
        """
        # Încearcă mai întâi sursele oficiale
        boundaries_data = None
        
        # 1. HDX (Humanitarian Data Exchange)
        result = self.download_humdata_boundaries()
        if result and result.suffix == '.geojson':
            with open(result, 'r') as f:
                boundaries_data = json.load(f)
        
        # 2. Fallback dacă nu am găsit date
        if not boundaries_data:
            boundaries_data = self.create_fallback_boundaries()
        
        return boundaries_data

# Exemplu de utilizare
if __name__ == "__main__":
    scraper = BoundariesScraper()
    data = scraper.fetch_all_boundaries()
    print(f"Boundaries loaded: {len(data.get('features', []))} features")