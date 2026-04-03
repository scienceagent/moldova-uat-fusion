from pathlib import Path
import json
from src.scrapers.primariiputernice_scraper import scrape_pages

RAW = Path("data/raw")
PROC = Path("data/processed")
RAW.mkdir(parents=True, exist_ok=True)
PROC.mkdir(parents=True, exist_ok=True)

def main():
    admin = scrape_pages()
    (RAW / "primariiputernice_pages.json").write_text(
        json.dumps(admin, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    master = {
        "updated_at": admin["fetched_at"],
        "sources": {"primariiputernice": admin["source"]},
        "admin_pages_count": admin["count"],
        "uat": []
    }
    (PROC / "uat_master.json").write_text(
        json.dumps(master, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print("OK: pipeline finished")

if __name__ == "__main__":
    main()