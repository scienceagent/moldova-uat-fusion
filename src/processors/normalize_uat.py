from unidecode import unidecode
import re

def normalize_name(name: str) -> str:
    s = (name or "").strip().lower()
    s = unidecode(s)
    s = re.sub(r"\b(municipiul|orasul|comuna|satul)\b", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s