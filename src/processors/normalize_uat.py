from unidecode import unidecode
import re

def normalize_name(name: str) -> str:
    if not name: return ""
    # Standardize characters and lower case
    s = name.strip().lower()
    s = unidecode(s)
    
    # Remove common administrative prefixes and abbreviations
    # We use \b and non-alphanumeric separators
    prefixes = r"\b(municipiul|orasul|comuna|satul|raionul|r-nul|mun|or|com|sat|mld)\b"
    s = re.sub(prefixes, " ", s)
    
    # Remove all non-alphanumeric except space
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    
    # Collapse multiple spaces, split into words, sort, and join
    # This makes "Briceni Hlina" equal to "Hlina Briceni"
    words = sorted([w for w in s.split() if len(w) > 1])
    return " ".join(words)

if __name__ == "__main__":
    test_cases = [
        "r-nul Briceni, or. Briceni",
        "mun. Chișinău",
        "R-NUL BRICENI, OR. BRICENI",
        "com. Bălășești",
        "orașul Bălți",
        "Hlina|Briceni",
        "r-nul Briceni, sat. Hlina"
    ]
    for tc in test_cases:
        print(f"'{tc}' -> '{normalize_name(tc)}'")