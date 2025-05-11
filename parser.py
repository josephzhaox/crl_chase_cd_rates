"""
parser.py
----------
Extracts term + APY + rate label from Chase CD page HTML.
"""

import re
from typing import List, Dict
from bs4 import BeautifulSoup

def _clean(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()

def _is_rate_cell(txt: str) -> bool:
    return bool(re.search(r"\d+\.\d+\s*%", txt))

def parse_rates(html: str) -> List[Dict[str, str | float]]:
    soup = BeautifulSoup(html, "html.parser")

    # Find the first <table> with an "APY" header
    target = None
    for tbl in soup.find_all("table"):
        headers = [_clean(th.get_text()) for th in tbl.find_all("th")]
        if any("APY" in h.upper() for h in headers):
            target = tbl
            break
    if not target:
        return []

    out = []
    for row in target.find_all("tr"):
        cells = [_clean(c.get_text()) for c in row.find_all(["th","td"])]
        if len(cells) < 2:
            continue
        term = cells[0]
        rate_cells = [c for c in cells[1:] if _is_rate_cell(c)]
        for idx, rc in enumerate(rate_cells):
            m = re.search(r"([\d.]+)\s*%", rc)
            if not m:
                continue
            out.append({
                "term": term,
                "apy": float(m.group(1)),
                "rate_label": "Relationship" if idx==1 else "Standard"
            })
    return out
