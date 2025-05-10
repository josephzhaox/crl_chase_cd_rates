import os, datetime, logging, requests, bs4
from google.cloud import firestore

CD_URL = "https://www.chase.com/personal/savings/bank-cd"
COLLECTION = os.environ.get("FIRESTORE_COLLECTION", "chase_cd_rates")

def fetch_html() -> str:
    headers = {"User-Agent": "Mozilla/5.0 (compatible; cd-crawler/1.0)"}
    resp = requests.get(CD_URL, headers=headers, timeout=15)
    resp.raise_for_status()
    return resp.text

def parse_rates(html: str) -> list[dict]:
    soup = bs4.BeautifulSoup(html, "html.parser")
    rates = []
    # Chase renders a static table under an element that contains “APY”.
    for row in soup.find_all("tr"):
        cells = [c.get_text(strip=True) for c in row.find_all(["th", "td"])]
        if len(cells) >= 3 and "APY" in cells[1]:
            term = cells[0]
            apy  = cells[1].replace("%", "")
            rates.append({
                "term": term,
                "apy": float(apy),
            })
    return rates

def save_firestore(rates: list[dict]) -> None:
    db = firestore.Client()
    ts = datetime.datetime.now(datetime.timezone.utc)
    for r in rates:
        doc_id = f"{ts.isoformat()}_{r['term'].replace(' ', '_')}"
        db.collection(COLLECTION).document(doc_id).set({
            **r,
            "source": CD_URL,
            "timestamp": ts,
        })

def main(_event=None):
    html = fetch_html()
    rates = parse_rates(html)
    if not rates:
        logging.warning("No rates parsed - check parser selectors.")
    save_firestore(rates)
    logging.info("Saved %d rate records.", len(rates))

if __name__ == "__main__":
    main()
