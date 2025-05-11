import os, datetime, logging, requests
from google.cloud import firestore
from parser import parse_rates

CD_URL      = "https://www.chase.com/personal/savings/bank-cd"
COLLECTION  = os.getenv("FIRESTORE_COLLECTION", "chase_cd_rates")

def fetch_html() -> str:
    headers = {"User-Agent": "Mozilla/5.0 (compatible; cd-crawler/1.0)"}
    r = requests.get(CD_URL, headers=headers, timeout=15)
    r.raise_for_status()
    return r.text

def save_firestore(rates):
    db = firestore.Client()
    ts = datetime.datetime.now(datetime.timezone.utc)
    for r in rates:
        doc_id = f"{ts.isoformat()}_{r['term'].replace(' ','_')}_{r['rate_label']}"
        db.collection(COLLECTION).document(doc_id).set({
            **r,
            "source": CD_URL,
            "timestamp": ts,
        })
    logging.info("Saved %d records", len(rates))

def main(_event=None, _ctx=None):
    html = fetch_html()
    rates = parse_rates(html)
    if not rates:
        logging.warning("No rates parsed; check selectors.")
    else:
        save_firestore(rates)

if __name__=="__main__":
    main()
