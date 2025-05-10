import requests
from bs4 import BeautifulSoup
from google.cloud import firestore
from datetime import datetime
import logging
import google.cloud.logging

# Set up Google Cloud Logging
client = google.cloud.logging.Client()
client.setup_logging()

# Initialize Firestore client
db = firestore.Client()

def scrape_chase_cd_rates():
    url = "https://www.chase.com/personal/savings/bank-cd"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    
    try:
        # Send HTTP request
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        
        # Parse HTML
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Find CD rate sections (adjust selectors based on actual HTML structure)
        cd_sections = soup.find_all('div', class_='rate-table')  # Example class, inspect actual site
        
        cd_data = []
        for section in cd_sections:
            term = section.find('span', class_='term').text.strip()  # Example selector
            apy = section.find('span', class_='apy').text.strip()    # Example selector
            cd_data.append({
                'term': term,
                'apy': apy,
                'timestamp': datetime.utcnow().isoformat(),
                'source': url
            })
        
        # Store in Firestore
        for data in cd_data:
            doc_ref = db.collection('cd_rates').document()
            doc_ref.set(data)
            logging.info(f"Saved CD data: {data}")
        
        return {"status": "success", "records_saved": len(cd_data)}
    
    except Exception as e:
        logging.error(f"Error during scraping: {str(e)}")
        return {"status": "error", "message": str(e)}

def main(request=None):
    """Cloud Function entry point"""
    result = scrape_chase_cd_rates()
    return result

if __name__ == "__main__":
    main()