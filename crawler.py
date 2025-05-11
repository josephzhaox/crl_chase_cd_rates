import requests
import logging
from datetime import datetime, timezone
from google.cloud import firestore
## from x_api import XApi  # Replace with tweepy if using tweepy
import time

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


# def post_to_x():
#     x_client = XApi(
#         consumer_key="YOUR_CONSUMER_KEY",
#         consumer_secret="YOUR_CONSUMER_SECRET",
#         access_token="YOUR_ACCESS_TOKEN",
#         access_token_secret="YOUR_ACCESS_TOKEN_SECRET"
#     )
    
#     try:
#         post_text = "Testing Grok posting to X"
#         response = x_client.post_tweet(post_text)
#         post_id = response.get('id_str')
#         time.sleep(2)
#         return {
#             "status": "success",
#             "post_id": post_id,
#             "post_url": f"https://x.com/user/status/{post_id}"
#         }
#     except Exception as e:
#         return {"status": "error", "message": str(e)}

def scrape_chase_cd_rates(db):
    url = "https://www.chase.com/bin/services/cdRate"
    headers = {
        "accept": "application/json",
        "cache-control": "no-cache",
        "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
        "origin": "https://www.chase.com",
        "referer": "https://www.chase.com/personal/savings/bank-cd",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36 Edg/136.0.0.0"
    }
    
    # Form data
    data = {
        "zipcode": "46032",
        "type": "consumer",
        "language": "en-US",
        "bankCode": "053",
        "regionCode": "001"
    }
    
    try:
        # Initialize session
        session = requests.Session()
        session.get("https://www.chase.com/personal/savings/bank-cd")  
        # Send POST request
        response = requests.post(url, headers=headers, data=data, timeout=10)
        response.raise_for_status()
        
        # Parse JSON response
        json_data = response.json()
        
        # Extract deposit tiers (skip "CD Term" at index 0)
        deposit_tiers = json_data['ratesData']['cdTermLabels'][1:]  # ["$0 - $9,999.99", ...]
        
        # Extract rates and terms
        rates = json_data['ratesData']['rates']  # {"1-Month": ["0.02%", ...], ...}
        effective_date = json_data.get('ratesEffectiveDate', '')
        
        cd_data = []
        # Iterate over each term and its APYs
        for term, apy_list in rates.items():
            # Pair each APY with its deposit tier
            for i, apy in enumerate(apy_list):
                if apy:  # Ensure APY is not empty
                    cd_data.append({
                        'term': term,
                        'deposit_tier': deposit_tiers[i],
                        'apy': apy,
                        'timestamp': datetime.now(timezone.utc).isoformat(),
                        'source': "https://www.chase.com/personal/savings/bank-cd",
                        'zipcode': data['zipcode'],
                        'effective_date': effective_date
                    })
                    ## logging.info(f"Extracted: term={term}, deposit_tier={deposit_tiers[i]}, apy={apy}")
        
        if not cd_data:
            raise ValueError("No CD data extracted from API response")
        
        # Post to X
        # post_result = post_to_x()
        # post_id = post_result.get("post_id", None)
        # post_url = post_result.get("post_url", None)
        
        # Store in Firestore
        for record in cd_data:
            # record['post_id'] = post_id
            # record['post_url'] = post_url
            doc_ref = db.collection('cd_rates').document()
            doc_ref.set(record)
            ## logging.info(f"Saved CD data: {record}")
        
        return {
            "status": "success",
            "records_saved": len(cd_data),
            # "post_id": post_id,
            # "post_url": post_url
        }
    
    except Exception as e:
        logging.error(f"Error during API scraping: {str(e)}")
        return {"status": "error", "message": str(e)}

def main(request=None):

    # Initialize Firestore client
    db = firestore.Client(project="crawler-chase-cd-rates", database="scraped-chase-cd-rates")  # Replace with your project ID

    """Cloud Function entry point"""
    result = scrape_chase_cd_rates(db)
    return result

if __name__ == "__main__":
    main()