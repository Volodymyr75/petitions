import requests
from bs4 import BeautifulSoup
import time
import json
import re
import random

BASE_URL = "https://petition.president.gov.ua"
# Specific URL for 'continuing' petitions (active) - Corrected to root pagination to avoid login
# Now dynamic based on status
BASE_STATUS_URL = "https://petition.president.gov.ua/?status={}&sort=date&order=desc&page={}"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

def clean_votes(vote_str):
    if not vote_str:
        return 0
    digits = re.sub(r'\D', '', vote_str)
    return int(digits) if digits else 0

def scrape_president_petitions(max_pages=1, start_page=1, status="active"):
    """
    Scrapes petitions with a specific status.
    status options: 'active', 'answered', 'archive', 'processing' (on review)
    """
    all_petitions = []
    
    print(f"--- Scraping status: {status} ---")
    
    for page in range(start_page, start_page + max_pages):
        url = BASE_STATUS_URL.format(status, page)
        print(f"Fetching {url}...")
        
        try:
            response = requests.get(url, headers=HEADERS, timeout=10)
            if response.status_code != 200:
                print(f"Failed to fetch page {page}: {response.status_code}")
                break
                
            soup = BeautifulSoup(response.text, 'html.parser')
            items = soup.select(".pet_item")
            
            if not items:
                print(f"No items found on page {page}. Stopping.")
                break
                
            print(f"  Found {len(items)} petitions.")

            for item in items:
                try:
                    link_tag = item.select_one(".pet_link")
                    if not link_tag: continue
                    
                    href = link_tag['href']
                    pet_id = href.split("/")[-1]
                    title = link_tag.get_text(strip=True)
                    
                    number_tag = item.select_one(".pet_number")
                    number_text = number_tag.get_text(strip=True) if number_tag else "N/A"
                    
                    date_tag = item.select_one(".pet_date")
                    date_text = date_tag.get_text(strip=True).replace("Дата оприлюднення:", "").strip() if date_tag else None
                    
                    status_tag = item.select_one(".pet_status")
                    # Use the scraped text, or fallback to our requested status type if parsing fails
                    status_text = status_tag.get_text(strip=True) if status_tag else status

                    counts_tag = item.select_one(".pet_counts")
                    raw_votes = counts_tag.get_text(strip=True) if counts_tag else "0"
                    votes = clean_votes(raw_votes)

                    all_petitions.append({
                        "source": "president",
                        "id": pet_id,
                        "number": number_text,
                        "title": title,
                        "date": date_text,
                        "status": status_text,
                        "votes": votes,
                        "url": BASE_URL + href
                    })
                except Exception as e:
                    print(f"Error parsing item: {e}")
                    continue
            
            # Rate Limiting
            if page < start_page + max_pages - 1:
                sleep_time = random.uniform(2.0, 5.0)
                print(f"  Sleeping for {sleep_time:.2f}s...")
                time.sleep(sleep_time)

        except Exception as e:
            print(f"Error fetching page {page}: {e}")
            break

    return all_petitions

if __name__ == "__main__":
    # Test run: 2 pages
    data = scrape_president_petitions(max_pages=2)
    print(f"Total collected: {len(data)}")
    if data:
        print(json.dumps(data[0], indent=2, ensure_ascii=False))
