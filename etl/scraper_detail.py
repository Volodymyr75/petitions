"""
Enhanced scraper that fetches individual petition pages to extract full metadata.
This is used for detailed data collection (author, text_length, has_answer).
"""
import requests
from bs4 import BeautifulSoup
import time
import random

BASE_URL = "https://petition.president.gov.ua"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
}

def scrape_petition_detail(petition_id):
    """
    Scrapes a single petition page to extract all available metadata.
    Returns dict with author, text_length, has_answer, and other fields.
    """
    url = f"{BASE_URL}/petition/{petition_id}"
    
    try:
        resp = requests.get(url, headers=HEADERS, timeout=10)
        
        if resp.status_code != 200:
            return None
            
        # Check for 404
        if "404" in resp.text or "не існує" in resp.text or "Redirecting" in resp.text:
            return None
            
        soup = BeautifulSoup(resp.text, 'html.parser')
        
        # Title
        h1 = soup.find('h1')
        if not h1:
            return None
        title = h1.get_text(strip=True)
        
        # Number
        num_tag = soup.find(class_='pet_number')
        number = num_tag.get_text(strip=True) if num_tag else None
        
        # Extract all date fields and author
        date_tags = soup.find_all(class_='pet_date')
        date_published = None
        author = None
        
        for dt in date_tags:
            text = dt.get_text(strip=True)
            if "Автор" in text or "ініціатор" in text:
                # Extract author name after colon
                if ":" in text:
                    author = text.split(":", 1)[1].strip()
                else:
                    author = text.replace("Автор (ініціатор)", "").strip()
            elif "Дата оприлюднення" in text:
                if ":" in text:
                    date_published = text.split(":", 1)[1].strip()
                else:
                    date_published = text.replace("Дата оприлюднення", "").strip()
        
        # Status
        status = "Unknown"
        if soup.find(string=lambda t: t and "Триває збір підписів" in t):
            status = "Триває збір підписів"
        elif soup.find(string=lambda t: t and "На розгляді" in t):
            status = "На розгляді"
        elif soup.find(string=lambda t: t and "З відповіддю" in t):
            status = "З відповіддю"
        elif soup.find(string=lambda t: t and "Архів" in t):
            status = "Архів"
        
        # Text length
        article = soup.find(class_='article')
        text_length = len(article.get_text()) if article else 0
        
        # Has answer
        answer_tab = soup.find(string=lambda t: t and "Відповідь на петицію" in t)
        has_answer = answer_tab is not None
        
        return {
            "source": "president",
            "id": str(petition_id),
            "number": number,
            "title": title,
            "date": date_published,
            "status": status,
            "votes": None,  # Not reliably extractable from detail page
            "url": url,
            "author": author,
            "text_length": text_length,
            "has_answer": has_answer
        }
        
    except Exception as e:
        print(f"Error scraping petition {petition_id}: {e}")
        return None


def enrich_petitions_with_details(petition_ids, delay=0.5):
    """
    Takes a list of petition IDs and scrapes their detail pages.
    Returns list of enriched petition dicts.
    
    Args:
        petition_ids: List of petition ID strings
        delay: Delay between requests in seconds
    """
    results = []
    
    for i, pet_id in enumerate(petition_ids):
        print(f"[{i+1}/{len(petition_ids)}] Scraping petition {pet_id}...")
        
        data = scrape_petition_detail(pet_id)
        
        if data:
            results.append(data)
            print(f"  ✅ {data['title'][:50]}...")
        else:
            print(f"  ❌ Failed or 404")
        
        # Polite delay
        if i < len(petition_ids) - 1:
            time.sleep(random.uniform(delay * 0.8, delay * 1.2))
    
    return results


if __name__ == "__main__":
    # Test with a known petition
    test_data = scrape_petition_detail(256946)
    if test_data:
        import json
        print(json.dumps(test_data, indent=2, ensure_ascii=False))
