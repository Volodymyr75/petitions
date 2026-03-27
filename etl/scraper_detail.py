
from curl_cffi import requests
from bs4 import BeautifulSoup
import time
import random
import re
from datetime import datetime

# --- CONFIG & HEADERS ---
BASE_URL = "https://petition.president.gov.ua/petition/"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
}

# --- DATE HELPERS ---
MONTHS_UA = {
    'січня': 1, 'лютого': 2, 'березня': 3, 'квітня': 4,
    'травня': 5, 'червня': 6, 'липня': 7, 'серпня': 8,
    'вересня': 9, 'жовтня': 10, 'листопада': 11, 'грудня': 12
}

def normalize_date(date_str):
    """Конвертує '15 жовтня 2015' або ISO → 'YYYY-MM-DD'"""
    if not date_str:
        return None
    try:
        # President format: "15 жовтня 2015"
        match = re.match(r'(\d{1,2})\s+(\w+)\s+(\d{4})', date_str)
        if match:
            day, month_ua, year = match.groups()
            month = MONTHS_UA.get(month_ua.lower())
            if month:
                return f"{year}-{month:02d}-{int(day):02d}"
        # Cabinet format: ISO "2021-12-02T00:00:00.000Z"
        if 'T' in date_str:
            return date_str[:10]
        return None
    except:
        return None

# --- PARSING HELPERS ---
def clean_votes(vote_str):
    if not vote_str:
        return 0
    digits = re.sub(r'\D', '', vote_str)
    return int(digits) if digits else 0

def extract_status(soup, page_text):
    """Determines status from page content"""
    # 1. Check classes first (more reliable)
    if soup.find(class_='status_active'): return "Триває збір підписів"
    if soup.find(class_='status_answered'): return "З відповіддю"
    if soup.find(class_='status_archive'): return "Архів"
    if soup.find(class_='status_process'): return "На розгляді"
    
    # 2. Text fallback with variations
    if "Триває збір підписів" in page_text: return "Триває збір підписів"
    
    if "На розгляді" in page_text or "Очікує на розгляд" in page_text: 
        return "На розгляді"
        
    if "З відповіддю" in page_text or "Розглянуто" in page_text: 
        return "З відповіддю"
        
    if "Архів" in page_text or "Збір підписів завершено" in page_text or "Не підтримана" in page_text: 
        return "Архів"
    
    return "Unknown"

# --- MAIN SCRAPER ---
def fetch_petition_detail(pet_id, session=None, attempt=1, max_attempts=3):
    """
    Fetches a single petition by ID.
    Returns dict or None if 404/Error.
    """
    if session is None:
        session = requests.Session(impersonate="chrome")
        
    url = f"{BASE_URL}{pet_id}"
    
    try:
        resp = session.get(url, timeout=15)
        
        # Handle 404 cleanly
        if resp.status_code == 404:
            return {"id": str(pet_id), "status": "Not Found", "error": 404}
            
        # Rate limits
        if resp.status_code in (429, 503):
            if attempt < max_attempts:
                wait_time = 30 * attempt
                print(f"⏳ Rate limit {resp.status_code} on ID {pet_id}, waiting {wait_time}s...")
                time.sleep(wait_time)
                return fetch_petition_detail(pet_id, session, attempt + 1, max_attempts)
            else:
                return {"id": str(pet_id), "error": resp.status_code}

        if resp.status_code != 200:
            return {"id": str(pet_id), "error": resp.status_code}
            
        # Parse
        soup = BeautifulSoup(resp.text, 'html.parser')
        h1 = soup.find('h1')
        
        if not h1 or "Такої сторінки не існує" in h1.get_text():
            return {"id": str(pet_id), "status": "Not Found", "error": 404}

        data = {
            'source': 'president',
            'id': str(pet_id),
            'title': h1.get_text(strip=True),
            'url': url
        }

        # Number
        num_tag = soup.find(class_='pet_number')
        data['number'] = num_tag.get_text(strip=True) if num_tag else None

        # Dates & Author
        date_tags = soup.find_all(class_='pet_date')
        data['author'] = None
        data['date'] = None
        for dt in date_tags:
            text = dt.get_text(strip=True)
            if "Автор" in text or "ініціатор" in text:
                data['author'] = text.split(":", 1)[1].strip() if ":" in text else text.replace("Автор (ініціатор)", "").strip()
            elif "Дата оприлюднення" in text:
                data['date'] = text.split(":", 1)[1].strip() if ":" in text else text.replace("Дата оприлюднення", "").strip()

        # Status
        # 1. Try legacy class-based status
        # 2. Try new .petition_votes_status container
        # 3. Fallback to extracting from text
        status_text = extract_status(soup, resp.text)
        
        # Check new container if extract_status returned Unknown
        if status_text == "Unknown":
            new_status_div = soup.find(class_='petition_votes_status')
            if new_status_div:
                st_text = new_status_div.get_text(strip=True)
                if "Триває збір" in st_text: status_text = "Триває збір підписів"
                elif "На розгляді" in st_text: status_text = "На розгляді"
                elif "З відповіддю" in st_text: status_text = "З відповіддю"
                elif "Архів" in st_text: status_text = "Архів"
        
        data['status'] = status_text

        # Votes
        votes_tag = soup.find(class_='pet_votes_num')
        if not votes_tag:
             votes_tag = soup.find(class_='pet_votes')
        
        # New structure support: votes are in .petition_votes_txt span
        if not votes_tag:
             # Find .petition_votes_txt and get the first span
             txt_div = soup.find(class_='petition_votes_txt')
             if txt_div:
                 votes_tag = txt_div.find('span')

        data['votes'] = clean_votes(votes_tag.get_text(strip=True)) if votes_tag else 0

        # Text length
        # New structure: text is usually in #pet-tab-1
        article = soup.find(id='pet-tab-1')
        if not article:
             article = soup.find(class_='tab_container')
        if not article:
             article = soup.find('article', class_='article')
             
        data['text_length'] = len(article.get_text(strip=True)) if article else 0
        
        # Legacy field (ignored but kept for schema)
        data['has_answer'] = (data['status'] == "З відповіддю")
        
        # Normalized date
        data['date_normalized'] = normalize_date(data.get('date'))

        return data

    except Exception as e:
        print(f"💥 Error scraping ID {pet_id}: {e}")
        return {"id": str(pet_id), "error": str(e)}
