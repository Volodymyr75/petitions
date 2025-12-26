import requests
import duckdb
import time
import random
import re
from bs4 import BeautifulSoup
from pipeline import save_to_db, DB_FILE

# Range to scrape (approx 3000 items from recent history backwards)
START_ID = 256000
END_ID = 253000 

URL_TEMPLATE = "https://petition.president.gov.ua/petition/{}"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

def parse_single_page(html, pet_id):
    soup = BeautifulSoup(html, 'html.parser')
    
    # Title
    title_tag = soup.find('h1')
    if not title_tag:
        return None # Effectively 404 or bad page
    title = title_tag.get_text(strip=True)
    
    # Filter 404
    if "404" in title or "не існує" in title:
        return None
    
    # Number
    number_tag = soup.find(class_='pet_number')
    number = number_tag.get_text(strip=True) if number_tag else f"№22/{pet_id}-ep"
    
    # Date
    date = "Unknown"
    date_tags = soup.find_all(class_='pet_date')
    for dt in date_tags:
        text = dt.get_text(strip=True)
        if "Дата оприлюднення" in text:
            # Extract content after colon
            try:
                date = text.split(":", 1)[1].strip()
            except:
                date = text.replace("Дата оприлюднення", "").strip()
            break
            
    # Status (Inference)
    # If "Відповідь" tab is disabled, it's Active or Archive.
    # If it's old (date check) and Active, it's active.
    # For now, default to "Archive" unless we see "active" signals.
    status = "Archive" # Default bucket for deep scrape
    
    # Votes - tricky, trying regex on the whole body or checking specific classes
    votes = 0
    
    return {
        "source": "president",
        "id": str(pet_id),
        "number": number,
        "title": title,
        "date": date,
        "status": status,
        "votes": votes,
        "url": URL_TEMPLATE.format(pet_id)
    }

def run_deep_scrape():
    con = duckdb.connect(DB_FILE)
    
    # Target only broken records
    print("🔍 Searching for 'Unknown' dates in DB...")
    targets = con.execute("SELECT external_id FROM petitions WHERE source='president' AND date='Unknown'").fetchall()
    target_ids = [row[0] for row in targets]
    print(f"🚀 Found {len(target_ids)} petitions to repair.")

    batch = []
    
    for str_id in target_ids:
        current_id = int(str_id)
        url = URL_TEMPLATE.format(current_id)
        
        # Delay
        time.sleep(random.uniform(0.5, 1.2))
        
        try:
            resp = requests.get(url, headers=HEADERS)
            if resp.status_code == 200:
                # Basic check for empty/redirect
                if "Redirecting" in resp.text:
                    print(f"[{current_id}] ⚠️ Redirect/404 (Removing?)")
                    # Optionally remove from DB if it's dead, but let's keep for now
                    continue
                    
                pet = parse_single_page(resp.text, current_id)
                
                if pet is None:
                    # It's a 404 or bad page. Purge it!
                    print(f"[{current_id}] 🗑️ Invalid/404 - Deleting from DB")
                    con.execute("DELETE FROM petitions WHERE source='president' AND external_id=?", (str(current_id),))
                    continue
                    
                if pet['date'] != "Unknown":
                    print(f"[{current_id}] ✅ Fixed Date: {pet['date']}")
                    batch.append(pet)
                else:
                    print(f"[{current_id}] ⚠️ Date still not found (HTML mismatch?)")
            else:
                print(f"[{current_id}] ❌ HTTP {resp.status_code}")
                
        except Exception as e:
            print(f"[{current_id}] 💥 Error: {e}")
            
        # Save every 20 items (updates)
        if len(batch) >= 20:
            save_to_db(con, batch)
            batch = []
            
    if batch:
        save_to_db(con, batch)
        
    con.close()
    print("🏁 Repair complete.")

if __name__ == "__main__":
    run_deep_scrape()
