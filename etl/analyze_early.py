import requests
from bs4 import BeautifulSoup
import time
import json

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
}

def extract_all_fields(html, pet_id):
    """Extract every possible field from a petition page"""
    soup = BeautifulSoup(html, 'html.parser')
    
    # Check if 404
    if "404" in soup.get_text() or "не існує" in soup.get_text():
        return None
    
    data = {"id": pet_id, "url": f"https://petition.president.gov.ua/petition/{pet_id}"}
    
    # Title
    h1 = soup.find('h1')
    data['title'] = h1.get_text(strip=True) if h1 else None
    
    # Number
    num_tag = soup.find(class_='pet_number')
    data['number'] = num_tag.get_text(strip=True) if num_tag else None
    
    # ALL date fields
    date_tags = soup.find_all(class_='pet_date')
    dates = {}
    for dt in date_tags:
        text = dt.get_text(strip=True)
        if "Автор" in text or "ініціатор" in text:
            dates['author'] = text.split(":", 1)[-1].strip() if ":" in text else text
        elif "Дата оприлюднення" in text:
            dates['published'] = text.split(":", 1)[-1].strip() if ":" in text else text
        elif "Дата початку" in text:
            dates['start'] = text.split(":", 1)[-1].strip() if ":" in text else text
        elif "Дата закінчення" in text:
            dates['end'] = text.split(":", 1)[-1].strip() if ":" in text else text
        else:
            dates['other'] = text
    data['dates'] = dates
    
    # Status (check for various indicators)
    status_indicators = []
    if soup.find(string=lambda t: t and "Триває збір підписів" in t):
        status_indicators.append("Триває збір підписів")
    if soup.find(string=lambda t: t and "На розгляді" in t):
        status_indicators.append("На розгляді")
    if soup.find(string=lambda t: t and "З відповіддю" in t):
        status_indicators.append("З відповіддю")
    if soup.find(string=lambda t: t and "Архів" in t):
        status_indicators.append("Архів")
    data['status'] = status_indicators[0] if status_indicators else "Unknown"
    
    # Votes/signatures count (try multiple selectors)
    votes = None
    for selector in ['.pet_votes', '.votes', '.count']:
        v_tag = soup.find(class_=selector.strip('.'))
        if v_tag:
            votes = v_tag.get_text(strip=True)
            break
    data['votes'] = votes
    
    # Text length
    article = soup.find(class_='article')
    data['text_length'] = len(article.get_text()) if article else 0
    
    # Check for answer tab
    answer_tab = soup.find(string=lambda t: t and "Відповідь на петицію" in t)
    data['has_answer'] = answer_tab is not None
    
    # Signers count
    signers_container = soup.find(class_='users_table')
    if signers_container:
        rows = signers_container.find_all(class_='table_row')
        data['signers_shown'] = len(rows)
    else:
        data['signers_shown'] = 0
    
    return data

# Scan IDs 1-100
results = []
found_count = 0

print("🔍 Scanning petition IDs 1-100...\n")

for pet_id in range(1, 101):
    url = f"https://petition.president.gov.ua/petition/{pet_id}"
    
    try:
        resp = requests.get(url, headers=HEADERS, timeout=10)
        
        if resp.status_code == 200:
            data = extract_all_fields(resp.text, pet_id)
            
            if data:
                found_count += 1
                results.append(data)
                print(f"[{pet_id}] ✅ {data['title'][:50]}...")
            else:
                print(f"[{pet_id}] ❌ 404/Invalid")
        else:
            print(f"[{pet_id}] ❌ HTTP {resp.status_code}")
            
    except Exception as e:
        print(f"[{pet_id}] 💥 Error: {e}")
    
    time.sleep(0.5)  # Be polite

print(f"\n{'='*60}")
print(f"SUMMARY: Found {found_count}/100 valid petitions")
print(f"{'='*60}\n")

# Save results
with open('etl/early_petitions_analysis.json', 'w', encoding='utf-8') as f:
    json.dump(results, f, indent=2, ensure_ascii=False)

# Print detailed analysis
if results:
    print("=== SAMPLE PETITION (First found) ===")
    print(json.dumps(results[0], indent=2, ensure_ascii=False))
    
    print("\n=== AVAILABLE FIELDS ACROSS ALL ===")
    all_fields = set()
    for r in results:
        all_fields.update(r.keys())
    print(", ".join(sorted(all_fields)))
    
    print("\n=== DATE FIELDS FOUND ===")
    date_types = set()
    for r in results:
        if r.get('dates'):
            date_types.update(r['dates'].keys())
    print(", ".join(sorted(date_types)))

print("\n✅ Results saved to: etl/early_petitions_analysis.json")
