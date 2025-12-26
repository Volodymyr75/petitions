"""
Sample petition IDs from different ranges to understand field availability and statuses
"""
import requests
from bs4 import BeautifulSoup
import time
import json
from collections import Counter

BASE_URL = "https://petition.president.gov.ua/petition/"
HEADERS = {"User-Agent": "Mozilla/5.0"}

# ID ranges to sample
RANGES = [
    (1, 20),
    (1001, 1020),
    (5001, 5020),
    (60001, 60020),
    (100001, 100020),
    (150001, 150020)
]

def extract_petition_data(pet_id):
    """Extract all available fields from a petition page"""
    url = f"{BASE_URL}{pet_id}"
    
    try:
        resp = requests.get(url, headers=HEADERS, timeout=10)
        
        # Check for 404
        if "404" in resp.text or "не існує" in resp.text or resp.status_code != 200:
            return None
            
        soup = BeautifulSoup(resp.text, 'html.parser')
        
        # Title
        h1 = soup.find('h1')
        if not h1:
            return None
        
        data = {
            'id': pet_id,
            'title': h1.get_text(strip=True),
            'fields_found': []
        }
        
        # Number
        num_tag = soup.find(class_='pet_number')
        if num_tag:
            data['number'] = num_tag.get_text(strip=True)
            data['fields_found'].append('number')
        
        # All date/author fields
        date_tags = soup.find_all(class_='pet_date')
        for dt in date_tags:
            text = dt.get_text(strip=True)
            if "Автор" in text or "ініціатор" in text:
                data['author'] = text
                data['fields_found'].append('author')
            elif "Дата оприлюднення" in text:
                data['date_published'] = text
                data['fields_found'].append('date_published')
        
        # Status - check all possible indicators
        status_found = []
        if soup.find(string=lambda t: t and "Триває збір підписів" in t):
            status_found.append("Триває збір підписів")
        if soup.find(string=lambda t: t and "На розгляді" in t):
            status_found.append("На розгляді")
        if soup.find(string=lambda t: t and "З відповіддю" in t):
            status_found.append("З відповіддю")
        if soup.find(string=lambda t: t and "Архів" in t):
            status_found.append("Архів")
        if soup.find(string=lambda t: t and "Очікує" in t):
            status_found.append("Очікує на розгляд")
            
        data['status'] = status_found[0] if status_found else "Unknown"
        data['fields_found'].append('status')
        
        # Text
        article = soup.find(class_='article')
        if article:
            data['text_length'] = len(article.get_text())
            data['fields_found'].append('text')
        
        # Answer
        answer_tab = soup.find(string=lambda t: t and "Відповідь на петицію" in t)
        if answer_tab:
            data['has_answer'] = True
            data['fields_found'].append('answer')
        
        return data
        
    except Exception as e:
        print(f"  Error: {e}")
        return None


# Collect data
all_data = []
status_counter = Counter()

print("🔍 Sampling petition IDs across different ranges...\n")

for start, end in RANGES:
    print(f"\n{'='*60}")
    print(f"Range: {start}-{end}")
    print('='*60)
    
    valid_count = 0
    
    for pet_id in range(start, end + 1):
        data = extract_petition_data(pet_id)
        
        if data:
            valid_count += 1
            all_data.append(data)
            status_counter[data['status']] += 1
            print(f"  ✅ {pet_id}: {data['status']}")
        else:
            print(f"  ❌ {pet_id}: 404/Invalid")
        
        time.sleep(0.3)  # Be polite
    
    print(f"\nЗнайдено валідних: {valid_count}/{end-start+1}")

# Analysis
print("\n\n" + "="*60)
print("📊 АНАЛІЗ РЕЗУЛЬТАТІВ")
print("="*60)

print("\n1. СТАТУСИ ЗНАЙДЕНІ:")
for status, count in status_counter.most_common():
    print(f"   {status:30} = {count} петицій")

print("\n2. ПОЛЯ ЗНАЙДЕНІ У ПЕТИЦІЯХ:")
all_fields = set()
for d in all_data:
    all_fields.update(d.get('fields_found', []))
print(f"   {', '.join(sorted(all_fields))}")

print("\n3. ЧИ Є 'АРХІВ' ЯК СТАТУС?")
if any('Архів' in d['status'] for d in all_data):
    archive_count = status_counter.get('Архів', 0)
    print(f"   ✅ ТАК! Знайдено {archive_count} петицій зі статусом 'Архів'")
    
    # Show examples
    print("\n   Приклади архівних петицій:")
    for d in all_data:
        if 'Архів' in d['status']:
            print(f"     - ID {d['id']}: {d['title'][:50]}...")
            if len([x for x in all_data if 'Архів' in x['status']]) >= 3:
                break
else:
    print("   ❌ НІ! Жодної петиції зі статусом 'Архів' не знайдено")

# Save to file
with open('etl/petition_sampling_results.json', 'w', encoding='utf-8') as f:
    json.dump(all_data, f, indent=2, ensure_ascii=False)

print(f"\n✅ Детальні результати збережено: etl/petition_sampling_results.json")
print(f"   Всього зібрано: {len(all_data)} валідних петицій")
