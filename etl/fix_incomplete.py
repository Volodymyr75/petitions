"""
Скрипт для точкового дозаповнення метаданих (author, text_length, has_answer).
Бере список ID з файлу incomplete_petitions.json.
Виконує тільки UPDATE існуючих записів.
"""
import requests
from bs4 import BeautifulSoup
import duckdb
import time
import random
import json
from datetime import datetime

BASE_URL = "https://petition.president.gov.ua/petition/"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
}
DB_FILE = "petitions.duckdb"
IDS_FILE = "incomplete_petitions.json"

session = requests.Session()

def polite_sleep(iteration):
    time.sleep(random.uniform(0.7, 1.4))
    if iteration % 50 == 0:
        extra = random.uniform(3.0, 6.0)
        print(f"⏳ Пауза {extra:.1f} с...")
        time.sleep(extra)

def extract_petition_data(pet_id):
    url = f"{BASE_URL}{pet_id}"
    try:
        resp = session.get(url, headers=HEADERS, timeout=15)
        if resp.status_code == 404 or resp.url.endswith('/404'):
            return None
        if resp.status_code in (429, 503):
            print(f"⏳ Rate limit {resp.status_code} на ID {pet_id}, чекаємо 30с")
            time.sleep(30)
            return extract_petition_data(pet_id)
        
        if resp.status_code != 200:
            return None

        soup = BeautifulSoup(resp.text, 'html.parser')
        h1 = soup.find('h1')
        if not h1 or "Такої сторінки не існує" in h1.get_text():
            return None

        data = {
            'source': 'president',
            'id': str(pet_id),
            'title': h1.get_text(strip=True),
            'url': url
        }

        num_tag = soup.find(class_='pet_number')
        data['number'] = num_tag.get_text(strip=True) if num_tag else None

        date_tags = soup.find_all(class_='pet_date')
        data['author'] = None
        data['date'] = None
        for dt in date_tags:
            text = dt.get_text(strip=True)
            if "Автор" in text or "ініціатор" in text:
                data['author'] = text.split(":", 1)[1].strip() if ":" in text else text.replace("Автор (ініціатор)", "").strip()
            elif "Дата оприлюднення" in text:
                data['date'] = text.split(":", 1)[1].strip() if ":" in text else text.replace("Дата оприлюднення", "").strip()

        page_text = resp.text
        data['status'] = "Unknown"
        
        # Порядок важливий: від найбільш специфічних до загальних
        if "З відповіддю" in page_text: 
            data['status'] = "З відповіддю"
        elif "На розгляді" in page_text: 
            data['status'] = "На розгляді"
        elif "Триває збір підписів" in page_text or "Залишилося" in page_text or "Збір підписів триває" in page_text:
            data['status'] = "Триває збір підписів"
        elif "Не підтримано" in page_text:
            data['status'] = "Не підтримано"
        elif "Архів" in page_text: 
            data['status'] = "Архів"

        votes_graph = soup.find(class_='petition_votes_graph')
        data['votes'] = int(votes_graph.get('data-votes', 0)) if votes_graph else None

        article = soup.find(class_='article')
        data['text_length'] = len(article.get_text()) if article else None
        data['has_answer'] = "Відповідь на петицію" in page_text

        return data
    except Exception as e:
        print(f"💥 Помилка на ID {pet_id}: {e}")
        return None

def run_fix():
    print("="*70)
    print("🚀 TARGETED BACKFILL FOR INCOMPLETE PETITIONS")
    print("="*70)

    try:
        with open(IDS_FILE, 'r') as f:
            ids_to_fix = json.load(f)
    except FileNotFoundError:
        print(f"❌ Файл {IDS_FILE} не знайдено. Запустіть аудит спочатку.")
        return

    total = len(ids_to_fix)
    print(f"📋 Завантажено {total} ID для виправлення.")
    
    con = duckdb.connect(DB_FILE)
    stats = {'checked': 0, 'updated': 0, 'skipped': 0}
    
    for pet_id in ids_to_fix:
        stats['checked'] += 1
        
        data = extract_petition_data(pet_id)
        
        if not data:
            # Якщо петиція раптом стала 404, але вона була в базі - просто скіпаємо
            stats['skipped'] += 1
        else:
            # Виконуємо повне оновлення полів
            fields = ['number', 'title', 'date', 'status', 'votes', 'url', 'author', 'text_length', 'has_answer']
            set_clause = ", ".join([f"{f} = ?" for f in fields])
            params = [data.get(f) for f in fields]
            params.extend(['president', str(pet_id)])
            
            con.execute(f"UPDATE petitions SET {set_clause} WHERE source=? AND external_id=?", params)
            stats['updated'] += 1
        
        if stats['checked'] % 10 == 0:
            print(f"[{stats['checked']}/{total}] Оновлено: {stats['updated']} | Пропущено: {stats['skipped']}")
        
        polite_sleep(stats['checked'])

    con.close()
    print(f"\n✅ ГОТОВО! Оновлено: {stats['updated']} петицій.")

if __name__ == "__main__":
    run_fix()
