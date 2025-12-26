"""
Скрипт для дозаповнення метаданих петицій Кабінету Міністрів через API.
Використовує офіційний JSON API: https://petition.kmu.gov.ua/api/petitions/[ID]
"""
import requests
import duckdb
import time
import random
import json

API_BASE_URL = "https://petition.kmu.gov.ua/api/petitions/"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json",
    "Referer": "https://petition.kmu.gov.ua/"
}
DB_FILE = "petitions.duckdb"

def polite_sleep(iteration):
    # API працює швидше, тому затримки можна зробити меншими
    time.sleep(random.uniform(0.3, 0.7))
    if iteration % 100 == 0:
        extra = random.uniform(2.0, 4.0)
        print(f"⏳ Пауза {extra:.1f} с...")
        time.sleep(extra)

def fetch_cabinet_data(pet_id):
    url = f"{API_BASE_URL}{pet_id}"
    try:
        # Для Кабміну обов'язково передавати Referer, інакше буде 400 Bad Request
        headers = HEADERS.copy()
        headers["Referer"] = f"https://petition.kmu.gov.ua/kmu/petition/{pet_id}"
        
        resp = requests.get(url, headers=headers, timeout=10)
        if resp.status_code != 200:
            return None
        
        data = resp.json()
        
        # Мапінг статусів (Cabinet -> Наша база)
        # Кабмін використовує: Unsupported, Approved, Answered, Supported
        status_map = {
            'Unsupported': 'Не підтримано',
            'Answered': 'З відповіддю',
            'Approved': 'На розгляді',
            'Supported': 'Триває збір підписів'
        }
        raw_status = data.get('status', 'Unknown')
        
        return {
            'author': data.get('author'),
            'text_length': len(data.get('content', '')) if data.get('content') else 0,
            'votes': data.get('signaturesNumber', 0),
            'status': status_map.get(raw_status, raw_status),
            'has_answer': data.get('answer') is not None or data.get('answeredAt') is not None
        }
    except Exception as e:
        print(f"💥 Помилка на ID {pet_id}: {e}")
        return None

def run_fix():
    print("="*70)
    print("🚀 CABINET API BACKFILL")
    print("="*70)

    con = duckdb.connect(DB_FILE)
    
    # Знаходимо всі ID Кабміну, де порожній автор
    ids_to_fix = con.execute("""
        SELECT external_id FROM petitions 
        WHERE source = 'cabinet' AND author IS NULL
    """).fetchall()
    
    ids_to_fix = [row[0] for row in ids_to_fix]
    total = len(ids_to_fix)
    
    if total == 0:
        print("✅ Немає петицій Кабміну для оновлення.")
        con.close()
        return

    print(f"📋 Знайдено {total} петицій Кабміну для дозаповнення.")
    
    stats = {'checked': 0, 'updated': 0, 'skipped': 0}
    
    for pet_id in ids_to_fix:
        stats['checked'] += 1
        
        data = fetch_cabinet_data(pet_id)
        
        if not data:
            stats['skipped'] += 1
        else:
            # Оновлюємо тільки потрібні поля
            con.execute("""
                UPDATE petitions 
                SET author = ?, text_length = ?, votes = ?, status = ?, has_answer = ?
                WHERE source = 'cabinet' AND external_id = ?
            """, [data['author'], data['text_length'], data['votes'], data['status'], data['has_answer'], pet_id])
            stats['updated'] += 1
        
        if stats['checked'] % 50 == 0:
            print(f"[{stats['checked']}/{total}] Оновлено: {stats['updated']} | Пропущено: {stats['skipped']}")
        
        polite_sleep(stats['checked'])

    con.close()
    print(f"\n✅ ГОТОВО! Оновлено: {stats['updated']} петицій Кабміну.")

if __name__ == "__main__":
    run_fix()
