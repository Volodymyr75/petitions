"""
Оптимізований скрипт для дозаповнення метаданих Кабміну.
Завантажує ВСІ петиції одним запитом до API і оновлює базу.
"""
import requests
import duckdb
import json

API_URL = "https://petition.kmu.gov.ua/api/petitions"
HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json",
    "Referer": "https://petition.kmu.gov.ua/"
}
DB_FILE = "petitions.duckdb"

def run_bulk_fix():
    print("🚀 Починаю масове завантаження даних Кабміну...")
    
    try:
        resp = requests.get(API_URL, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        rows = data.get("rows", [])
        print(f"✅ Отримано {len(rows)} записів з API.")
    except Exception as e:
        print(f"❌ Помилка при запиті до API: {e}")
        return

    con = duckdb.connect(DB_FILE)
    
    status_map = {
        'Unsupported': 'Не підтримано',
        'Answered': 'З відповіддю',
        'Approved': 'На розгляді',
        'Supported': 'Триває збір підписів'
    }

    updated_count = 0
    print("⏳ Оновлення бази даних...")
    
    for item in rows:
        pet_id = str(item.get("id"))
        author = item.get("author")
        content = item.get("content", "")
        text_length = len(content) if content else 0
        votes = item.get("signaturesNumber", 0)
        raw_status = item.get("status", "Unknown")
        status = status_map.get(raw_status, raw_status)
        has_answer = item.get('answer') is not None or item.get('answeredAt') is not None
        
        # Оновлюємо тільки ті петиції, які належать Кабміну і вже є в базі
        con.execute("""
            UPDATE petitions 
            SET author = ?, text_length = ?, votes = ?, status = ?, has_answer = ?
            WHERE source = 'cabinet' AND external_id = ?
        """, [author, text_length, votes, status, has_answer, pet_id])
        updated_count += 1

    con.close()
    print(f"✅ ГОТОВО! Оброблено записів: {updated_count}")

if __name__ == "__main__":
    run_bulk_fix()
