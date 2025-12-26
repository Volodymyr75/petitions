import requests
from bs4 import BeautifulSoup
import duckdb
import time
import random
import argparse
from datetime import datetime

BASE_URL = "https://petition.president.gov.ua/petition/"
# Використовуємо реальний User-Agent, щоб сайт не вважав нас підозрілим ботом
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
    "Accept-Language": "uk-UA,uk;q=0.9,en-US;q=0.8,en;q=0.7",
}
DB_FILE = "petitions.duckdb"

# Глобальна сесія для Keep-Alive з'єднань
session = requests.Session()

def polite_sleep(iteration):
    """Пауза між запитами для зменшення навантаження на сервер"""
    time.sleep(random.uniform(0.7, 1.4))
    if iteration % 50 == 0:
        extra = random.uniform(3.0, 6.0)
        print(f"⏳ Додаткова пауза {extra:.1f} с...")
        time.sleep(extra)

def extract_petition_data(pet_id):
    """Витягує дані. Повертає None тільки якщо сторінка реально не існує (404)"""
    url = f"{BASE_URL}{pet_id}"
    max_attempts = 3
    attempt = 0

    while attempt < max_attempts:
        try:
            resp = session.get(url, headers=HEADERS, timeout=15)

            # Якщо сервер повернув 404 — петиції точно немає
            if resp.status_code == 404:
                return None

            # Якщо ліміт запитів або помилка сервера
            if resp.status_code in (429, 503):
                wait = 30 * (attempt + 1)
                print(f"⏳ Rate limit {resp.status_code} на ID {pet_id}, чекаємо {wait}s")
                time.sleep(wait)
                attempt += 1
                continue

            if resp.status_code != 200:
                return None

            soup = BeautifulSoup(resp.text, 'html.parser')
            
            # Головна ознака існуючої петиції — заголовок h1
            h1 = soup.find('h1')
            if not h1:
                return None

            data = {
                'source': 'president',
                'id': str(pet_id),
                'title': h1.get_text(strip=True),
                'url': url
            }

            # Номер петиції
            num_tag = soup.find(class_='pet_number')
            data['number'] = num_tag.get_text(strip=True) if num_tag else None

            # Дата та Автор
            date_tags = soup.find_all(class_='pet_date')
            data['author'] = None
            data['date'] = None
            for dt in date_tags:
                text = dt.get_text(strip=True)
                if "Автор" in text or "ініціатор" in text:
                    data['author'] = text.split(":", 1)[1].strip() if ":" in text else text.replace("Автор (ініціатор)", "").strip()
                elif "Дата оприлюднення" in text:
                    data['date'] = text.split(":", 1)[1].strip() if ":" in text else text.replace("Дата оприлюднення", "").strip()

            # Статус
            data['status'] = "Unknown"
            # Шукаємо статус у тексті сторінки
            page_text = resp.text
            if "Архів" in page_text: data['status'] = "Архів"
            elif "З відповіддю" in page_text: data['status'] = "З відповіддю"
            elif "На розгляді" in page_text: data['status'] = "На розгляді"
            elif "Триває збір підписів" in page_text: data['status'] = "Триває збір підписів"

            # Голоси
            votes_graph = soup.find(class_='petition_votes_graph')
            data['votes'] = int(votes_graph.get('data-votes', 0)) if votes_graph else None

            # Текст та відповідь
            article = soup.find(class_='article')
            data['text_length'] = len(article.get_text()) if article else None
            data['has_answer'] = "Відповідь на петицію" in page_text

            return data

        except Exception as e:
            attempt += 1
            print(f"💥 Помилка на ID {pet_id}: {e}, спроба {attempt}")
            time.sleep(5 * attempt)
    return None

def load_existing_ids(con):
    """Завантажує існуючі ID для швидкої перевірки"""
    result = con.execute("SELECT external_id FROM petitions WHERE source='president'").fetchall()
    return set(row[0] for row in result)

def insert_new(con, petition):
    """Додавання нового запису"""
    con.execute("""
        INSERT INTO petitions (source, external_id, number, title, date, status, votes, url, author, text_length, has_answer)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (petition['source'], petition['id'], petition['number'], petition['title'], 
          petition['date'], petition['status'], petition['votes'], petition['url'], 
          petition['author'], petition['text_length'], petition['has_answer']))

def update_existing(con, petition):
    """Повне оновлення ВСІХ полів існуючого запису"""
    fields = ['number', 'title', 'date', 'status', 'votes', 'url', 'author', 'text_length', 'has_answer']
    set_clause = ", ".join([f"{f} = ?" for f in fields])
    params = [petition.get(f) for f in fields]
    
    # Додаємо параметри для умови WHERE
    sql = f"UPDATE petitions SET {set_clause} WHERE source=? AND external_id=?"
    params.extend([petition['source'], petition['id']])
    con.execute(sql, params)

def backfill(start_id, end_id):
    """Головний цикл оновлення та наповнення"""
    print("="*70)
    print("🚀 PETITION UPDATER & BACKFILL (Safe Mode)")
    print(f"Діапазон: ID {start_id} → {end_id}")
    print("="*70)

    con = duckdb.connect(DB_FILE)
    existing_ids = load_existing_ids(con)
    print(f"✅ В базі вже є {len(existing_ids)} записів.")

    stats = {'checked': 0, 'inserted': 0, 'updated': 0, 'skipped': 0}

    for pet_id in range(start_id, end_id + 1):
        stats['checked'] += 1
        
        # Отримуємо свіжі дані з сайту
        data = extract_petition_data(pet_id)

        if not data:
            stats['skipped'] += 1
        else:
            if data['id'] in existing_ids:
                update_existing(con, data)
                stats['updated'] += 1
            else:
                insert_new(con, data)
                stats['inserted'] += 1
                existing_ids.add(data['id'])

        # Звіт кожні 10 ID
        if stats['checked'] % 10 == 0:
            print(f"[{pet_id}] Оновлено: {stats['updated']} | Нових: {stats['inserted']} | Пропущено (404): {stats['skipped']}")

        polite_sleep(stats['checked'])

    con.close()
    print("\n" + "="*70)
    print(f"✅ ЗАВЕРШЕНО! Оновлено: {stats['updated']}, Додано нових: {stats['inserted']}")
    print("="*70)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--start', type=int, required=True)
    parser.add_argument('--end', type=int, required=True)
    args = parser.parse_args()
    
    backfill(args.start, args.end)