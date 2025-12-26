"""
Оптимізований скрипт для 'добивання' бази даних.
Ціль: 
1. Додати петиції, яких взагалі немає в базі (Missing IDs).
2. Оновити петиції, які є в базі, але мають порожні (NULL) поля (author, text_length тощо).
3. Пропускати (не робити запитів) ті петиції, які вже повністю заповнені.
"""
import requests
from bs4 import BeautifulSoup
import duckdb
import time
import random
import argparse
import re
from datetime import datetime

# Ukrainian month names to numbers
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

BASE_URL = "https://petition.president.gov.ua/petition/"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
}
DB_FILE = "petitions.duckdb"

session = requests.Session()

def polite_sleep(iteration):
    time.sleep(random.uniform(0.7, 1.4))
    if iteration % 50 == 0:
        extra = random.uniform(3.0, 6.0)
        print(f"⏳ Пауза {extra:.1f} с...")
        time.sleep(extra)

def extract_petition_data(pet_id, attempt=1, max_attempts=3):
    url = f"{BASE_URL}{pet_id}"
    try:
        resp = session.get(url, headers=HEADERS, timeout=15)
        if resp.status_code == 404:
            return None
        if resp.status_code in (429, 503):
            print(f"⏳ Rate limit {resp.status_code} на ID {pet_id}, чекаємо 30с")
            time.sleep(30)
            return extract_petition_data(pet_id, attempt, max_attempts)
        
        if resp.status_code != 200 or resp.url.endswith('/404'):
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

        data['status'] = "Unknown"
        page_text = resp.text
        
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
    except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
        # Retry logic for timeout/connection errors
        if attempt < max_attempts:
            wait_time = 30 * attempt  # 30s, 60s, 90s
            print(f"⏳ Timeout на ID {pet_id}, спроба {attempt}/{max_attempts}, чекаємо {wait_time}с...")
            time.sleep(wait_time)
            return extract_petition_data(pet_id, attempt + 1, max_attempts)
        else:
            print(f"❌ ID {pet_id}: всі {max_attempts} спроб невдалі, пропускаємо")
            return None
    except Exception as e:
        print(f"💥 Помилка на ID {pet_id}: {e}")
        return None

def get_work_lists(con):
    """Розподіляє ID на три категорії: Повні, Потребують оновлення, Відсутні"""
    # 1. Ті, що вже мають автора (вважаємо їх повними)
    complete_ids = set([str(r[0]) for r in con.execute("SELECT external_id FROM petitions WHERE author IS NOT NULL AND source='president'").fetchall()])
    
    # 2. Ті, що в базі, але без автора (потребують дозаповнення)
    needs_update_ids = set([str(r[0]) for r in con.execute("SELECT external_id FROM petitions WHERE author IS NULL AND source='president'").fetchall()])
    
    return complete_ids, needs_update_ids

def backfill_smart(start_id, end_id):
    con = duckdb.connect(DB_FILE)
    complete_ids, needs_update_ids = get_work_lists(con)
    print(f"✅ В базі {len(complete_ids)} заповнених петицій.")
    print(f"⚠️ {len(needs_update_ids)} петицій потребують дозаповнення.")
    
    stats = {'checked': 0, 'inserted': 0, 'updated': 0, 'skipped': 0}
    
    for pet_id in range(start_id, end_id + 1):
        s_id = str(pet_id)
        stats['checked'] += 1
        
        # СТРАТЕГІЯ ПРОПУСКУ:
        # Якщо петиція вже є в базі І вона повна (має автора) -> ПРОПУСКАЄМО
        if s_id in complete_ids:
            continue
            
        # Якщо ми тут, значить петиції або немає в базі, або вона неповна
        data = extract_petition_data(pet_id)
        
        if not data:
            stats['skipped'] += 1
        else:
            if s_id in needs_update_ids:
                # UPDATE
                date_norm = normalize_date(data.get('date'))
                fields = ['number', 'title', 'date', 'status', 'votes', 'url', 'author', 'text_length', 'has_answer']
                set_clause = ", ".join([f"{f} = ?" for f in fields])
                set_clause += ", date_normalized = ?"
                params = [data.get(f) for f in fields]
                params.append(date_norm)
                params.extend(['president', s_id])
                con.execute(f"UPDATE petitions SET {set_clause} WHERE source=? AND external_id=?", params)
                stats['updated'] += 1
            else:
                # INSERT
                date_norm = normalize_date(data.get('date'))
                con.execute("""
                    INSERT INTO petitions (source, external_id, number, title, date, status, votes, url, author, text_length, has_answer, date_normalized)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, ('president', s_id, data['number'], data['title'], data['date'], data['status'], data['votes'], data['url'], data['author'], data['text_length'], data['has_answer'], date_norm))
                stats['inserted'] += 1
        
        if stats['checked'] % 10 == 0:
            print(f"[{pet_id}] Оновлено: {stats['updated']} | Нових: {stats['inserted']} | Пропущено (404): {stats['skipped']}")
        
        polite_sleep(stats['checked'])

    con.close()
    print(f"\n✅ ГОТОВО! Оновлено: {stats['updated']}, Додано: {stats['inserted']}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--start', type=int, required=True)
    parser.add_argument('--end', type=int, required=True)
    args = parser.parse_args()
    backfill_smart(args.start, args.end)
