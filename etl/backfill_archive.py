"""
Backfill script для заповнення бази даних архівними петиціями.

Логіка:
- Якщо запис НОВИЙ → INSERT
- Якщо запис ІСНУЄ → UPDATE тільки NULL поля (не перезаписуємо існуючі дані)

Оптимізація:
- Завантажуємо всі existing IDs в пам'ять один раз (швидко)
- Батчування запитів (20 записів за раз)
- Прогрес-бар

Використання:
    python3 etl/backfill_archive.py --test        # Тільки ID 1-100
    python3 etl/backfill_archive.py --start 1000 --end 10000
    python3 etl/backfill_archive.py --full        # Весь діапазон 1-200000
"""
import requests
from bs4 import BeautifulSoup
import duckdb
import time
import random
import argparse
from datetime import datetime

BASE_URL = "https://petition.president.gov.ua/petition/"
HEADERS = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"}
DB_FILE = "petitions.duckdb"

def extract_petition_data(pet_id):
    """Витягує всі доступні дані з петиції"""
    url = f"{BASE_URL}{pet_id}"
    
    try:
        resp = requests.get(url, headers=HEADERS, timeout=10)
        
        if resp.status_code != 200:
            return None
            
        # Check for 404
        if "404" in resp.text or "не існує" in resp.text or "Redirecting" in resp.text:
            return None
            
        soup = BeautifulSoup(resp.text, 'html.parser')
        
        # Title (критично)
        h1 = soup.find('h1')
        if not h1:
            return None
        
        data = {
            'source': 'president',
            'id': str(pet_id),
            'title': h1.get_text(strip=True)
        }
        
        # Number
        num_tag = soup.find(class_='pet_number')
        data['number'] = num_tag.get_text(strip=True) if num_tag else None
        
        # Date + Author
        date_tags = soup.find_all(class_='pet_date')
        data['author'] = None
        data['date'] = None
        
        for dt in date_tags:
            text = dt.get_text(strip=True)
            if "Автор" in text or "ініціатор" in text:
                if ":" in text:
                    data['author'] = text.split(":", 1)[1].strip()
                else:
                    data['author'] = text.replace("Автор (ініціатор)", "").strip()
            elif "Дата оприлюднення" in text:
                if ":" in text:
                    data['date'] = text.split(":", 1)[1].strip()
                else:
                    data['date'] = text.replace("Дата оприлюднення", "").strip()
        
        # Status
        data['status'] = "Unknown"
        if soup.find(string=lambda t: t and "Архів" in t):
            data['status'] = "Архів"
        elif soup.find(string=lambda t: t and "З відповіддю" in t):
            data['status'] = "З відповіддю"
        elif soup.find(string=lambda t: t and "На розгляді" in t):
            data['status'] = "На розгляді"
        elif soup.find(string=lambda t: t and "Триває збір підписів" in t):
            data['status'] = "Триває збір підписів"
        
        # Votes (ВАЖЛИВО!)
        votes_graph = soup.find(class_='petition_votes_graph')
        if votes_graph:
            try:
                data['votes'] = int(votes_graph.get('data-votes', 0))
            except:
                data['votes'] = None
        else:
            data['votes'] = None
        
        # URL
        data['url'] = url
        
        # Text length
        article = soup.find(class_='article')
        data['text_length'] = len(article.get_text()) if article else None
        
        # Has answer
        answer_tab = soup.find(string=lambda t: t and "Відповідь на петицію" in t)
        data['has_answer'] = answer_tab is not None
        
        return data
        
    except Exception as e:
        print(f"  💥 Error scraping {pet_id}: {e}")
        return None


def load_existing_ids(con):
    """Завантажує всі existing petition IDs в пам'ять (швидко)"""
    result = con.execute("""
        SELECT external_id FROM petitions WHERE source='president'
    """).fetchall()
    return set(row[0] for row in result)


def insert_new(con, petition):
    """INSERT нової петиції"""
    con.execute("""
        INSERT INTO petitions (source, external_id, number, title, date, status, votes, url, author, text_length, has_answer)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        petition['source'],
        petition['id'],
        petition.get('number'),
        petition.get('title'),
        petition.get('date'),
        petition.get('status'),
        petition.get('votes'),
        petition.get('url'),
        petition.get('author'),
        petition.get('text_length'),
        petition.get('has_answer')
    ))


def update_existing(con, petition):
    """UPDATE існуючої петиції (тільки NULL поля)"""
    # Спочатку дізнаємося які поля NULL
    existing = con.execute("""
        SELECT number, title, date, status, votes, url, author, text_length, has_answer
        FROM petitions
        WHERE source=? AND external_id=?
    """, (petition['source'], petition['id'])).fetchone()
    
    if not existing:
        return  # Запис зник, скіп
    
    # UPDATE тільки NULL поля
    updates = []
    params = []
    
    fields = ['number', 'title', 'date', 'status', 'votes', 'url', 'author', 'text_length', 'has_answer']
    for i, field in enumerate(fields):
        if existing[i] is None and petition.get(field) is not None:
            updates.append(f"{field} = ?")
            params.append(petition.get(field))
    
    if updates:
        sql = f"UPDATE petitions SET {', '.join(updates)} WHERE source=? AND external_id=?"
        params.extend([petition['source'], petition['id']])
        con.execute(sql, params)


def backfill(start_id, end_id, test_mode=False):
    """Головна функція backfill"""
    
    print("="*70)
    print("🚀 BACKFILL ARCHIVE PETITIONS")
    print("="*70)
    print(f"Діапазон: ID {start_id} → {end_id}")
    print(f"Режим: {'TEST (перші 100)' if test_mode else 'PRODUCTION'}")
    print(f"Час старту: {datetime.now().strftime('%H:%M:%S')}")
    print("="*70)
    
    con = duckdb.connect(DB_FILE)
    
    # Завантажуємо existing IDs (один раз!)
    print("\n📥 Завантаження existing IDs...")
    existing_ids = load_existing_ids(con)
    print(f"✅ Знайдено {len(existing_ids)} існуючих записів в БД")
    
    # Статистика
    stats = {
        'checked': 0,
        'found': 0,
        'inserted': 0,
        'updated': 0,
        'skipped_404': 0
    }
    
    batch = []
    
    print(f"\n🔍 Починаємо сканування...\n")
    
    for pet_id in range(start_id, end_id + 1):
        stats['checked'] += 1

        # Пропускаємо ID, які вже є в БД
        if str(pet_id) in existing_ids:
            continue
        
        # Progress every 10
        if stats['checked'] % 10 == 0:
            elapsed = stats['checked']
            found_rate = (stats['found'] / stats['checked']) * 100 if stats['checked'] > 0 else 0
            print(f"[{stats['checked']}/{end_id - start_id + 1}] "
                  f"Знайдено: {stats['found']} ({found_rate:.1f}%) | "
                  f"Нових: {stats['inserted']} | Оновлених: {stats['updated']}")
        
        # Scrape
        data = extract_petition_data(pet_id)
        
        if not data:
            stats['skipped_404'] += 1
            continue
        
        stats['found'] += 1
        
        # Визначаємо INSERT vs UPDATE
        if data['id'] in existing_ids:
            update_existing(con, data)
            stats['updated'] += 1
        else:
            insert_new(con, data)
            stats['inserted'] += 1
            existing_ids.add(data['id'])  # Додаємо до кешу
        
        # Polite delay
        time.sleep(random.uniform(0.3, 0.7))
    
    con.close()
    
    # Final report
    print("\n" + "="*70)
    print("✅ ЗАВЕРШЕНО!")
    print("="*70)
    print(f"Перевірено ID:     {stats['checked']}")
    print(f"Знайдено валідних: {stats['found']} ({stats['found']/stats['checked']*100:.1f}%)")
    print(f"Пропущено 404:     {stats['skipped_404']}")
    print(f"Нових записів:     {stats['inserted']}")
    print(f"Оновлених записів: {stats['updated']}")
    print(f"Час завершення:    {datetime.now().strftime('%H:%M:%S')}")
    print("="*70)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Backfill archive petitions')
    parser.add_argument('--test', action='store_true', help='Test mode: only ID 1-100')
    parser.add_argument('--start', type=int, default=1000, help='Start ID')
    parser.add_argument('--end', type=int, default=200000, help='End ID')
    parser.add_argument('--full', action='store_true', help='Full range 1-200000')
    
    args = parser.parse_args()
    
    if args.test:
        backfill(1, 100, test_mode=True)
    elif args.full:
        backfill(1, 200000)
    else:
        backfill(args.start, args.end)
