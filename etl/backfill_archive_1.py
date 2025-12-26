import requests
from bs4 import BeautifulSoup
import duckdb
import time
import random
import argparse
from datetime import datetime


BASE_URL = "https://petition.president.gov.ua/petition/"
HEADERS = {
    "User-Agent": "PetitionsResearchBot/1.0 (+contact: your-email@example.com)"
}
DB_FILE = "petitions.duckdb"

# Глобальна сесія
session = requests.Session()


def polite_sleep(iteration):
    """Більш м'які затримки між запитами"""
    # Базова «людська» пауза 2–6 секунд
    time.sleep(random.uniform(0.7, 1.4))

    # Додаткова довга пауза кожні 50 перевірених ID
    if iteration % 50 == 0:
        extra = random.uniform(3.0, 6.0)
        print(f"⏳ Додаткова пауза {extra:.1f} с для зменшення навантаження")
        time.sleep(extra)


def extract_petition_data(pet_id):
    """Витягує всі доступні дані з петиції з ретраями та бекофом"""
    url = f"{BASE_URL}{pet_id}"

    max_attempts = 3
    attempt = 0

    while attempt < max_attempts:
        try:
            resp = session.get(url, headers=HEADERS, timeout=15)

            # Явне обмеження: 429/503 → довша пауза з бекофом
            if resp.status_code in (429, 503):
                wait = 30 * (attempt + 1)
                print(f"⏳ Rate limit {resp.status_code} на ID {pet_id}, sleep {wait}s (attempt {attempt+1})")
                time.sleep(wait)
                attempt += 1
                continue

            if resp.status_code != 200:
                return None

            # Check for 404/redirect/не існує
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

            # Votes
            votes_graph = soup.find(class_='petition_votes_graph')
            if votes_graph:
                try:
                    data['votes'] = int(votes_graph.get('data-votes', 0))
                except Exception:
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
            attempt += 1
            wait = 5 * attempt
            print(f"  💥 Error scraping {pet_id}: {e}, retry in {wait}s (attempt {attempt})")
            time.sleep(wait)

    # Якщо всі спроби провалилися
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
    existing = con.execute("""
        SELECT number, title, date, status, votes, url, author, text_length, has_answer
        FROM petitions
        WHERE source=? AND external_id=?
    """, (petition['source'], petition['id'])).fetchone()

    if not existing:
        return  # Запис зник, скіп

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
    """Головна функція backfill з м'якшим скрейпінгом"""

    print("="*70)
    print("🚀 BACKFILL ARCHIVE PETITIONS (polite)")
    print("="*70)
    print(f"Діапазон: ID {start_id} → {end_id}")
    print(f"Режим: {'TEST (перші 100)' if test_mode else 'PRODUCTION'}")
    print(f"Час старту: {datetime.now().strftime('%H:%M:%S')}")
    print("="*70)

    con = duckdb.connect(DB_FILE)

    print("\n📥 Завантаження existing IDs...")
    existing_ids = load_existing_ids(con)
    print(f"✅ Знайдено {len(existing_ids)} існуючих записів в БД")

    stats = {
        'checked': 0,
        'found': 0,
        'inserted': 0,
        'updated': 0,
        'skipped_404': 0,
        'skipped_existing': 0
    }

    print(f"\n🔍 Починаємо сканування...\n")

    total = end_id - start_id + 1

    for pet_id in range(start_id, end_id + 1):
        stats['checked'] += 1

        # Пропускаємо ID, які вже є в БД (мінусим навантаження)
        if str(pet_id) in existing_ids:
            stats['skipped_existing'] += 1
            # все одно робимо невелику паузу, щоб не «летіти» по циклу надто швидко
            polite_sleep(stats['checked'])
            continue

        # Progress every 10
        if stats['checked'] % 10 == 0:
            found_rate = (stats['found'] / stats['checked']) * 100 if stats['checked'] > 0 else 0
            print(f"[{stats['checked']}/{total}] "
                  f"Знайдено: {stats['found']} ({found_rate:.1f}%) | "
                  f"Нових: {stats['inserted']} | Оновлених: {stats['updated']} | "
                  f"Скіп (existing): {stats['skipped_existing']}")

        # Scrape
        data = extract_petition_data(pet_id)

        if not data:
            stats['skipped_404'] += 1
            polite_sleep(stats['checked'])
            continue

        stats['found'] += 1

        # Визначаємо INSERT vs UPDATE (сюди потрапляють тільки нові ID,
        # але залишаємо гнучкість)
        if data['id'] in existing_ids:
            update_existing(con, data)
            stats['updated'] += 1
        else:
            insert_new(con, data)
            stats['inserted'] += 1
            existing_ids.add(data['id'])

        # «Людська» пауза
        polite_sleep(stats['checked'])

    con.close()

    print("\n" + "="*70)
    print("✅ ЗАВЕРШЕНО!")
    print("="*70)
    print(f"Перевірено ID:          {stats['checked']}")
    print(f"Знайдено валідних:      {stats['found']} ({stats['found']/stats['checked']*100:.1f}%)")
    print(f"Пропущено 404/помилок:  {stats['skipped_404']}")
    print(f"Пропущено existing ID:  {stats['skipped_existing']}")
    print(f"Нових записів:          {stats['inserted']}")
    print(f"Оновлених записів:      {stats['updated']}")
    print(f"Час завершення:         {datetime.now().strftime('%H:%M:%S')}")
    print("="*70)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Backfill archive petitions (polite)')
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
