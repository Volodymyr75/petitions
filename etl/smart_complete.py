"""
Оптимізований скрипт для 'добивання' бази даних.
Використовує спільний модуль scraper_detail.
"""
import duckdb
import time
import random
import argparse
from scraper_detail import fetch_petition_detail

DB_FILE = "petitions.duckdb"

def polite_sleep(iteration):
    time.sleep(random.uniform(0.7, 1.4))
    if iteration % 50 == 0:
        extra = random.uniform(3.0, 6.0)
        print(f"⏳ Пауза {extra:.1f} с...")
        time.sleep(extra)

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
    
    ids_to_check = range(start_id, end_id + 1)
    print(f"Range to check: {start_id} - {end_id} ({len(ids_to_check)} IDs)")

    for pet_id in ids_to_check:
        stats['checked'] += 1
        s_id = str(pet_id)
        
        # SKIP STRATEGY:
        if s_id in complete_ids:
            continue
            
        # SCRAPE
        data = fetch_petition_detail(pet_id)
        
        if not data or 'error' in data:
            if data and data.get('error') == 404:
                pass # 404 is normal for gaps
            stats['skipped'] += 1
        else:
            if s_id in needs_update_ids:
                # UPDATE
                date_norm = data.get('date_normalized')
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
                date_norm = data.get('date_normalized')
                con.execute("""
                    INSERT INTO petitions (source, external_id, number, title, date, status, votes, url, author, text_length, has_answer, date_normalized)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, ('president', s_id, data['number'], data['title'], data['date'], data['status'], data['votes'], data['url'], data['author'], data['text_length'], data['has_answer'], date_norm))
                stats['inserted'] += 1
        
        if stats['checked'] % 10 == 0:
            print(f"[{pet_id}] Upd: {stats['updated']} | New: {stats['inserted']} | Skip: {stats['skipped']}")
        
        polite_sleep(stats['checked'])

    con.close()
    print(f"\n✅ ГОТОВО! Оновлено: {stats['updated']}, Додано: {stats['inserted']}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--start', type=int, required=True)
    parser.add_argument('--end', type=int, required=True)
    args = parser.parse_args()
    backfill_smart(args.start, args.end)
