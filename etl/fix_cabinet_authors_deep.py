"""
Скрипт для глибокого вилучення авторів петицій Кабміну.
Якщо поле 'author' у петиції порожнє (null), скрипт бере ім'я зі списку підписантів 
(останній підписант у системі Кабміну зазвичай є автором).
"""
import requests
import duckdb
import time
import random

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json",
    "Referer": "https://petition.kmu.gov.ua/"
}
DB_FILE = "petitions.duckdb"

def fetch_author_deep(pet_id, votes_count):
    # У Кабміну автор - це останній підписант. 
    # Щоб отримати його, ми просимо останню сторінку підписів.
    # Оскільки ми не знаємо точно к-ть сторінок (limit=1), 
    # ми просто беремо останній запис з загальної кількості.
    if votes_count == 0:
        return "Невідомий автор"
        
    url = f"https://petition.kmu.gov.ua/api/petitions/{pet_id}/signatories?page=1&limit=1"
    headers = HEADERS.copy()
    headers["Referer"] = f"https://petition.kmu.gov.ua/kmu/petition/{pet_id}"
    
    try:
        resp = requests.get(url, headers=headers, timeout=10)
        if resp.status_code != 200:
            return None
            
        data = resp.json()
        rows = data.get("rows", [])
        if not rows:
            return None
            
        # У Кабміну підписант - це об'єкт з firstName, lastName, patronymic
        s = rows[0].get("signatory", {})
        parts = [s.get("lastName"), s.get("firstName"), s.get("patronymic")]
        author_name = " ".join([p for p in parts if p])
        return author_name if author_name.strip() else "Невідомий автор"
        
    except Exception as e:
        print(f"💥 Помилка на ID {pet_id}: {e}")
        return None

def run_deep_fix():
    print("🚀 ГЛИБОКЕ ОНОВЛЕННЯ АВТОРІВ КАБМІНУ")
    print("="*50)
    
    con = duckdb.connect(DB_FILE)
    
    # Знаходимо петиції Кабміну без автора
    ids_to_fix = con.execute("""
        SELECT external_id, votes FROM petitions 
        WHERE source = 'cabinet' AND author IS NULL
    """).fetchall()
    
    total = len(ids_to_fix)
    print(f"📋 Потрібно оновити: {total} записів.")
    
    if total == 0:
        con.close()
        return

    updated = 0
    for i, (pet_id, votes) in enumerate(ids_to_fix):
        author = fetch_author_deep(pet_id, votes)
        if author:
            con.execute("UPDATE petitions SET author = ? WHERE source = 'cabinet' AND external_id = ?", [author, pet_id])
            updated += 1
        
        if (i+1) % 50 == 0:
            print(f"[{i+1}/{total}] Оновлено авторів: {updated}")
            
        # Ввічлива пауза
        time.sleep(random.uniform(0.1, 0.3))

    con.close()
    print(f"\n✅ ГОТОВО! Оновлено авторів для {updated} петицій.")

if __name__ == "__main__":
    run_deep_fix()
