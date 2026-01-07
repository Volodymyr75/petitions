import duckdb
from scraper_detail import fetch_petition_detail

def fix_unknown_statuses():
    con = duckdb.connect('petitions.duckdb')
    unknowns = con.execute("SELECT external_id FROM petitions WHERE status='Unknown'").fetchall()
    
    if not unknowns:
        print("✅ Петицій зі статусом Unknown не знайдено.")
        return

    print(f"🔄 Знайдено {len(unknowns)} петицій для виправлення...")
    
    for row in unknowns:
        pet_id = row[0]
        print(f"   Скрапінг ID {pet_id}...")
        data = fetch_petition_detail(pet_id)
        
        if data and 'status' in data and data['status'] != 'Unknown':
            con.execute("UPDATE petitions SET status = ? WHERE external_id = ?", (data['status'], str(pet_id)))
            print(f"      ✅ Новий статус: {data['status']}")
        else:
            print(f"      ⚠️ Не вдалося визначити статус для {pet_id}")
            
    con.close()
    print("\n🎉 Виправлення завершено!")

if __name__ == "__main__":
    fix_unknown_statuses()
