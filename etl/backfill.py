import duckdb
from scraper_president import scrape_president_petitions
from pipeline import save_to_db, DB_FILE

def run_backfill():
    print("🚀 Starting Comprehensive Historical Backfill...")
    statuses = ["active", "in_process", "processed", "archive"]
    
    con = duckdb.connect(DB_FILE)
    
    for status in statuses:
        print(f"\n>>> Processing status: {status.upper()}")
        print("Target: 30 pages (approx 600 items)")
        
        # Scrape 30 pages per status (covers the ~28 page limit)
        data = scrape_president_petitions(max_pages=30, status=status)
        
        if data:
            save_to_db(con, data)
            print(f"✅ Saved {len(data)} records for '{status}'.")
        else:
            print(f"⚠️ No data collected for '{status}'.")
            
    con.close()
    print("\n🏁 All backfills complete.")

if __name__ == "__main__":
    run_backfill()
