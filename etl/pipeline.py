import duckdb
import json
import time
from scraper_president import scrape_president_petitions
from scraper_cabinet import fetch_cabinet_petitions

DB_FILE = 'petitions.duckdb'

def init_db(con):
    """
    Initializes the database schema if it doesn't exist.
    """
    print("Initializing database...")
    con.execute("""
        CREATE SEQUENCE IF NOT EXISTS petition_id_seq;
        CREATE TABLE IF NOT EXISTS petitions (
            internal_id INTEGER DEFAULT nextval('petition_id_seq'),
            source VARCHAR,
            external_id VARCHAR,
            number VARCHAR,
            title VARCHAR,
            date VARCHAR, 
            status VARCHAR,
            votes INTEGER,
            url VARCHAR,
            crawled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (source, external_id)
        );
    """)

def save_to_db(con, petitions):
    """
    Inserts or updates petitions in DuckDB.
    """
    if not petitions:
        print("No petitions to save.")
        return

    print(f"Saving {len(petitions)} petitions to DB...")
    
    # We use INSERT OR REPLACE (Upsert) logic. 
    # In DuckDB standard SQL, we can use INSERT OR REPLACE INTO or ON CONFLICT
    
    # Let's perform bulk insert via appender or executemany.
    # For simplicity and upsert behavior, we'll loop or use a temp table.
    # Using 'INSERT OR REPLACE INTO' is supported in DuckDB.
    
    for p in petitions:
        con.execute("""
            INSERT OR REPLACE INTO petitions (source, external_id, number, title, date, status, votes, url)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            p['source'], 
            p['id'], 
            p['number'], 
            p['title'], 
            p['date'], 
            p['status'], 
            p['votes'], 
            p['url']
        ))
    
    print("Saved successfully.")

def export_analytics(con, growth_stats=[]):
    """
    Calculates stats and saves to src/analytics_data.json
    """
    print("📊 Generating Analytics JSON...")
    
    # 1. Totals
    totals = con.execute("SELECT source, count(*) FROM petitions GROUP BY source").fetchall()
    total_map = {row[0]: row[1] for row in totals}
    
    # 2. Top 10 President
    top_pres = con.execute("""
        SELECT number, title, votes, url, date, status 
        FROM petitions WHERE source='president' 
        ORDER BY votes DESC LIMIT 10
    """).fetchall()
    
    # 3. Top 10 Cabinet
    top_cab = con.execute("""
        SELECT number, title, votes, url, date, status 
        FROM petitions WHERE source='cabinet' 
        ORDER BY votes DESC LIMIT 10
    """).fetchall()

    # Format for JSON
    def fmt(rows):
        return [{
            "number": r[0], "title": r[1], "votes": r[2], 
            "url": r[3], "date": r[4], "status": r[5]
        } for r in rows]

    # Sort growth stats by delta descending
    growth_stats.sort(key=lambda x: x['delta'], reverse=True)

    data = {
        "totals": total_map,
        "top_president": fmt(top_pres),
        "top_cabinet": fmt(top_cab),
        "trending": growth_stats[:10], # Top 10 growers
        "generated_at": time.strftime("%Y-%m-%d %H:%M:%S")
    }
    
    with open('src/analytics_data.json', 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    
    print(f"✅ Saved to src/analytics_data.json (Trends: {len(growth_stats)})")

def run_pipeline():
    # 1. Connect to DB
    con = duckdb.connect(DB_FILE)
    init_db(con)

    # 2. Scrape President (Source A)
    # Strategy: Scrape ALL active pages (approx 28) to update vote counts for every running petition
    print("\n--- Starting President Scraper (Full Active Update) ---")
    
    # 1. Load existing votes to calculate delta
    existing_votes = con.execute("SELECT external_id, votes FROM petitions WHERE source='president'").fetchall()
    vote_map = {row[0]: row[1] for row in existing_votes}
    
    # 2. Scrape fresh data
    pres_data = scrape_president_petitions(max_pages=30, status="active")
    
    # 3. Calculate growth and enrich data
    growth_stats = []
    for p in pres_data:
        str_id = str(p['id'])
        old_votes = vote_map.get(str_id, 0)
        new_votes = p['votes']
        delta = new_votes - old_votes
        
        # Only meaningful growth
        if delta > 0:
            growth_stats.append({
                "title": p['title'],
                "delta": delta,
                "total": new_votes,
                "url": p['url']
            })

    save_to_db(con, pres_data)

    # 3. Scrape Cabinet (Source B)
    # Cabinet API returns "most recent" by default. 
    # To get updates on older ones we might need a different strategy, but their API is fast.
    print("\n--- Starting Cabinet Scraper ---")
    cab_data = fetch_cabinet_petitions()
    save_to_db(con, cab_data)

    # 4. Export Analytics (Pass growth stats)
    export_analytics(con, growth_stats)

    con.close()

if __name__ == "__main__":
    run_pipeline()
