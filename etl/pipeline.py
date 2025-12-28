import duckdb
import json
import time
import os
from scraper_president import scrape_president_petitions
from scraper_cabinet import fetch_cabinet_petitions

# Get project root (parent of etl/)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_FILE = os.path.join(BASE_DIR, 'petitions.duckdb')
JSON_FILE = os.path.join(BASE_DIR, 'src', 'analytics_data.json')

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
            author VARCHAR,
            text_length INTEGER,
            has_answer BOOLEAN,
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
            INSERT OR REPLACE INTO petitions (source, external_id, number, title, date, status, votes, url, author, text_length, has_answer)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            p['source'], 
            p['id'], 
            p['number'], 
            p['title'], 
            p['date'], 
            p['status'], 
            p['votes'], 
            p['url'],
            p.get('author'),
            p.get('text_length'),
            p.get('has_answer')
        ))
    
    print("Saved successfully.")

def export_analytics(con, growth_stats=[]):
    """
    Calculates stats and saves to src/analytics_data.json
    Structure matches the 4-block dashboard design.
    """
    print("📊 Generating Analytics JSON...")
    
    # --- BLOCK 1: OVERVIEW ---
    print("   1. Computing Overview...")
    overview_query = """
        SELECT 
            COUNT(*) as total,
            COUNT(*) FILTER (WHERE source='president') as president_count,
            COUNT(*) FILTER (WHERE source='cabinet') as cabinet_count,
            
            -- Success rate (>= 25k)
            ROUND(COUNT(*) FILTER (WHERE votes >= 25000) * 100.0 / COUNT(*), 2) as success_rate,
            
            -- Median votes
            MEDIAN(votes) as median_votes,
            
            -- Response rate (approximate based on status)
            ROUND(COUNT(*) FILTER (WHERE status IN ('З відповіддю', 'Answered')) * 100.0 / COUNT(*), 2) as response_rate
        FROM petitions
    """
    ov = con.execute(overview_query).fetchone()
    
    overview_data = {
        "total": ov[0],
        "president_count": ov[1],
        "cabinet_count": ov[2],
        "success_rate": float(ov[3]),
        "median_votes": ov[4],
        "response_rate": float(ov[5]),
        "insight": f"Only {ov[3]}% of petitions reach the 25,000 signature threshold. Median votes: {ov[4]}."
    }

    # --- BLOCK 2: DAILY DYNAMICS ---
    # In a real daily run, we would query 'daily_stats'. For now, we use current runtime inputs.
    print("   2. Computing Daily Dynamics...")
    
    # Sort growth stats by delta descending
    growth_stats.sort(key=lambda x: x['delta'], reverse=True)
    
    today_date = time.strftime("%Y-%m-%d")
    
    # Fetch last 7 days history for Sparkline
    history_query = """
        SELECT date, total_votes_delta 
        FROM daily_stats 
        ORDER BY date ASC 
        LIMIT 7
    """
    history_rows = con.execute(history_query).fetchall()
    sparkline_data = [{"date": str(h[0]), "value": h[1]} for h in history_rows]

    daily_data = {
        "new_petitions": 0, # Placeholder until fetched from daily_stats
        "votes_added": sum(g['delta'] for g in growth_stats),
        "biggest_movers": growth_stats[:5],
        "history": sparkline_data,
        "status_changes": [] 
    }
    
    # Try to fill "new_petitions" and "votes_added" from DB if growth_stats is empty (e.g. running generate_json without scrape)
    if not growth_stats:
        print("   ⚠️ growth_stats is empty. Fetching fallback data from DB...")
        
        # 1. Fetch totals from daily_stats
        current_stats = con.execute("SELECT president_new + cabinet_new, total_votes_delta FROM daily_stats WHERE date = ?", [today_date]).fetchone()
        if current_stats:
            daily_data["new_petitions"] = current_stats[0]
            daily_data["votes_added"] = current_stats[1]
            
        # 2. Fetch Biggest Movers from petitions table (votes - votes_previous)
        movers_query = """
            SELECT title, url, (votes - votes_previous) as delta, votes
            FROM petitions 
            WHERE votes_previous IS NOT NULL 
              AND (votes - votes_previous) > 0
            ORDER BY delta DESC 
            LIMIT 5
        """
        movers_rows = con.execute(movers_query).fetchall()
        daily_data["biggest_movers"] = [
            {"title": r[0], "url": r[1], "delta": r[2], "total": r[3]} 
            for r in movers_rows
        ]

    # --- BLOCK 3: DEEP ANALYTICS ---
    print("   3. Computing Deep Analytics...")
    
    # 3.1 Votes Histogram
    # Binning: 0-100, 100-1k, 1k-10k, 10k-25k, 25k+
    hist_query = """
        SELECT 
            CASE 
                WHEN votes < 100 THEN '0-100'
                WHEN votes < 1000 THEN '100-1k'
                WHEN votes < 10000 THEN '1k-10k'
                WHEN votes < 25000 THEN '10k-25k'
                ELSE '25k+' 
            END as bin,
            COUNT(*) as count
        FROM petitions
        GROUP BY bin
    """
    hist_rows = con.execute(hist_query).fetchall()
    # Ensure order
    bin_order = ['0-100', '100-1k', '1k-10k', '10k-25k', '25k+']
    hist_map = {r[0]: r[1] for r in hist_rows}
    histogram_data = [{"bin": b, "count": hist_map.get(b, 0)} for b in bin_order]

    # 3.2 Timeline (Stacked by Month)
    # Using normalized date
    timeline_query = """
        SELECT 
            STRFTIME(date_normalized, '%Y-%m') as month,
            COUNT(*) FILTER (WHERE source='president') as president,
            COUNT(*) FILTER (WHERE source='cabinet') as cabinet
        FROM petitions
        WHERE date_normalized IS NOT NULL
        GROUP BY month
        ORDER BY month ASC
    """
    timeline_rows = con.execute(timeline_query).fetchall()
    timeline_data = [{"month": r[0], "president": r[1], "cabinet": r[2]} for r in timeline_rows]

    # 3.3 Text Length vs Votes (Scatter sample)
    # Take a sample of 300 points for performance
    scatter_query = """
        SELECT text_length, votes, source, 
               CASE WHEN status IN ('З відповіддю', 'Answered') THEN true ELSE false END as has_ans
        FROM petitions
        WHERE text_length IS NOT NULL AND votes > 0
        USING SAMPLE 300
    """
    scatter_rows = con.execute(scatter_query).fetchall()
    scatter_data = [{"x": r[0], "y": r[1], "source": r[2], "has_answer": r[3]} for r in scatter_rows]
    
    analytics_data = {
        "histogram": histogram_data,
        "timeline": timeline_data,
        "scatter": scatter_data
    }

    # --- BLOCK 4: PIPELINE INFO ---
    pipeline_data = {
        "last_updated": time.strftime("%Y-%m-%d %H:%M:%S"),
        "db_size_mb": 27, # Approx
        "total_records": ov[0],
        "sources": ["president.gov.ua", "petition.kmu.gov.ua"]
    }

    # --- FINAL ASSEMBLY ---
    output = {
        "overview": overview_data,
        "daily": daily_data,
        "analytics": analytics_data,
        "pipeline": pipeline_data
    }
    
    with open(JSON_FILE, 'w', encoding='utf-8') as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    
    print(f"✅ Saved to src/analytics_data.json")

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

    # 2.1. Check "Winners Circle" (In Process / Answered)
    # We must scan these to catch petitions that moved from "Active" -> "In Process" -> "Answered"
    # Scrape top 5 pages (approx 100 items) is enough for daily updates
    print("\n--- Checking Status Transitions (Lifecycle Update) ---")
    # Check "Winners" (In Process/Processed) AND "Losers" (Archive)
    # Scrape top 5 pages of each to catch recent movements
    for st in ["in_process", "processed", "archive"]:
        print(f"Scanning '{st}' updates...")
        status_data = scrape_president_petitions(max_pages=5, status=st)
        save_to_db(con, status_data)

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
