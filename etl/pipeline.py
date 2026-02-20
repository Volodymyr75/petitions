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
    print("   2. Computing Daily Dynamics...")
    
    # Sort growth stats by delta descending
    growth_stats.sort(key=lambda x: x['delta'], reverse=True)
    
    today_date = time.strftime("%Y-%m-%d")
    
    # Fetch per-source history using votes_history + daily_stats for new petitions
    history_query = """
        WITH daily_totals AS (
            SELECT date, source, SUM(votes) as total_votes
            FROM votes_history
            GROUP BY date, source
        ),
        deltas AS (
            SELECT 
                t.date, 
                t.source,
                t.total_votes - LAG(t.total_votes) OVER (PARTITION BY t.source ORDER BY t.date) as vote_delta
            FROM daily_totals t
        ),
        source_deltas AS (
            SELECT date,
                   COALESCE(SUM(vote_delta) FILTER (WHERE source='president'), 0) as president_delta,
                   COALESCE(SUM(vote_delta) FILTER (WHERE source='cabinet'), 0) as cabinet_delta
            FROM deltas
            WHERE vote_delta IS NOT NULL
            GROUP BY date
        )
        SELECT sd.date, sd.president_delta, sd.cabinet_delta,
               COALESCE(ds.president_new, 0) as pres_new,
               COALESCE(ds.cabinet_new, 0) as cab_new
        FROM source_deltas sd
        LEFT JOIN daily_stats ds ON sd.date = ds.date
        ORDER BY sd.date ASC
    """
    history_rows = con.execute(history_query).fetchall()
    sparkline_data = [{
        "date": str(h[0]),
        "president": max(h[1], 0),
        "cabinet": max(h[2], 0),
        "total": max(h[1], 0) + max(h[2], 0),
        "pres_new": h[3],
        "cab_new": h[4]
    } for h in history_rows]

    daily_data = {
        "new_petitions": 0, 
        "votes_added": sum(g['delta'] for g in growth_stats),
        "biggest_movers": growth_stats[:5],
        "history": sparkline_data,
        "status_changes": [],
        "last_sync_date": None
    }
    
    # Try today's stats first, then fall back to LATEST available row
    current_stats = con.execute(
        "SELECT president_new + cabinet_new, total_votes_delta, date FROM daily_stats WHERE date = ?", 
        [today_date]
    ).fetchone()
    
    if not current_stats:
        # Fallback: use the most recent daily_stats entry
        current_stats = con.execute(
            "SELECT president_new + cabinet_new, total_votes_delta, date FROM daily_stats ORDER BY date DESC LIMIT 1"
        ).fetchone()
    
    if current_stats:
        daily_data["new_petitions"] = current_stats[0]
        daily_data["votes_added"] = current_stats[1]
        daily_data["last_sync_date"] = str(current_stats[2])
    
    # Fallback for "Biggest Movers" if runtime stats are empty
    if not growth_stats:
        print("   ⚠️ growth_stats is empty. Fetching fallback data from DB...")
            
        # Fetch Biggest Movers from petitions table
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

    # 3.4 Status Distribution (per source)
    # Normalize Cabinet English statuses to Ukrainian equivalents
    print("   3.4 Status Distribution...")
    status_dist_query = """
        SELECT 
            CASE status
                WHEN 'Unsupported' THEN 'Архів'
                WHEN 'Approved' THEN 'На розгляді'
                WHEN 'Answered' THEN 'З відповіддю'
                WHEN 'Supported' THEN 'Збір підписів'
                WHEN 'Триває збір підписів' THEN 'Збір підписів'
                WHEN 'Не підтримано' THEN 'Архів'
                ELSE status
            END as unified_status,
            source, 
            COUNT(*) as count
        FROM petitions
        WHERE status IS NOT NULL AND status != 'Unknown'
        GROUP BY unified_status, source
        ORDER BY count DESC
    """
    status_rows = con.execute(status_dist_query).fetchall()
    status_distribution = [{"status": r[0], "source": r[1], "count": r[2]} for r in status_rows]

    # 3.5 Top Authors by total votes
    print("   3.5 Top Authors...")
    authors_query = """
        SELECT author, 
               COUNT(*) as petition_count,
               SUM(votes) as total_votes,
               MAX(votes) as max_votes,
               ROUND(AVG(votes), 0) as avg_votes
        FROM petitions
        WHERE author IS NOT NULL AND author != '' AND LENGTH(author) > 2
        GROUP BY author
        ORDER BY total_votes DESC
        LIMIT 10
    """
    authors_rows = con.execute(authors_query).fetchall()
    top_authors = [{
        "author": r[0], "petitions": r[1], "total_votes": r[2],
        "max_votes": r[3], "avg_votes": int(r[4]) if r[4] else 0
    } for r in authors_rows]

    # 3.6 Category Breakdown (regex-based)
    print("   3.6 Category Breakdown...")
    categories_query = """
        SELECT 
            CASE
                WHEN title ILIKE '%герой%' OR title ILIKE '%героя%' OR title ILIKE '%звання%'
                     OR title ILIKE '%посмертно%' OR title ILIKE '%військов%' OR title ILIKE '%воїн%'
                     OR title ILIKE '%захисни%' OR title ILIKE '%бойов%' THEN 'Військові честі'
                WHEN title ILIKE '%тариф%' OR title ILIKE '%газ%' OR title ILIKE '%енерг%'
                     OR title ILIKE '%подат%' OR title ILIKE '%економ%' OR title ILIKE '%ціна%' THEN 'Економічні'
                WHEN title ILIKE '%екологі%' OR title ILIKE '%навколишн%' OR title ILIKE '%сміт%'
                     OR title ILIKE '%забруднен%' OR title ILIKE '%довкілля%' THEN 'Екологічні'
                WHEN title ILIKE '%пенсі%' OR title ILIKE '%субсиді%' OR title ILIKE '%заробіт%'
                     OR title ILIKE '%житло%' OR title ILIKE '%соціальн%' OR title ILIKE '%медицин%'
                     OR title ILIKE '%здоров%' OR title ILIKE '%освіт%' THEN 'Соціальні'
                WHEN title ILIKE '%міністер%' OR title ILIKE '%реформ%' OR title ILIKE '%закон%'
                     OR title ILIKE '%суд%' OR title ILIKE '%корупці%' OR title ILIKE '%влад%' THEN 'Адміністративні'
                ELSE 'Інші'
            END as category,
            COUNT(*) as count,
            ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM petitions), 1) as percentage
        FROM petitions
        GROUP BY category
        ORDER BY count DESC
    """
    cat_rows = con.execute(categories_query).fetchall()
    categories_data = [{"category": r[0], "count": r[1], "percentage": float(r[2])} for r in cat_rows]

    # 3.7 Vote Velocity (top active petitions, last 7 days from votes_history)
    print("   3.7 Vote Velocity...")
    velocity_query = """
        SELECT vh.petition_id, p.title, p.url,
               MIN(vh.votes) as votes_7d_ago,
               MAX(vh.votes) as votes_now,
               MAX(vh.votes) - MIN(vh.votes) as growth_7d,
               COUNT(DISTINCT vh.date) as days_tracked
        FROM votes_history vh
        JOIN petitions p ON vh.petition_id = p.external_id AND vh.source = p.source
        WHERE vh.date >= CURRENT_DATE - INTERVAL '7 days'
          AND p.status = 'Триває збір підписів'
        GROUP BY vh.petition_id, p.title, p.url
        HAVING COUNT(DISTINCT vh.date) >= 2
        ORDER BY growth_7d DESC
        LIMIT 10
    """
    try:
        vel_rows = con.execute(velocity_query).fetchall()
        vote_velocity = [{
            "id": r[0], "title": r[1], "url": r[2],
            "votes_start": r[3], "votes_current": r[4],
            "growth_7d": r[5], "days_tracked": r[6],
            "daily_rate": round(r[5] / max(r[6] - 1, 1), 0)
        } for r in vel_rows]
    except Exception as e:
        print(f"   ⚠️ Vote velocity query failed: {e}")
        vote_velocity = []

    # 3.8 Keywords Top-10 from titles
    print("   3.8 Keywords Top-10...")
    # Extract most frequent meaningful words from titles (>= 4 chars to skip prepositions)
    keywords_query = """
        WITH words AS (
            SELECT UNNEST(string_split(LOWER(title), ' ')) as word
            FROM petitions
            WHERE title IS NOT NULL
        )
        SELECT word, COUNT(*) as freq
        FROM words
        WHERE LENGTH(word) >= 4
          AND word NOT IN ('про', 'для', 'від', 'або', 'що', 'який', 'яка', 'яке', 'які',
                           'його', 'його', 'цього', 'того', 'нього', 'неї', 'них',
                           'при', 'під', 'над', 'між', 'через', 'після', 'перед',
                           'також', 'щодо', 'та', 'the', 'and', 'for', 'with', 'this', 'that',
                           'прошу', 'президента', 'україни', 'звання')
        GROUP BY word
        ORDER BY freq DESC
        LIMIT 10
    """
    try:
        kw_rows = con.execute(keywords_query).fetchall()
        keywords_top10 = [{"word": r[0], "count": r[1]} for r in kw_rows]
    except Exception as e:
        print(f"   ⚠️ Keywords query failed: {e}")
        keywords_top10 = []

    # --- PLATFORM COMPARISON ---
    print("   3.9 Platform Comparison...")
    platform_query = """
        SELECT 
            source,
            COUNT(*) as total,
            ROUND(AVG(votes), 0) as avg_votes,
            MEDIAN(votes) as median_votes,
            ROUND(COUNT(*) FILTER (WHERE votes >= 25000) * 100.0 / COUNT(*), 2) as success_rate,
            ROUND(COUNT(*) FILTER (WHERE status IN ('З відповіддю', 'Answered')) * 100.0 / COUNT(*), 2) as response_rate
        FROM petitions
        GROUP BY source
    """
    plat_rows = con.execute(platform_query).fetchall()
    platform_comparison = [{
        "source": r[0], "total": r[1], "avg_votes": int(r[2]) if r[2] else 0,
        "median_votes": r[3], "success_rate": float(r[4]), "response_rate": float(r[5])
    } for r in plat_rows]

    # --- AUTO-INSIGHTS ---
    print("   3.10 Generating Insights...")
    insights = []
    
    # Insight 1: Military petition dominance
    military_cat = next((c for c in categories_data if c["category"] == "Військові честі"), None)
    if military_cat:
        insights.append({
            "emoji": "⚔️",
            "text": f"{military_cat['percentage']}% of all petitions are military honor requests, reflecting the ongoing war impact.",
            "type": "military_dominance"
        })
    
    # Insight 2: Viral rarity
    viral_count = hist_map.get('25k+', 0)
    viral_pct = round(viral_count * 100.0 / max(ov[0], 1), 1)
    insights.append({
        "emoji": "🦄",
        "text": f"Only {viral_pct}% of petitions reach the 25,000 signature threshold. Getting viral is exceptionally rare.",
        "type": "viral_rarity"
    })
    
    # Insight 3: Median engagement
    insights.append({
        "emoji": "📊",
        "text": f"The median petition receives only {ov[4]} votes — half of all petitions get less than this.",
        "type": "median_engagement"
    })
    
    # Insight 4: Response rate
    insights.append({
        "emoji": "📬",
        "text": f"Only {ov[5]}% of petitions receive an official response — the vast majority go unanswered.",
        "type": "response_rate"
    })
    
    # Insight 5: Platform scale difference
    pres_data_plat = next((p for p in platform_comparison if p["source"] == "president"), None)
    cab_data_plat = next((p for p in platform_comparison if p["source"] == "cabinet"), None)
    if pres_data_plat and cab_data_plat:
        ratio = round(pres_data_plat["total"] / max(cab_data_plat["total"], 1))
        insights.append({
            "emoji": "🏛️",
            "text": f"Presidential portal has {ratio}x more petitions than Cabinet, but Cabinet petitions average {cab_data_plat['avg_votes']} votes vs {pres_data_plat['avg_votes']}.",
            "type": "platform_comparison"
        })

    # --- DATA SPAN ---
    span_query = """
        SELECT MIN(date_normalized), MAX(date_normalized)
        FROM petitions WHERE date_normalized IS NOT NULL
    """
    span_row = con.execute(span_query).fetchone()
    data_span_start = str(span_row[0])[:4] if span_row[0] else "2015"
    data_span_end = str(span_row[1])[:4] if span_row[1] else "2026"

    analytics_data = {
        "histogram": histogram_data,
        "timeline": timeline_data,
        "scatter": scatter_data,
        "status_distribution": status_distribution,
        "top_authors": top_authors,
        "categories": categories_data,
        "vote_velocity": vote_velocity,
        "keywords_top10": keywords_top10
    }

    # --- BLOCK 4: PIPELINE INFO ---
    pipeline_data = {
        "last_updated": time.strftime("%Y-%m-%d %H:%M:%S"),
        "db_size_mb": 27, # Approx
        "total_records": ov[0],
        "sources": ["president.gov.ua", "petition.kmu.gov.ua"],
        "data_span": f"{data_span_start}-{data_span_end}",
        "coverage": "~100% of significant petitions"
    }

    # --- FINAL ASSEMBLY ---
    output = {
        "overview": {**overview_data, "platform_comparison": platform_comparison},
        "daily": daily_data,
        "analytics": analytics_data,
        "insights": insights,
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
