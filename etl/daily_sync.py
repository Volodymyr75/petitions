
import duckdb
import requests
import time
import json
import os
from datetime import datetime, date
from scraper_detail import fetch_petition_detail, normalize_date
from scraper_cabinet import fetch_cabinet_petitions
from pipeline import export_analytics

# --- CONFIG ---
# Get project root (parent of etl/)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_FILE = os.path.join(BASE_DIR, 'petitions.duckdb')

def get_db_connection():
    return duckdb.connect(DB_FILE)

def sync_president_updates(con, today_str):
    """
    Updates petitions that are currently active ('Триває збір підписів').
    Tracks vote history and status changes.
    """
    print("\n--- 1. President Updates (Active) ---")
    
    # Get active petitions
    active_ids = con.execute("""
        SELECT external_id, votes, status 
        FROM petitions 
        WHERE source='president' AND status='Триває збір підписів'
    """).fetchall()
    
    print(f"Checking {len(active_ids)} active petitions...")
    
    updates_count = 0
    votes_delta_sum = 0
    status_changes = []
    growth_stats = []
    
    for row in active_ids:
        pet_id, old_votes, old_status = row[0], row[1], row[2]
        
        # Fetch fresh data
        data = fetch_petition_detail(pet_id)
        
        if not data:
            continue
            
        if 'error' in data:
            if data['error'] == 404:
                print(f"⚠️ ID {pet_id} not found (404). Marking as 'Not Found'.")
                con.execute("UPDATE petitions SET status='Not Found', updated_at=CURRENT_TIMESTAMP WHERE source='president' AND external_id=?", [pet_id])
                status_changes.append({"id": pet_id, "from": old_status, "to": "Not Found"})
            elif data['error'] in (429, 503):
                 print(f"Skipping {pet_id} due to rate limit/error.")
            continue

        # Calculate delta
        new_votes = data.get('votes', 0)
        
        # SAFETY CHECK: If votes dropped to 0 from something, it's likely a scrape fail (unless active -> archive, but even then votes usually stay)
        # President petitions don't usually lose votes.
        if new_votes == 0 and (old_votes or 0) > 100:
             print(f"⚠️ Suspicious vote drop for {pet_id}: {old_votes} -> {new_votes}. Keeping old votes.")
             new_votes = old_votes
        
        current_status = data.get('status')
        delta = new_votes - (old_votes or 0)
        
        # Update DB
        con.execute("""
            UPDATE petitions 
            SET votes=?, votes_previous=?, status=?, text_length=?, updated_at=CURRENT_TIMESTAMP
            WHERE source='president' AND external_id=?
        """, (new_votes, old_votes, current_status, data.get('text_length', 0), pet_id))
        
        # Add to history
        con.execute("""
            INSERT OR REPLACE INTO votes_history (petition_id, source, date, votes)
            VALUES (?, ?, ?, ?)
        """, (pet_id, 'president', today_str, new_votes))

        updates_count += 1
        votes_delta_sum += delta
        
        if delta > 0:
            growth_stats.append({
                "title": data['title'],
                "delta": delta,
                "total": new_votes,
                "url": data['url']
            })
        
        if current_status != old_status:
            print(f"🔄 Status change for {pet_id}: {old_status} -> {current_status}")
            status_changes.append({"id": pet_id, "from": old_status, "to": current_status})
            
        time.sleep(0.5) # Be polite
        
    print(f"✅ Updated: {updates_count}. Total Vote Delta: {votes_delta_sum}")
    return votes_delta_sum, status_changes, growth_stats

def sync_president_new(con, today_str):
    """
    Finds new petitions by scanning the first few pages of the 'active' list.
    This is more robust than ID-range as site IDs are not strictly sequential.
    """
    print("\n--- 2. President New Petitions (Discovery) ---")
    
    new_count = 0
    new_petitions_list = []
    processed_ids = set()
    
    # We scan up to 5 pages. Usually 1-2 is enough if run daily.
    for page in range(1, 6):
        url = f"https://petition.president.gov.ua/?status=active&sort=date&order=desc&page={page}"
        print(f"Scanning page {page}...")
        
        try:
            resp = requests.get(url, timeout=15)
            if resp.status_code != 200:
                print(f"⚠️ Error {resp.status_code} fetching page {page}")
                break
                
            import re
            found_ids = re.findall(r'/petition/(\d+)', resp.text)
            if not found_ids:
                print(f"No IDs found on page {page}")
                break
            
            page_new_count = 0
            for s_id in found_ids:
                if s_id in processed_ids: continue
                processed_ids.add(s_id)
                
                # Check DB
                exists = con.execute("SELECT 1 FROM petitions WHERE source='president' AND external_id=?", [s_id]).fetchone()
                if exists:
                    continue
                
                # It's new!
                pet_id = int(s_id)
                data = fetch_petition_detail(pet_id)
                
                if data and 'error' not in data:
                    print(f"✨ Found NEW: {s_id} - {data['title'][:40]}...")
                    
                    date_norm = data.get('date_normalized')
                    con.execute("""
                        INSERT INTO petitions (source, external_id, number, title, date, status, votes, url, author, text_length, has_answer, date_normalized, crawled_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                    """, ('president', s_id, data['number'], data['title'], data['date'], data['status'], data['votes'], data['url'], data['author'], data['text_length'], data.get('has_answer'), date_norm))
                    
                    con.execute("INSERT OR REPLACE INTO votes_history VALUES (?, ?, ?, ?)", (s_id, 'president', today_str, data['votes']))
                    
                    new_count += 1
                    page_new_count += 1
                    new_petitions_list.append({
                        "title": data['title'],
                        "delta": data['votes'],
                        "total": data['votes'],
                        "url": data['url']
                    })
                    time.sleep(0.8) # Gentle delay
                
            print(f"Page {page}: found {page_new_count} new petitions.")
            
            # Smart Stop: if we found 0 new petitions on a page, we probably reached familiar territory
            if page_new_count == 0 and len(found_ids) > 0:
                print("Stopping discovery: reached already known petitions.")
                break
                
        except Exception as e:
            print(f"Error on page {page}: {e}")
            break
            
    print(f"✅ Discovery complete. Added {new_count} new petitions.")
    return new_count, new_petitions_list

def sync_cabinet(con, today_str):
    """
    Syncs Cabinet petitions via API.
    """
    print("\n--- 3. Cabinet Sync ---")
    data = fetch_cabinet_petitions()
    if not data:
        return 0, 0, []
        
    new_count = 0
    votes_delta = 0
    growth_stats = []
    
    # Get existing map
    existing = con.execute("SELECT external_id, votes FROM petitions WHERE source='cabinet'").fetchall()
    vote_map = {row[0]: row[1] for row in existing}
    
    for p in data:
        p_id = p['id']
        old_votes = vote_map.get(p_id)
        new_votes = p['votes']
        
        if old_votes is None:
            # New
            new_count += 1
            con.execute("""
                INSERT OR REPLACE INTO petitions (source, external_id, number, title, date, status, votes, url, author, text_length, has_answer, date_normalized, crawled_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """, ('cabinet', p_id, p['number'], p['title'], p['date'], p['status'], new_votes, p['url'], None, None, False, p['date'][:10]))
            
            # Add to growth stats as new
            growth_stats.append({
                "title": p['title'],
                "delta": new_votes,
                "total": new_votes,
                "url": p['url']
            })
            
        else:
            # Update
            if new_votes != old_votes:
                delta = new_votes - old_votes
                votes_delta += delta
                con.execute("UPDATE petitions SET votes=?, votes_previous=?, updated_at=CURRENT_TIMESTAMP WHERE source='cabinet' AND external_id=?", (new_votes, old_votes, p_id))
                
                if delta > 0:
                    growth_stats.append({
                        "title": p['title'],
                        "delta": delta,
                        "total": new_votes,
                        "url": p['url']
                    })
        
        # History
        con.execute("INSERT OR REPLACE INTO votes_history VALUES (?, ?, ?, ?)", (p_id, 'cabinet', today_str, new_votes))

    print(f"✅ Cabinet: {new_count} new, {votes_delta} votes delta.")
    return new_count, votes_delta, growth_stats

def main():
    con = get_db_connection()
    today = date.today()
    today_str = today.isoformat()
    
    print(f"📅 Daily Sync for {today_str}")
    
    # 1. President Updates
    pres_delta, pres_status_changes, pres_growth = sync_president_updates(con, today_str)
    
    # 2. President New
    pres_new, pres_new_list = sync_president_new(con, today_str)
    
    # 3. Cabinet Sync
    cab_new, cab_delta, cab_growth = sync_cabinet(con, today_str)
    
    # 4. Aggregation
    total_delta = pres_delta + cab_delta
    total_new_pres = pres_new
    total_new_cab = cab_new
    
    # Combine growth stats
    all_growth = pres_growth + pres_new_list + cab_growth
    
    print("\n--- 4. Saving Daily Stats ---")
    con.execute("""
        INSERT OR REPLACE INTO daily_stats (date, president_new, cabinet_new, total_votes_delta, status_changes)
        VALUES (?, ?, ?, ?, ?)
    """, (today, total_new_pres, total_new_cab, total_delta, json.dumps(pres_status_changes)))
    
    print(f"Saved stats: +{total_new_pres + total_new_cab} petitions, +{total_delta} votes.")

    print("\n--- 5. Exporting JSON ---")
    export_analytics(con, growth_stats=all_growth) 
    
    con.close()
    print("\n🎉 Daily Sync Complete!")

if __name__ == "__main__":
    main()
