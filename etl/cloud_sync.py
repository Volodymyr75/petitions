"""
Cloud Sync Script for MotherDuck
This is a separate script that connects to MotherDuck cloud database.
The original daily_sync.py remains unchanged for local development.

Usage:
    python cloud_sync.py                    # Full sync with validation
    python cloud_sync.py --skip-preflight   # Skip pre-flight (not recommended)
    python cloud_sync.py --dry-run          # Validate only, no changes
"""

import os
import sys
import duckdb
import requests
import time
import json
import argparse
from datetime import datetime, date

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from scraper_detail import fetch_petition_detail, normalize_date
from scraper_cabinet import fetch_cabinet_petitions
from validator import run_preflight_check, run_postsync_validation
from notifier import notify_sync_failure, notify_sync_success, load_env

# --- CONFIG ---
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
JSON_FILE = os.path.join(BASE_DIR, 'src', 'analytics_data.json')


def get_motherduck_connection():
    """Connect to MotherDuck cloud database."""
    load_env()
    token = os.environ.get("MOTHERDUCK_TOKEN")
    
    if not token:
        raise ValueError("MOTHERDUCK_TOKEN not found in environment")
    
    # Connect to MotherDuck
    con = duckdb.connect(f"md:petitions_prod?motherduck_token={token}")
    print("✅ Connected to MotherDuck")
    return con


def create_backup(con):
    """Create a backup table before sync."""
    print("\n📦 Creating backup...")
    con.execute("CREATE OR REPLACE TABLE petitions_backup AS SELECT * FROM petitions")
    con.execute("CREATE OR REPLACE TABLE daily_stats_backup AS SELECT * FROM daily_stats")
    print("✅ Backup created")


def rollback_from_backup(con):
    """Restore from backup in case of failure."""
    print("\n⚠️ Rolling back from backup...")
    try:
        con.execute("DROP TABLE IF EXISTS petitions")
        con.execute("ALTER TABLE petitions_backup RENAME TO petitions")
        con.execute("DROP TABLE IF EXISTS daily_stats")
        con.execute("ALTER TABLE daily_stats_backup RENAME TO daily_stats")
        print("✅ Rollback complete")
    except Exception as e:
        print(f"❌ Rollback failed: {e}")


def cleanup_backup(con):
    """Remove backup tables after successful sync."""
    print("\n🧹 Cleaning up backup...")
    con.execute("DROP TABLE IF EXISTS petitions_backup")
    con.execute("DROP TABLE IF EXISTS daily_stats_backup")
    print("✅ Backup removed")


def sync_president_updates(con, today_str, stats):
    """Updates active petitions."""
    print("\n--- 1. President Updates (Active) ---")
    
    active_ids = con.execute("""
        SELECT external_id, votes, status 
        FROM petitions 
        WHERE source='president' AND status='Триває збір підписів'
    """).fetchall()
    
    print(f"Checking {len(active_ids)} active petitions...")
    stats["total_checked"] = len(active_ids)
    
    updates_count = 0
    votes_delta_sum = 0
    status_changes = []
    growth_stats = []
    errors = 0
    
    for row in active_ids:
        pet_id, old_votes, old_status = row[0], row[1], row[2]
        
        data = fetch_petition_detail(pet_id)
        
        if not data:
            errors += 1
            continue
            
        if 'error' in data:
            if data['error'] == 404:
                con.execute("UPDATE petitions SET status='Not Found', updated_at=CURRENT_TIMESTAMP WHERE source='president' AND external_id=?", [pet_id])
                status_changes.append({"id": pet_id, "from": old_status, "to": "Not Found"})
            errors += 1
            continue

        new_votes = data.get('votes', 0)
        
        # Safety check
        if new_votes == 0 and (old_votes or 0) > 100:
            print(f"⚠️ Suspicious vote drop for {pet_id}: {old_votes} -> {new_votes}. Keeping old votes.")
            new_votes = old_votes
            errors += 1
        
        current_status = data.get('status')
        
        # Check for Unknown status
        if current_status == "Unknown":
            print(f"⚠️ Unknown status for {pet_id}")
            errors += 1
            continue
        
        delta = new_votes - (old_votes or 0)
        
        con.execute("""
            UPDATE petitions 
            SET votes=?, votes_previous=?, status=?, text_length=?, updated_at=CURRENT_TIMESTAMP
            WHERE source='president' AND external_id=?
        """, (new_votes, old_votes, current_status, data.get('text_length', 0), pet_id))
        
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
            
        time.sleep(0.5)
        
    stats["errors"] = errors
    stats["vote_delta"] = votes_delta_sum
    stats["status_changes"] = len(status_changes)
    
    print(f"✅ Updated: {updates_count}. Total Vote Delta: {votes_delta_sum}")
    return votes_delta_sum, status_changes, growth_stats


def sync_president_new(con, today_str, stats):
    """Discovers new petitions from listing pages."""
    print("\n--- 2. President New Petitions (Discovery) ---")
    
    new_count = 0
    new_petitions_list = []
    processed_ids = set()
    
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
                break
            
            page_new_count = 0
            for s_id in found_ids:
                if s_id in processed_ids: continue
                processed_ids.add(s_id)
                
                exists = con.execute("SELECT 1 FROM petitions WHERE source='president' AND external_id=?", [s_id]).fetchone()
                if exists:
                    continue
                
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
                    time.sleep(0.8)
                
            print(f"Page {page}: found {page_new_count} new petitions.")
            
            if page_new_count == 0 and len(found_ids) > 0:
                print("Stopping discovery: reached already known petitions.")
                break
                
        except Exception as e:
            print(f"Error on page {page}: {e}")
            break
    
    stats["new_petitions"] = new_count
    print(f"✅ Discovery complete. Added {new_count} new petitions.")
    return new_count, new_petitions_list


def sync_cabinet(con, today_str, stats):
    """Syncs Cabinet petitions via API."""
    print("\n--- 3. Cabinet Sync ---")
    data = fetch_cabinet_petitions()
    if not data:
        return 0, 0, []
        
    new_count = 0
    votes_delta = 0
    growth_stats = []
    
    existing = con.execute("SELECT external_id, votes FROM petitions WHERE source='cabinet'").fetchall()
    vote_map = {row[0]: row[1] for row in existing}
    
    for p in data:
        p_id = p['id']
        old_votes = vote_map.get(p_id)
        new_votes = p['votes']
        
        if old_votes is None:
            new_count += 1
            con.execute("""
                INSERT OR REPLACE INTO petitions (source, external_id, number, title, date, status, votes, url, author, text_length, has_answer, date_normalized, crawled_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """, ('cabinet', p_id, p['number'], p['title'], p['date'], p['status'], new_votes, p['url'], None, None, False, p['date'][:10]))
            
            growth_stats.append({
                "title": p['title'],
                "delta": new_votes,
                "total": new_votes,
                "url": p['url']
            })
        else:
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
        
        con.execute("INSERT OR REPLACE INTO votes_history VALUES (?, ?, ?, ?)", (p_id, 'cabinet', today_str, new_votes))

    stats["cabinet_new"] = new_count
    stats["vote_delta"] = stats.get("vote_delta", 0) + votes_delta
    
    print(f"✅ Cabinet: {new_count} new, {votes_delta} votes delta.")
    return new_count, votes_delta, growth_stats


def export_analytics_cloud(con, growth_stats=None):
    """Export analytics JSON (simplified version for cloud)."""
    from pipeline import export_analytics
    export_analytics(con, growth_stats=growth_stats)


def main():
    parser = argparse.ArgumentParser(description="Cloud Sync for MotherDuck")
    parser.add_argument("--skip-preflight", action="store_true", help="Skip pre-flight validation")
    parser.add_argument("--dry-run", action="store_true", help="Validate only, no changes")
    parser.add_argument("--notify-success", action="store_true", help="Send Telegram on success")
    args = parser.parse_args()
    
    today = date.today()
    today_str = today.isoformat()
    
    print(f"☁️ Cloud Sync for {today_str}")
    print("=" * 50)
    
    # Stats dictionary to track metrics
    stats = {
        "total_checked": 0,
        "errors": 0,
        "new_petitions": 0,
        "vote_delta": 0,
        "status_changes": 0,
        "cabinet_new": 0
    }
    
    # Step 1: Connect to MotherDuck (Needed for dynamic pre-flight)
    try:
        con = get_motherduck_connection()
    except Exception as e:
        print(f"❌ Failed to connect to MotherDuck: {e}")
        notify_sync_failure("Connection", [str(e)])
        sys.exit(1)

    # Step 2: Pre-flight Check
    if not args.skip_preflight:
        preflight_result = run_preflight_check(con, verbose=True)
        if not preflight_result.passed:
            print("\n❌ Pre-flight check failed. Aborting sync.")
            notify_sync_failure("Pre-flight Check", preflight_result.errors)
            con.close()
            sys.exit(1)
    
    if args.dry_run:
        print("\n🔍 Dry run complete. No changes made.")
        con.close()
        sys.exit(0)
    
    # Step 3: Create Backup
    create_backup(con)
    
    try:
        # Step 4: Run Sync
        pres_delta, pres_status_changes, pres_growth = sync_president_updates(con, today_str, stats)
        pres_new, pres_new_list = sync_president_new(con, today_str, stats)
        cab_new, cab_delta, cab_growth = sync_cabinet(con, today_str, stats)
        
        # Step 5: Post-sync Validation
        postsync_result = run_postsync_validation(con, stats, verbose=True)
        
        if not postsync_result.passed:
            print("\n❌ Post-sync validation failed. Rolling back...")
            rollback_from_backup(con)
            notify_sync_failure("Post-sync Validation", postsync_result.errors, stats)
            con.close()
            sys.exit(1)
        
        # Step 6: Save daily stats
        total_delta = pres_delta + cab_delta
        all_growth = pres_growth + pres_new_list + cab_growth
        
        print("\n--- 4. Saving Daily Stats ---")
        con.execute("""
            INSERT OR REPLACE INTO daily_stats (date, president_new, cabinet_new, total_votes_delta, status_changes)
            VALUES (?, ?, ?, ?, ?)
        """, (today, pres_new, cab_new, total_delta, json.dumps(pres_status_changes)))
        
        print(f"Saved stats: +{pres_new + cab_new} petitions, +{total_delta} votes.")
        
        # Step 7: Export JSON
        print("\n--- 5. Exporting JSON ---")
        export_analytics_cloud(con, growth_stats=all_growth)
        
        # Step 8: Cleanup
        cleanup_backup(con)
        
        # Step 9: Optional success notification
        if args.notify_success:
            stats["new_petitions"] = pres_new + cab_new
            stats["vote_delta"] = total_delta
            notify_sync_success(stats)
        
        con.close()
        print("\n🎉 Cloud Sync Complete!")
        
    except Exception as e:
        print(f"\n❌ Error during sync: {e}")
        rollback_from_backup(con)
        notify_sync_failure("Sync Execution", [str(e)], stats)
        con.close()
        sys.exit(1)


if __name__ == "__main__":
    main()
