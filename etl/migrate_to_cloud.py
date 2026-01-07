"""
Migration script to copy local DuckDB database to MotherDuck cloud.
Run this once to initialize the cloud database.

Usage:
    python migrate_to_cloud.py
"""

import os
import duckdb
import sys

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from notifier import load_env

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOCAL_DB = os.path.join(BASE_DIR, 'petitions.duckdb')


def migrate():
    """Migrate local database to MotherDuck."""
    load_env()
    token = os.environ.get("MOTHERDUCK_TOKEN")
    
    if not token:
        print("❌ MOTHERDUCK_TOKEN not found.")
        print("Please create a .env file with your token or set the environment variable.")
        sys.exit(1)
    
    print(f"📦 Local database: {LOCAL_DB}")
    
    # Connect to local and check stats
    local_con = duckdb.connect(LOCAL_DB)
    local_count = local_con.execute("SELECT COUNT(*) FROM petitions").fetchone()[0]
    print(f"   Local petitions: {local_count}")
    local_con.close()  # Close before attaching!
    
    # Connect to MotherDuck
    print("\n☁️ Connecting to MotherDuck...")
    cloud_con = duckdb.connect(f"md:petitions_prod?motherduck_token={token}")
    print("   Connected!")
    
    # Check if tables exist in cloud
    existing_tables = [t[0] for t in cloud_con.execute("SHOW TABLES").fetchall()]
    print(f"   Existing cloud tables: {existing_tables}")
    
    if 'petitions' in existing_tables:
        cloud_count = cloud_con.execute("SELECT COUNT(*) FROM petitions").fetchone()[0]
        print(f"   Cloud petitions: {cloud_count}")
        
        if cloud_count > 0:
            response = input("\n⚠️ Cloud database already has data. Overwrite? (yes/no): ")
            if response.lower() != 'yes':
                print("Migration cancelled.")
                return
    
    # Attach local to cloud connection
    print("\n🚀 Starting migration...")
    cloud_con.execute(f"ATTACH '{LOCAL_DB}' AS local_db")

    
    # Migrate petitions
    print("   Migrating petitions table...")
    cloud_con.execute("CREATE OR REPLACE TABLE petitions AS SELECT * FROM local_db.petitions")
    
    # Migrate daily_stats
    print("   Migrating daily_stats table...")
    try:
        cloud_con.execute("CREATE OR REPLACE TABLE daily_stats AS SELECT * FROM local_db.daily_stats")
    except:
        print("   (daily_stats not found, creating empty)")
        cloud_con.execute("""
            CREATE TABLE IF NOT EXISTS daily_stats (
                date DATE PRIMARY KEY,
                president_new INTEGER,
                cabinet_new INTEGER,
                total_votes_delta INTEGER,
                status_changes VARCHAR
            )
        """)
    
    # Migrate votes_history
    print("   Migrating votes_history table...")
    try:
        cloud_con.execute("CREATE OR REPLACE TABLE votes_history AS SELECT * FROM local_db.votes_history")
    except:
        print("   (votes_history not found, creating empty)")
        cloud_con.execute("""
            CREATE TABLE IF NOT EXISTS votes_history (
                petition_id VARCHAR,
                source VARCHAR,
                date DATE,
                votes INTEGER,
                PRIMARY KEY (petition_id, source, date)
            )
        """)
    
    # Verify
    print("\n✅ Migration complete! Verifying...")
    cloud_count = cloud_con.execute("SELECT COUNT(*) FROM petitions").fetchone()[0]
    print(f"   Cloud petitions: {cloud_count}")
    
    # Show tables
    tables = cloud_con.execute("SHOW TABLES").fetchall()
    print(f"   Cloud tables: {[t[0] for t in tables]}")
    
    cloud_con.close()
    local_con.close()
    
    print("\n🎉 Migration successful!")
    print("You can now run cloud_sync.py for automated updates.")


if __name__ == "__main__":
    migrate()
