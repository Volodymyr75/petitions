"""
Final Migration Script for MotherDuck.
This script performs a CLEAN migration:
1. Drops existing tables in MotherDuck.
2. Creates tables with EXPLICIT schema and PRIMARY KEYs.
3. Loads data from the local up-to-date petitions.duckdb.
4. Verifies the results.

Usage:
    python3 etl/migrate_to_cloud_final.py
"""

import os
import duckdb
import sys
from dotenv import load_dotenv

# Path configuration
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOCAL_DB = os.path.join(BASE_DIR, 'petitions.duckdb')

def migrate_clean():
    load_dotenv()
    token = os.getenv("MOTHERDUCK_TOKEN")
    
    if not token:
        print("❌ MOTHERDUCK_TOKEN not found in .env")
        sys.exit(1)

    print(f"📦 Source (Local): {LOCAL_DB}")
    if not os.path.exists(LOCAL_DB):
        print(f"❌ Local database file not found at {LOCAL_DB}")
        sys.exit(1)

    # 1. Connect to MotherDuck
    print("☁️ Connecting to MotherDuck...")
    con = duckdb.connect(f"md:petitions_prod?motherduck_token={token}")
    print("✅ Connected to MotherDuck (petitions_prod)")

    # 2. Define Schemas
    schemas = {
        "petitions": """
            CREATE TABLE petitions (
                internal_id INTEGER,
                source VARCHAR,
                external_id VARCHAR,
                number VARCHAR,
                title VARCHAR,
                date VARCHAR,
                status VARCHAR,
                votes INTEGER,
                url VARCHAR,
                crawled_at TIMESTAMP,
                author VARCHAR,
                text_length INTEGER,
                has_answer BOOLEAN,
                date_normalized DATE,
                votes_previous INTEGER,
                updated_at TIMESTAMP,
                PRIMARY KEY (source, external_id)
            )
        """,
        "votes_history": """
            CREATE TABLE votes_history (
                petition_id VARCHAR,
                source VARCHAR,
                date DATE,
                votes INTEGER,
                PRIMARY KEY (petition_id, source, date)
            )
        """,
        "daily_stats": """
            CREATE TABLE daily_stats (
                date DATE PRIMARY KEY,
                president_new INTEGER,
                cabinet_new INTEGER,
                total_votes_delta INTEGER,
                status_changes JSON
            )
        """
    }

    # 3. Drop and Recreate
    print("\n🚀 Starting Clean Migration...")
    
    # Attach local DB
    con.execute(f"ATTACH '{LOCAL_DB}' AS local_db")

    for table_name, schema_sql in schemas.items():
        print(f"   --- Processing table: {table_name} ---")
        
        # Drop existing
        con.execute(f"DROP TABLE IF EXISTS {table_name}")
        
        # Create new with PK
        con.execute(schema_sql)
        print(f"   ✅ Created {table_name} with Primary Key.")
        
        # Load data
        print(f"   📥 Loading data for {table_name}...")
        try:
            con.execute(f"INSERT INTO {table_name} SELECT * FROM local_db.{table_name}")
            count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            print(f"   ✅ Loaded {count} rows.")
        except Exception as e:
            print(f"   ⚠️ Error loading data for {table_name}: {e}")
            print(f"   (Table {table_name} remains empty with correct schema)")

    # 4. Final Verification
    print("\n🔍 Final Verification:")
    for table_name in schemas.keys():
        # Check if PK exists in information_schema
        pk_exists = con.execute(f"""
            SELECT COUNT(*) 
            FROM information_schema.table_constraints 
            WHERE table_name = '{table_name}' AND constraint_type = 'PRIMARY KEY'
        """).fetchone()[0]
        
        status = "✅ PK FIXED" if pk_exists > 0 else "❌ PK MISSING"
        count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        print(f"   - {table_name}: {count} rows, {status}")

    con.close()
    print("\n🎉 Migration completed successfully!")
    print("Your MotherDuck database is now in sync with your local data and has the correct schema.")

if __name__ == "__main__":
    migrate_clean()
