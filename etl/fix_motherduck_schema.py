# fix_motherduck_schema.py
"""Utility script to fix missing PRIMARY KEY constraints in MotherDuck tables.

The script performs the following steps:
1. Connect to MotherDuck using the token from .env (MOTHERDUCK_TOKEN).
2. Re‑create `votes_history` with a composite PRIMARY KEY (petition_id, source, date).
3. Re‑create `daily_stats` with PRIMARY KEY on `date`.
4. Preserve existing data by copying it into temporary tables and then moving it back.
5. Print a short summary of actions performed.

Run it once (e.g. `python etl/fix_motherduck_schema.py`). After successful execution you can delete the script.
"""

import os
import duckdb
from dotenv import load_dotenv

def get_connection():
    load_dotenv()
    token = os.getenv("MOTHERDUCK_TOKEN")
    if not token:
        raise ValueError("MOTHERDUCK_TOKEN not set in .env")
    con = duckdb.connect(f"md:petitions_prod?motherduck_token={token}")
    print("✅ Connected to MotherDuck")
    return con

def recreate_votes_history(con):
    print("🔧 Recreating `votes_history` with PRIMARY KEY...")
    # Backup existing data
    con.execute("CREATE OR REPLACE TABLE votes_history_tmp AS SELECT * FROM votes_history")
    # Drop old table
    con.execute("DROP TABLE IF EXISTS votes_history")
    # Create new table with proper PK
    con.execute(
        """
        CREATE TABLE votes_history (
            petition_id VARCHAR,
            source VARCHAR,
            date DATE,
            votes INTEGER,
            PRIMARY KEY (petition_id, source, date)
        )
        """
    )
    # Restore data
    con.execute("INSERT INTO votes_history SELECT * FROM votes_history_tmp")
    con.execute("DROP TABLE votes_history_tmp")
    print("✅ `votes_history` fixed.")

def recreate_daily_stats(con):
    print("🔧 Recreating `daily_stats` with PRIMARY KEY on `date`...")
    con.execute("CREATE OR REPLACE TABLE daily_stats_tmp AS SELECT * FROM daily_stats")
    con.execute("DROP TABLE IF EXISTS daily_stats")
    con.execute(
        """
        CREATE TABLE daily_stats (
            date DATE PRIMARY KEY,
            president_new INTEGER,
            cabinet_new INTEGER,
            total_votes_delta INTEGER,
            status_changes JSON
        )
        """
    )
    con.execute("INSERT INTO daily_stats SELECT * FROM daily_stats_tmp")
    con.execute("DROP TABLE daily_stats_tmp")
    print("✅ `daily_stats` fixed.")

def main():
    con = get_connection()
    recreate_votes_history(con)
    recreate_daily_stats(con)
    con.close()
    print("🎉 Schema fix completed.")

if __name__ == "__main__":
    main()
