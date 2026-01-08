# ensure_motherduck_schema.py
"""
Utility script – verify and (re)create PRIMARY KEY constraints
for the MotherDuck tables used by the sync pipeline.

Run:
    python3 etl/ensure_motherduck_schema.py
"""

import os
import duckdb
from dotenv import load_dotenv

# -------------------------------------------------
# 1️⃣  Connect to MotherDuck
# -------------------------------------------------
def get_md_connection():
    load_dotenv()
    token = os.getenv("MOTHERDUCK_TOKEN")
    if not token:
        raise RuntimeError("MOTHERDUCK_TOKEN not set in .env")
    con = duckdb.connect(f"md:petitions_prod?motherduck_token={token}")
    print("✅ Connected to MotherDuck")
    return con

# -------------------------------------------------
# 2️⃣  Helper – does a table already have a PK?
# -------------------------------------------------
def has_primary_key(con, table_name: str) -> bool:
    sql = """
        SELECT constraint_type
        FROM information_schema.table_constraints
        WHERE table_name = ?
          AND constraint_type = 'PRIMARY KEY'
    """
    res = con.execute(sql, (table_name,)).fetchall()
    return len(res) > 0

# -------------------------------------------------
# 3️⃣  Re‑create votes_history with composite PK
# -------------------------------------------------
def fix_votes_history(con):
    if has_primary_key(con, "votes_history"):
        print("🔎 votes_history already has PRIMARY KEY – nothing to do")
        return

    print("🔧 Re‑creating votes_history with composite PRIMARY KEY …")
    # backup
    con.execute("CREATE OR REPLACE TABLE votes_history_tmp AS SELECT * FROM votes_history")
    # drop old
    con.execute("DROP TABLE IF EXISTS votes_history")
    # create new with PK
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
    # restore data
    con.execute("INSERT INTO votes_history SELECT * FROM votes_history_tmp")
    con.execute("DROP TABLE votes_history_tmp")
    print("✅ votes_history PK created")

# -------------------------------------------------
# 4️⃣  Re‑create daily_stats with PK on date
# -------------------------------------------------
def fix_daily_stats(con):
    if has_primary_key(con, "daily_stats"):
        print("🔎 daily_stats already has PRIMARY KEY – nothing to do")
        return

    print("🔧 Re‑creating daily_stats with PRIMARY KEY on date …")
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
    print("✅ daily_stats PK created")

# -------------------------------------------------
# 5️⃣  Main entry point
# -------------------------------------------------
def main():
    con = get_md_connection()
    try:
        fix_votes_history(con)
        fix_daily_stats(con)
    finally:
        con.close()
        print("🎉 Schema verification / fix completed")

if __name__ == "__main__":
    main()
