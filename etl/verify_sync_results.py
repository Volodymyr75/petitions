
import duckdb
import os
import json
from datetime import date

# Use absolute path to ensure we hit the right DB
DB_FILE = os.path.join(os.getcwd(), 'petitions.duckdb')
con = duckdb.connect(DB_FILE)

print("--- 1. Checking Daily Stats ---")
stats = con.execute("SELECT * FROM daily_stats ORDER BY date DESC LIMIT 1").fetchdf()
print(stats)

print("\n--- 2. Checking Votes History (Sample) ---")
# Check if we have entries consistently for today
today_str = date.today().isoformat()
history_count = con.execute(f"SELECT COUNT(*) FROM votes_history WHERE date = '{today_str}'").fetchone()[0]
print(f"Total history entries for today: {history_count}")
print("Sample history:")
print(con.execute(f"SELECT * FROM votes_history WHERE date = '{today_str}' LIMIT 5").fetchdf())

print("\n--- 3. Checking Updated Statuses (Sample) ---")
# Check a few specific IDs mentioned in the logs to see if their status is now updated in the main table
check_ids = ['253594', '253212', '253584'] # In Review, In Review, Unknown
print(f"Checking IDs: {check_ids}")
print(con.execute(f"SELECT external_id, status, votes, updated_at FROM petitions WHERE external_id IN {tuple(check_ids)}").fetchdf())

con.close()
