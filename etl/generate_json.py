import duckdb
import os
from dotenv import load_dotenv
from pipeline import export_analytics, DB_FILE

# Load .env from project root
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '.env'))

def get_connection():
    """
    Connect to MotherDuck (cloud) if MOTHERDUCK_TOKEN is available,
    otherwise fall back to local petitions.duckdb.
    """
    token = os.getenv('MOTHERDUCK_TOKEN', '')
    if token:
        try:
            md_db = 'petitions_prod'
            print(f"☁️  Connecting to MotherDuck ({md_db})...")
            con = duckdb.connect(f"md:{md_db}?motherduck_token={token}")
            # Quick sanity check
            count = con.execute("SELECT COUNT(*) FROM petitions").fetchone()[0]
            print(f"   ✅ Connected! {count:,} petitions in cloud DB")
            return con
        except Exception as e:
            print(f"   ⚠️ MotherDuck connection failed: {e}")
            print(f"   Falling back to local DB...")
    
    print(f"💾 Connecting to local DB: {DB_FILE}")
    con = duckdb.connect(DB_FILE, read_only=True)
    count = con.execute("SELECT COUNT(*) FROM petitions").fetchone()[0]
    print(f"   ✅ Connected! {count:,} petitions in local DB")
    return con

def run():
    con = get_connection()
    export_analytics(con, growth_stats=[])
    con.close()
    print("Done!")

if __name__ == "__main__":
    run()
