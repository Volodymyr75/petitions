import duckdb
from pipeline import export_analytics, DB_FILE

def run():
    print(f"Connecting to {DB_FILE}...")
    try:
        con = duckdb.connect(DB_FILE)
        # We pass empty growth_stats for now as we are not scraping fresh data
        export_analytics(con, growth_stats=[])
        con.close()
        print("Done!")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    run()
