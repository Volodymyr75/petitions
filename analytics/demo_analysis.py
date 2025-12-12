import duckdb

DB_FILE = 'petitions.duckdb'

def run_analytics():
    print(f"Connecting to {DB_FILE}...\n")
    con = duckdb.connect(DB_FILE)

    # 1. Total Count by Source
    print("📊 TOTAL PETITIONS BY SOURCE:")
    res = con.execute("SELECT source, count(*) as count FROM petitions GROUP BY source").fetchall()
    for row in res:
        print(f"   {row[0]}: {row[1]}")
    print("-" * 30)

    # 2. Top 5 Most Voted (All time)
    print("🔥 TOP 5 MOST VOTED PETITIONS:")
    res = con.execute("""
        SELECT source, votes, left(title, 80) as short_title 
        FROM petitions 
        ORDER BY votes DESC 
        LIMIT 5
    """).fetchall()
    
    for i, row in enumerate(res, 1):
        print(f"{i}. [{row[0]}] {row[1]} votes | {row[2]}...")
    print("-" * 30)

    # 3. Average Votes by Source
    print("📈 AVERAGE VOTES BY SOURCE:")
    res = con.execute("""
        SELECT source, CAST(AVG(votes) AS INTEGER) as avg_votes 
        FROM petitions 
        GROUP BY source
    """).fetchall()
    for row in res:
        print(f"   {row[0]}: ~{row[1]} votes")

    con.close()

if __name__ == "__main__":
    run_analytics()
