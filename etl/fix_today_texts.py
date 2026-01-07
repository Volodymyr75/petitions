import duckdb
from scraper_detail import fetch_petition_detail
import time

DB_FILE = "petitions.duckdb"
TARGET_IDS = ["47", "94", "99", "103", "104", "105", "133", "210", "212", "263", "276", "305", "400", "404", "410", "449", "484", "503", "529", "588", "640", "646", "715", "721", "723", "740", "935", "995"]

def fix_texts():
    con = duckdb.connect(DB_FILE)
    print(f"fixing text_length for {len(TARGET_IDS)} petitions...")
    
    for s_id in TARGET_IDS:
        print(f"Checking {s_id}...")
        data = fetch_petition_detail(int(s_id))
        
        if data and 'text_length' in data and data['text_length'] > 0:
            new_len = data['text_length']
            con.execute("UPDATE petitions SET text_length = ? WHERE source='president' AND external_id = ?", (new_len, s_id))
            print(f"✅ Updated {s_id}: text_length={new_len}")
        else:
            print(f"❌ Failed to get text for {s_id}")
        time.sleep(0.7)
        
    con.close()
    print("Done!")

if __name__ == '__main__':
    fix_texts()
