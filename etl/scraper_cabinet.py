import requests
import json

API_URL = "https://petition.kmu.gov.ua/api/petitions"

def fetch_cabinet_petitions():
    print(f"Fetching from {API_URL}...")
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        "Accept": "application/json"
    }
    
    try:
        response = requests.get(API_URL, headers=headers, timeout=10)
        response.raise_for_status()
        json_resp = response.json()
        
        # KEY FIX: The API returns { count: N, rows: [...] }
        data_list = json_resp.get("rows", [])
        
        petitions = []
        for item in data_list:
            petitions.append({
                "source": "cabinet",
                "id": str(item.get("id")),
                "number": item.get("code"),
                "title": item.get("title"),
                "date": item.get("createdAt"), # ISO format
                "status": item.get("status"),
                "votes": item.get("signaturesNumber"),
                "url": f"https://petition.kmu.gov.ua/kmu/petition/{item.get('id')}",
                "content": item.get("content")
            })
            
        return petitions

    except Exception as e:
        print(f"Error fetching Cabinet petitions: {e}")
        return []

if __name__ == "__main__":
    data = fetch_cabinet_petitions()
    print(json.dumps(data[:3], indent=2, ensure_ascii=False))
