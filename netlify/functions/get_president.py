import json
import requests
from bs4 import BeautifulSoup
import re

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

def clean_votes(vote_str):
    if not vote_str:
        return 0
    digits = re.sub(r'\D', '', vote_str)
    return int(digits) if digits else 0

def handler(event, context):
    print("Function invoked: get_president")
    petitions = []
    start_url = "https://petition.president.gov.ua/" 

    try:
        response = requests.get(start_url, headers=HEADERS, timeout=10)
        # response.raise_for_status() 
        # Note: Netlify might capture errors differently, but let's try strict.
        
        if response.status_code != 200:
             return {
                'statusCode': 502,
                'body': json.dumps({'error': f"Upstream returned {response.status_code}"})
            }

        soup = BeautifulSoup(response.text, 'html.parser')
        items = soup.select(".pet_item")
        
        for item in items:
            try:
                link_tag = item.select_one(".pet_link")
                if not link_tag: continue
                
                href = link_tag['href']
                pet_id = href.split("/")[-1]
                title = link_tag.get_text(strip=True)
                
                number_tag = item.select_one(".pet_number")
                number_text = number_tag.get_text(strip=True) if number_tag else "N/A"
                
                date_tag = item.select_one(".pet_date")
                date_text = date_tag.get_text(strip=True).replace("Дата оприлюднення:", "").strip() if date_tag else None
                
                status_tag = item.select_one(".pet_status")
                status_text = status_tag.get_text(strip=True) if status_tag else "Unknown"

                counts_tag = item.select_one(".pet_counts")
                raw_votes = counts_tag.get_text(strip=True) if counts_tag else "0"
                votes = clean_votes(raw_votes)

                petitions.append({
                    "id": pet_id,
                    "number": number_text,
                    "title": title,
                    "date": date_text,
                    "status": status_text,
                    "votes": votes,
                    "url": "https://petition.president.gov.ua" + href
                })
            except:
                continue

        return {
            'statusCode': 200,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({'source': 'President of Ukraine', 'data': petitions}, ensure_ascii=False)
        }

    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }
