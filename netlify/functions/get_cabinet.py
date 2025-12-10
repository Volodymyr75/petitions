import json
import requests

API_URL = "https://petition.kmu.gov.ua/api/petitions"

def handler(event, context):
    print("Function invoked: get_cabinet")
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json"
    }
    
    try:
        response = requests.get(API_URL, headers=headers, timeout=10)
        
        if response.status_code != 200:
             return {
                'statusCode': 502,
                'body': json.dumps({'error': f"Cabinet API returned {response.status_code}"})
            }

        json_resp = response.json()
        data_list = json_resp.get("rows", [])
        
        petitions = []
        for item in data_list:
            petitions.append({
                "id": str(item.get("id")),
                "number": item.get("code"),
                "title": item.get("title"),
                "date": item.get("createdAt"),
                "status": item.get("status"),
                "votes": item.get("signaturesNumber"),
                "url": f"https://petition.kmu.gov.ua/kmu/petition/{item.get('id')}"
            })

        return {
            'statusCode': 200,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({'source': 'Cabinet of Ministers', 'data': petitions}, ensure_ascii=False)
        }

    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }
