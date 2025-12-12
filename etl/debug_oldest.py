import requests
from bs4 import BeautifulSoup

URL = "https://petition.president.gov.ua/?status=active&sort=date&order=asc&page=1"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
}

print(f"Fetching {URL}...")
response = requests.get(URL, headers=HEADERS)
soup = BeautifulSoup(response.text, 'html.parser')

items = soup.select(".pet_item")
print(f"Found {len(items)} items.")

for item in items:
    link = item.select_one(".pet_link")
    date_tag = item.select_one(".pet_date")
    if link:
        print(f"ID: {link['href'].split('/')[-1]} | Title: {link.get_text(strip=True)[:50]}... | Date: {date_tag.get_text(strip=True) if date_tag else '???'}")
