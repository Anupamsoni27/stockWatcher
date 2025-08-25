import requests
from datetime import datetime, timedelta, timezone

url = "https://api.marketaux.com/v1/news/all"

# Current UTC time minus 1 hour
published_after = (datetime.now(timezone.utc) - timedelta(minutes=240)).strftime("%Y-%m-%dT%H:%M:%S")

params = {
    "api_token": "LQz9iaxCPhvpOPqV2sBvP02iANaizbH19RhQi1u4",  # replace with your valid token
    "limit": 100,
    "countries": "in",
    "published_after": published_after
}
print(params)
response = requests.get(url, params=params)

if response.status_code == 200:
    print(response.json())  # parsed JSON response
    res = response.json()
    news_data = res
    for article in news_data['data']:
        print(article)
        # Extract only the fields you want
        selected_data = {
            "uuid": article.get("uuid"),
            "title": article.get("title"),
            "published_at": article.get("published_at"),
            "description": article.get("description"),
            "url": article.get("url"),
            "keywords": article.get("keywords"),
            "snippet": article.get("snippet"),
            "image_url": article.get("image_url"),
            "source": article.get("source"),
            "symbols": ", ".join(entity.get("symbol").split('.')[0] for entity in article.get("entities", []) if entity.get("symbol"))
        }
        print(selected_data)
else:
    print(f"Error: {response.status_code} - {response.text}")
