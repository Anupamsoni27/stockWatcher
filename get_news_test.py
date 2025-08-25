import requests
from datetime import datetime, timedelta, timezone

url = "https://api.marketaux.com/v1/news/all"

# Current UTC time minus 1 hour
published_after = (datetime.now(timezone.utc) - timedelta(minutes=90)).strftime("%Y-%m-%dT%H:%M:%S")

params = {
    "api_token": "LQz9iaxCPhvpOPqV2sBvP02iANaizbH19RhQi1u4",  # replace with your valid token
    "limit": 1,
    "countries": "in",
    "published_after": published_after
}
print(params)
response = requests.get(url, params=params)

if response.status_code == 200:
    print(response.json())  # parsed JSON response
    res = response.json()
    print(res)
else:
    print(f"Error: {response.status_code} - {response.text}")
