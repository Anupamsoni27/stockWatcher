from pprint import pprint
import pandas as pd

import requests
import pandas as pd
import json
import time

url = "https://api.polygon.io/v2/reference/news"
params = {
    "published_utc.gte": "2025-08-23T00:00:53Z",
    "order": "asc",
    "limit": 100,
    "sort": "published_utc",
    "apiKey": "Dcdo2UR68BfktLsbSf6doJEE_cEDnyDC"
}

response = requests.get(url, params=params)
data = response.json()
pprint(data['results'])

# Flatten nested dictionaries/lists
df = pd.json_normalize(data['results'],
                       meta=['article_url','author','description','id',
                             'image_url','keywords','published_utc','tickers','title',
                             ['publisher','name'], ['publisher','homepage_url']])



df.to_csv("test.csv", index=False)
print(df)


