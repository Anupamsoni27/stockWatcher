import os
import time
import requests

from airtable_crud import AirtableCRUD
from datetime import datetime, timedelta, timezone

# Airtable Configuration
AIRTABLE_API_KEY = "patTltTOPC4BsRhqI.f57c7f19de10ea692b9cc0ded290a4d569670e6ece3bee805a6582f64379bbc4"
AIRTABLE_BASE_ID = "appDtlezbD4ebHqi6"
AIRTABLE_TABLE_NAME = "tblsgL4bwLb4ujnZ5" # article table
AIRTABLE_PUBLISHERS_TABLE_NAME = "tbldeb53sZHQiaNXs" # publishers table


API_KEY = "bb_ma_eb6c9eb0a9a43915cead44aa562836"
TEMPLATE_ID = "lzw71BD6EoA750eYkn"
PROJECT_ID = "KDWmnAMxQqv180dVk4"  # from Bannerbear dashboard
BANNERBEAR_BASE_URL = "https://api.bannerbear.com/v2"


# Polygon.io Configuration
MARKET_AUX_API_KEY = "LQz9iaxCPhvpOPqV2sBvP02iANaizbH19RhQi1u4"
MARKET_AUX_NEWS_URL = "https://api.marketaux.com/v1/news/all"

# Current UTC time minus 1 hour
published_after_1 = (datetime.now(timezone.utc) - timedelta(minutes=60)).strftime("%Y-%m-%dT%H:%M:%S")
published_after_2 = (datetime.now(timezone.utc) - timedelta(minutes=240)).strftime("%Y-%m-%dT%H:%M:%S")

airtable_crud = AirtableCRUD(AIRTABLE_API_KEY, AIRTABLE_BASE_ID, AIRTABLE_TABLE_NAME)

uuid = ''
def create_image(row_data):
    # 1. Create image request
    url = "https://api.bannerbear.com/v2/images"
    data = row_data['fields']
    print(data)
    data = {
        "project_id": PROJECT_ID,  # required with Master Key
        "template": TEMPLATE_ID,
        "modifications": [
            {
                "name": "image_container",
                "image_url": data['image_url']
            },
            {
                "name": "title",
                "text": data['title']
            },
            {"name": "label_tag", "text": 'Stocks:  ' + data["symbols"] + ' Related News' if (data["symbols"] != '')  else  'Breaking News' },
            {"name": "subtitle", "text": "source: " + data["source"]},
            {"name": "footer_1", "text": "@the.current.capsule"},
            {"name": "footer_2", "text": "The Current Capsule"}
        ]
    }

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {API_KEY}"
    }

    response = requests.post(url, headers=headers, json=data)
    print("Status Code:", response.status_code)
    resp_json = response.json()
    uid = resp_json.get("uid")
    if (uid):
        time.sleep(30)
        success = fetch_bannerbear_image(API_KEY, uid, row_data['id'])
        if success:
            print("Image fetch process completed.")
        else:
            print("Image fetch process failed.")

    print("Initial Response:", resp_json)


def fetch_bannerbear_image(api_key, image_uid, row_id):
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }

    image_url = f"{BANNERBEAR_BASE_URL}/images/{image_uid}"

    try:
        # Step 1: Fetch image metadata from Bannerbear API
        metadata_response = requests.get(image_url, headers=headers, params={'project_id': PROJECT_ID})
        metadata_response.raise_for_status()  # Raise an exception for HTTP errors
        image_metadata = metadata_response.json()

        # Step 2: Extract the direct image URL from the metadata
        direct_image_download_url = image_metadata.get("image_url")
        if not direct_image_download_url:
            print("Error: 'image_url' not found in Bannerbear API response.")
            return False

        update_data = {"media_image": direct_image_download_url}
        print("erer")
        updated_record = airtable_crud.update_record(row_id, update_data)
        if updated_record:
            print("Record updated successfully:", updated_record)
        else:
            print("Failed to update record.")

        # Step 3: Download the image content directly
        # This request does not need Bannerbear API headers as it's a direct image URL
        image_content_response = requests.get(direct_image_download_url, stream=True)
        image_content_response.raise_for_status()  # Raise an exception for HTTP errors
        return True
    except requests.exceptions.RequestException as e:
        print(f"Error fetching image from Bannerbear: {e}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"Bannerbear API Response: {e.response.text}")
        return False


def fetch_news_from_polygon(api_key, date_gte, limit=10, order="asc", sort="published_utc"):
    params = {
        "api_token": api_key,  # replace with your valid token
        "limit": limit,
        "countries": "in",
        "published_after": date_gte
    }
    print(params)
    try:
        response = requests.get(MARKET_AUX_NEWS_URL, params=params)
        response.raise_for_status()
        data = response.json()
        return data.get('data', [])

    except requests.exceptions.RequestException as e:
        print(f"Error fetching news from Polygon.io: {e}")
        return []

def main():
    print("--- Fetching News and Pushing to Airtable ---")

    # Initialize Airtable CRUD
    publishers_crud = AirtableCRUD(AIRTABLE_API_KEY, AIRTABLE_BASE_ID, AIRTABLE_PUBLISHERS_TABLE_NAME)

    # Fetch news from Polygon.io (example date)
    news_data = fetch_news_from_polygon(MARKET_AUX_API_KEY, limit=1,date_gte=published_after_1 )
    if len(news_data) < 1:
        print(f"Fetched {len(news_data)} news articles. Trying again")
        news_data = fetch_news_from_polygon(MARKET_AUX_API_KEY, limit=1, date_gte=published_after_2)

    if news_data:
        print(f"Fetched {len(news_data)} news articles.")
        for article in news_data:
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

        # Batch create records in Airtable
        print("--- Batch Creating Records in Airtable ---")
        created_records = airtable_crud.batch_create_records([selected_data])

        if created_records:
            print(f"Successfully created {len(created_records)} records in Airtable.")

            for record in created_records:
                print(f"Created Record ID: {record.get('id')}, Title: {record.get('fields', {}).get('title')}")
                create_image(record)
                print(f"Creating Image...")
        else:
            print("Failed to batch create records in Airtable.")
    else:
        print("No news articles fetched.")

if __name__ == "__main__":
    main()
