import os
import requests
import pandas as pd
import json
from airtable_crud import AirtableCRUD

# Airtable Configuration
AIRTABLE_API_KEY = "patTltTOPC4BsRhqI.f57c7f19de10ea692b9cc0ded290a4d569670e6ece3bee805a6582f64379bbc4"
AIRTABLE_BASE_ID = "appDtlezbD4ebHqi6"
AIRTABLE_TABLE_NAME = "tblbPuBmDYGxWeSv3" # article table

# Polygon.io Configuration
POLYGON_API_KEY = "Dcdo2UR68BfktLsbSf6doJEE_cEDnyDC"
POLYGON_NEWS_URL = "https://api.polygon.io/v2/reference/news"

def fetch_news_from_polygon(api_key, date_gte, limit=5, order="asc", sort="published_utc"):
    params = {
        "published_utc.gte": date_gte,
        "order": order,
        "limit": limit,
        "sort": sort,
        "apiKey": api_key
    }
    try:
        response = requests.get(POLYGON_NEWS_URL, params=params)
        response.raise_for_status()
        data = response.json()
        return data.get('results', [])
    except requests.exceptions.RequestException as e:
        print(f"Error fetching news from Polygon.io: {e}")
        return []

def prepare_records_for_airtable(news_articles, allowed_tickers, allowed_keywords):
    records = []
    for article in news_articles:
        # Filter Tickers and Keywords to only include allowed options
        filtered_tickers = [ticker for ticker in article.get('tickers', []) if ticker in allowed_tickers]
        filtered_keywords = [keyword for keyword in article.get('keywords', []) if keyword in allowed_keywords]

        record = {
            'Title': article.get('title'),
            'Author': article.get('author'),
            'Published Date': article.get('published_utc', '').split('T')[0], # Extract only date
            'Article URL': article.get('article_url'),
            'Description': article.get('description'),
            'Publisher Name': article.get('publisher', {}).get('name'),
            'Publisher Homepage URL': article.get('publisher', {}).get('homepage_url'),
            'Publisher Logo URL': article.get('publisher', {}).get('logo_url'),
            'Publisher Favicon URL': article.get('publisher', {}).get('favicon_url'),
            'Image URL': article.get('image_url'),
            'Tickers': filtered_tickers, # Send as list of strings
            'Keywords': filtered_keywords, # Send as list of strings
            'Import Source ID': article.get('id')
        }
        records.append(record)
    return records

if __name__ == "__main__":
    print("--- Fetching News and Pushing to Airtable ---")
    
    # Initialize Airtable CRUD
    airtable_crud = AirtableCRUD(AIRTABLE_API_KEY, AIRTABLE_BASE_ID, AIRTABLE_TABLE_NAME)

    # Fetch table schema to get allowed multi-select options
    print("--- Fetching Airtable Table Schema ---")
    table_schema = airtable_crud.get_table_schema()

    allowed_tickers = set()
    allowed_keywords = set()

    if table_schema:
        for field in table_schema:
            if field.get('name') == 'Tickers' and field.get('type') == 'multipleSelect':
                for option in field.get('options', {}).get('choices', []):
                    allowed_tickers.add(option.get('name'))
            elif field.get('name') == 'Keywords' and field.get('type') == 'multipleSelect':
                for option in field.get('options', {}).get('choices', []):
                    allowed_keywords.add(option.get('name'))
        print(f"Allowed Tickers: {allowed_tickers}")
        print(f"Allowed Keywords: {allowed_keywords}")
    else:
        print("Failed to retrieve Airtable table schema. Proceeding without filtering multi-select options.")

    # Fetch news from Polygon.io (example date)
    news_data = fetch_news_from_polygon(POLYGON_API_KEY, date_gte="2025-08-23T00:00:00Z", limit=10)

    if news_data:
        print(f"Fetched {len(news_data)} news articles.")
        airtable_records = prepare_records_for_airtable(news_data, allowed_tickers, allowed_keywords)

        # Batch create records in Airtable
        print("--- Batch Creating Records in Airtable ---")
        created_records = airtable_crud.batch_create_records(airtable_records)

        if created_records:
            print(f"Successfully created {len(created_records)} records in Airtable.")
            for record in created_records:
                print(f"Created Record ID: {record.get('id')}, Title: {record.get('fields', {}).get('Title')}")
        else:
            print("Failed to batch create records in Airtable.")
    else:
        print("No news articles fetched.")
