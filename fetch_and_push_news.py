import os
import requests
import pandas as pd
import json
from datetime import datetime
from airtable_crud import AirtableCRUD

# Airtable Configuration
AIRTABLE_API_KEY = "patTltTOPC4BsRhqI.f57c7f19de10ea692b9cc0ded290a4d569670e6ece3bee805a6582f64379bbc4"
AIRTABLE_BASE_ID = "appDtlezbD4ebHqi6"
AIRTABLE_TABLE_NAME = "tblS9BOGas8yDS05t" # article table
AIRTABLE_PUBLISHERS_TABLE_NAME = "tbldeb53sZHQiaNXs" # publishers table

# Polygon.io Configuration
POLYGON_API_KEY = "Dcdo2UR68BfktLsbSf6doJEE_cEDnyDC"
POLYGON_NEWS_URL = "https://api.polygon.io/v2/reference/news"

def fetch_news_from_polygon(api_key, date_gte, limit=10, order="asc", sort="published_utc"):
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

def main():
    print("--- Fetching News and Pushing to Airtable ---")
    
    # Initialize Airtable CRUD
    airtable_crud = AirtableCRUD(AIRTABLE_API_KEY, AIRTABLE_BASE_ID, AIRTABLE_TABLE_NAME)
    publishers_crud = AirtableCRUD(AIRTABLE_API_KEY, AIRTABLE_BASE_ID, AIRTABLE_PUBLISHERS_TABLE_NAME)

    # Fetch table schema to get allowed multi-select options
    print("--- Fetching Airtable Table Schema ---")
    table_schema = airtable_crud.get_table_schema()
    publishers_schema = publishers_crud.get_table_schema()

    allowed_tickers = set()
    allowed_keywords = set()
    existing_publishers = {}

    if table_schema:
        for field in table_schema:
            if field.get('name') == 'tickers' and field.get('type') == 'multipleSelect':
                for option in field.get('options', {}).get('choices', []):
                    allowed_tickers.add(option.get('name'))
            elif field.get('name') == 'keywords' and field.get('type') == 'multipleSelect':
                for option in field.get('options', {}).get('choices', []):
                    allowed_keywords.add(option.get('name'))
        print(f"Allowed Tickers: {allowed_tickers}")
        print(f"Allowed Keywords: {allowed_keywords}")
    else:
        print("Failed to retrieve Airtable table schema for news_articles. Proceeding without filtering multi-select options.")

    if publishers_schema:
        for field in publishers_schema:
            if field.get('name') == 'name' and field.get('type') == 'singleLineText': # Assuming 'name' is the primary field for publishers
                # Fetch all existing publisher records to build a lookup
                all_publishers = publishers_crud.list_records()
                if all_publishers and "records" in all_publishers:
                    for publisher_record in all_publishers["records"]:
                        publisher_name = publisher_record.get("fields", {}).get('name')
                        publisher_id = publisher_record.get('id')
                        if publisher_name and publisher_id:
                            existing_publishers[publisher_name] = publisher_id
                    break
        print(f"Existing Publishers: {existing_publishers}")
    else:
        print("Failed to retrieve Airtable table schema for publishers. Proceeding without linking publishers.")

    # Fetch news from Polygon.io (example date)
    news_data = fetch_news_from_polygon(POLYGON_API_KEY, date_gte="2025-08-24T00:00:00Z", limit=10)

    if news_data:
        print(f"Fetched {len(news_data)} news articles.")
        airtable_records = []
        for article in news_data:
            # Filter Tickers and Keywords to only include allowed options
            filtered_tickers = [ticker for ticker in article.get('tickers', []) if ticker in allowed_tickers]
            filtered_keywords = [keyword for keyword in article.get('keywords', []) if keyword in allowed_keywords]

            # Handle Publisher linking
            publisher_name = article.get('publisher', {}).get('name')
            publisher_link_id = None

            if publisher_name:
                if publisher_name in existing_publishers:
                    publisher_link_id = existing_publishers[publisher_name]
                else:
                    # Create new publisher if it doesn't exist (and if allowed by API key)
                    new_publisher_data = {"name": publisher_name} # Use lowercase 'name'
                    created_publisher = publishers_crud.create_record(new_publisher_data)
                    if created_publisher:
                        publisher_link_id = created_publisher.get('id')
                        existing_publishers[publisher_name] = publisher_link_id # Add to lookup
                    else:
                        print(f"Failed to create publisher: {publisher_name}")
                
            record = {
                'id': article.get('id'),
                'title': article.get('title'),
                'author': article.get('author'),
                'published_date': datetime.fromisoformat(article.get('published_utc').replace('Z', '+00:00')).strftime('%Y-%m-%d'),
                'published_utc': article.get('published_utc').replace('Z', '+00:00'),
                'article_url': article.get('article_url'),
                'description': article.get('description'),
                'image_url': article.get('image_url'),
                'tickers': filtered_tickers,
                'keywords': filtered_keywords,
                'posted_status': 'false',
                'insights': json.dumps(article.get('insights', [])), # Storing as JSON string for multilineText
            }
            if publisher_link_id:
                record['publisher'] = [publisher_link_id]
            
            airtable_records.append(record)

        # Fetch existing records from Airtable to check for duplicates
        print("--- Fetching Existing Records from Airtable ---")
        existing_records_response = airtable_crud.list_records(published_date_gte="2025-08-25") # Use the same date as news fetch or adjust
        existing_unique_ids = set()

        if existing_records_response and "records" in existing_records_response:
            for record in existing_records_response["records"]:
                record_id = record.get("fields", {}).get("id") # Use 'id' as the unique identifier
                if record_id:
                    existing_unique_ids.add(record_id)
            print(f"Found {len(existing_unique_ids)} existing unique record IDs.")

        # Filter out duplicate news articles
        filtered_airtable_records = []
        for record in airtable_records:
            record_id = record.get("id") # Use 'id' as the unique identifier
            
            if record_id not in existing_unique_ids:
                filtered_airtable_records.append(record)

        if not filtered_airtable_records:
            print("All fetched news articles are duplicates or no valid records to push.")
            return

        # Batch create records in Airtable
        print("--- Batch Creating Records in Airtable ---")
        created_records = airtable_crud.batch_create_records(filtered_airtable_records)

        if created_records:
            print(f"Successfully created {len(created_records)} records in Airtable.")
            for record in created_records:
                print(f"Created Record ID: {record.get('id')}, Title: {record.get('fields', {}).get('title')}")
        else:
            print("Failed to batch create records in Airtable.")
    else:
        print("No news articles fetched.")

if __name__ == "__main__":
    main()
