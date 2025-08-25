import unittest
from unittest.mock import patch, MagicMock
import json
import requests
from fetch_and_push_news import fetch_news_from_polygon, prepare_records_for_airtable, AirtableCRUD

class TestNewsPipeline(unittest.TestCase):

    def test_fetch_news_from_polygon_success(self):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "results": [
                {"title": "Test Article 1", "published_utc": "2025-08-23T00:00:00Z", "tickers": ["TICK1"], "keywords": ["KEY1"]},
                {"title": "Test Article 2", "published_utc": "2025-08-23T01:00:00Z", "tickers": ["TICK2"], "keywords": ["KEY2"]}
            ]
        }
        with patch('requests.get', return_value=mock_response) as mock_get:
            news = fetch_news_from_polygon("test_api_key", "2025-08-23T00:00:00Z", limit=2)
            self.assertEqual(len(news), 2)
            self.assertEqual(news[0]['title'], "Test Article 1")
            mock_get.assert_called_once()

    def test_prepare_records_for_airtable(self):
        news_articles = [
            {
                "title": "Article 1",
                "author": "Author 1",
                "published_utc": "2025-08-23T10:00:00Z",
                "article_url": "url1",
                "description": "desc1",
                "publisher": {"name": "Pub1", "homepage_url": "home1", "logo_url": "logo1", "favicon_url": "fav1"},
                "image_url": "img1",
                "tickers": ["TICK1", "TICK2"],
                "keywords": ["KEY1", "KEY2"],
                "id": "id1"
            },
            {
                "title": "Article 2",
                "author": "Author 2",
                "published_utc": "2025-08-23T11:00:00Z",
                "article_url": "url2",
                "description": "desc2",
                "publisher": {"name": "Pub2", "homepage_url": "home2", "logo_url": "logo2", "favicon_url": "fav2"},
                "image_url": "img2",
                "tickers": ["TICK3"],
                "keywords": ["KEY3"],
                "id": "id2"
            }
        ]
        allowed_tickers = {"TICK1", "TICK3", "TICK4"}
        allowed_keywords = {"KEY1", "KEY3", "KEY4"}

        airtable_records = prepare_records_for_airtable(news_articles, allowed_tickers, allowed_keywords)

        self.assertEqual(len(airtable_records), 2)
        self.assertEqual(airtable_records[0]['Title'], "Article 1")
        self.assertEqual(airtable_records[0]['Tickers'], ["TICK1"])
        self.assertEqual(airtable_records[0]['Keywords'], ["KEY1"])
        self.assertEqual(airtable_records[1]['Tickers'], ["TICK3"])
        self.assertEqual(airtable_records[1]['Keywords'], ["KEY3"])
        self.assertEqual(airtable_records[0]['Published Date'], "2025-08-23")

    @patch('fetch_and_push_news.AirtableCRUD')
    @patch('fetch_and_push_news.fetch_news_from_polygon')
    def test_duplicate_checking_and_batch_creation(self, mock_fetch_news, MockAirtableCRUD):
        # Mock news data
        mock_fetch_news.return_value = [
            {"title": "Article A", "published_utc": "2025-08-23T00:00:00Z", "tickers": ["TICK1"], "keywords": ["KEY1"], "id": "id_A"},
            {"title": "Article B", "published_utc": "2025-08-23T01:00:00Z", "tickers": ["TICK2"], "keywords": ["KEY2"], "id": "id_B"},
            {"title": "Article C", "published_utc": "2025-08-23T02:00:00Z", "tickers": ["TICK1"], "keywords": ["KEY3"], "id": "id_C"} # Duplicate (id_C-TICK1) or new?
        ]

        # Mock existing Airtable records (simulating duplicates)
        mock_airtable_crud_instance = MockAirtableCRUD.return_value
        mock_airtable_crud_instance.list_records.return_value = {
            "records": [
                {"fields": {"Import Source ID": "id_A", "Tickers": ["TICK1"]}},
                {"fields": {"Import Source ID": "id_B", "Tickers": ["TICK_OLD"]}} # Not a duplicate with new data
            ]
        }
        
        # Mock schema to allow all tickers/keywords for this test
        mock_airtable_crud_instance.get_table_schema.return_value = [
            {'name': 'Tickers', 'type': 'multipleSelect', 'options': {'choices': [{'name': 'TICK1'}, {'name': 'TICK2'}, {'name': 'TICK_OLD'}]}},
            {'name': 'Keywords', 'type': 'multipleSelect', 'options': {'choices': [{'name': 'KEY1'}, {'name': 'KEY2'}, {'name': 'KEY3'}]}}
        ]

        # Mock batch_create_records to return a successful response
        mock_airtable_crud_instance.batch_create_records.return_value = [
            {'id': 'rec1', 'fields': {'Title': 'Article B'}},
            {'id': 'rec2', 'fields': {'Title': 'Article C'}}
        ]

        # Import and run the main function from fetch_and_push_news
        from fetch_and_push_news import main
        main()

        # Assertions
        mock_fetch_news.assert_called_once()
        mock_airtable_crud_instance.list_records.assert_called_once()
        
        # Expecting 2 records to be pushed (Article B and Article C, since A-TICK1 is a duplicate)
        mock_airtable_crud_instance.batch_create_records.assert_called_once()
        args, kwargs = mock_airtable_crud_instance.batch_create_records.call_args
        self.assertEqual(len(args[0]), 2)
        self.assertEqual(args[0][0]['Import Source ID'], 'id_B')
        self.assertEqual(args[0][1]['Import Source ID'], 'id_C')
        
        # Explicitly assert that the duplicate record (id_A) is not in the records to be created
        for record in args[0]:
            self.assertNotEqual(record['Import Source ID'], 'id_A')
