
import os
import requests
import json

class AirtableCRUD:
    def __init__(self, api_key, base_id, table_name):
        self.api_key = api_key
        self.base_id = base_id
        self.table_name = table_name
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        self.base_url = f"https://api.airtable.com/v0/{self.base_id}/{self.table_name}"
        self.meta_api_url = f"https://api.airtable.com/v0/meta/bases/{self.base_id}/tables"

    def create_record(self, data):
        try:
            payload = {"records": [{"fields": data}]}
            response = requests.post(self.base_url, headers=self.headers, json=payload)
            response.raise_for_status()  # Raise an exception for HTTP errors
            return response.json()["records"][0]
        except requests.exceptions.RequestException as e:
            print(f"Error creating record: {e}")
            return None

    def get_record(self, record_id):
        try:
            url = f"{self.base_url}/{record_id}"
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error retrieving record: {e}")
            return None

    def update_record(self, record_id, data):
        try:
            payload = {"records": [{"id": record_id, "fields": data}]}
            url = f"{self.base_url}"
            response = requests.patch(url, headers=self.headers, json=payload)
            response.raise_for_status()
            return response.json()["records"][0]
        except requests.exceptions.RequestException as e:
            print(f"Error updating record: {e}")
            return None

    def delete_record(self, record_id):
        try:
            url = f"{self.base_url}/{record_id}"
            response = requests.delete(url, headers=self.headers)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error deleting record: {e}")
            return None

    def list_records(self, view=None):
        try:
            params = {"view": view} if view else {}
            response = requests.get(self.base_url, headers=self.headers, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error listing records: {e}")
            return None

    def batch_create_records(self, records_data):
        try:
            payload = {"records": [{"fields": record} for record in records_data]}
            response = requests.post(self.base_url, headers=self.headers, json=payload)
            response.raise_for_status()
            return response.json()["records"]
        except requests.exceptions.RequestException as e:
            print(f"Error batch creating records: {e}")
            if hasattr(e, 'response') and e.response is not None:
                print(f"Airtable API Response: {e.response.text}")
            return None

    def get_table_schema(self):
        try:
            response = requests.get(self.meta_api_url, headers=self.headers)
            response.raise_for_status()
            tables_data = response.json().get('tables', [])
            for table in tables_data:
                if table.get('id') == self.table_name:
                    return table.get('fields', [])
            print(f"Table '{self.table_name}' not found in base schema.")
            return None
        except requests.exceptions.RequestException as e:
            print(f"Error fetching table schema: {e}")
            if hasattr(e, 'response') and e.response is not None:
                print(f"Airtable API Response for schema: {e.response.text}")
            return None
