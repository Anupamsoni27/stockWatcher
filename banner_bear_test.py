import requests

API_KEY = "bb_ma_25f7ea29a54f6f4b8edfd401a960e0"

url = "https://api.bannerbear.com/v2/projects"

headers = {
    "Authorization": f"Bearer {API_KEY}"
}

response = requests.get(url, headers=headers)

print("Status Code:", response.status_code)
try:
    print("Response:", response.json())
except Exception:
    print("Raw Response:", response.text)
