import requests
import time

API_KEY = "bb_ma_25f7ea29a54f6f4b8edfd401a960e0"
TEMPLATE_ID = "gwNr4n50xGdR5ROMBd"
PROJECT_ID = "YvAwdo1gj7XMp6QXKm"  # from Bannerbear dashboard

# 1. Create image request
url = "https://api.bannerbear.com/v2/images"

data = {
    "project_id": PROJECT_ID,  # required with Master Key
    "template": TEMPLATE_ID,
    "modifications": [
        {
            "name": "image_container",
            "image_url": 'https://img.etimg.com/thumb/width-1200,height-900,imgsize-83046,resizemode-75,msid-123502304/wealth/save/new-aadhaar-rule-can-you-get-two-baal-aadhaars-using-same-birth-certificate-heres-the-latest-government-amendment.jpg'
        },
        {
            "name": "title",
            "text": 'title'
        },
        {"name": "label_tag", "text": "label_tag"},
        {"name": "subtitle", "text": "subtitle"},
        {"name": "footer_1", "text": "footer_1"},
        {"name": "footer_2", "text": "footer_2"}
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
print("Initial Response:", resp_json)
