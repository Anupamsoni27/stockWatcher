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
            "image_url": "https://g.foolcdn.com/editorial/images/830256/arrow-symbol-grass.jpg"
        },
        {
            "name": "title",
            "text": "This Artificial Intelligence (AI) Stock Could Jump 27% at Least, According to Wall Street"
        },
        {"name": "label_tag", "text": "You can change this text"},
        {"name": "subtitle", "text": "You can change this text"},
        {"name": "footer_1", "text": "You can change this text"},
        {"name": "footer_2", "text": "You can change this text"}
    ]
}

headers = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {API_KEY}"
}
#
# response = requests.post(url, headers=headers, json=data)
# print("Status Code:", response.status_code)
# resp_json = response.json()
# uid = resp_json.get("uid")
# print("Initial Response:", resp_json)
#
# if response.status_code != 202:
#     raise Exception("❌ Failed to create image: " + str(resp_json))
#
# image_uid = resp_json["uid"]
#
# # 2. Poll until completed
# poll_url = f"https://api.bannerbear.com/v2/images/{image_uid}"
#
#
# poll_response = requests.get(poll_url, headers=headers)
# poll_data = poll_response.json()
#
# status = poll_data.get("status")
#
# print("Polling status:", status)
# time.sleep(60)  # wait 2s before polling again
image_url = f"https://api.bannerbear.com/v2/images"
uid = 'OA0Ekvge5YdKoPpXQKqRLpWxX'
def get_image_status(uid: str):
    """Fetch current status and details of an image by UID"""
    url_0 = f"{image_url}"
    resp = requests.get(url_0, headers=headers)
    resp.raise_for_status()
    return resp.json()

print(get_image_status(uid))
