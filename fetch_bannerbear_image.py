import os
import requests

import airtable_crud


AIRTABLE_API_KEY = "patTltTOPC4BsRhqI.f57c7f19de10ea692b9cc0ded290a4d569670e6ece3bee805a6582f64379bbc4"
AIRTABLE_BASE_ID = "appDtlezbD4ebHqi6"
AIRTABLE_TABLE_NAME = "tblS9BOGas8yDS05t" # article


BANNERBEAR_API_KEY = os.environ.get("BANNERBEAR_API_KEY", "bb_ma_25f7ea29a54f6f4b8edfd401a960e0")
BANNERBEAR_BASE_URL = "https://api.bannerbear.com/v2"
PROJECT_ID = "YvAwdo1gj7XMp6QXKm"  # from Bannerbear dashboard

airtable_crud = airtable_crud.AirtableCRUD(AIRTABLE_API_KEY, AIRTABLE_BASE_ID, AIRTABLE_TABLE_NAME)


def fetch_bannerbear_image(api_key, image_uid, output_filepath):
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    # The endpoint to fetch an image by UID (assuming it's directly accessible or part of a template render endpoint)
    # Bannerbear's API for fetching an image by UID usually involves a direct URL to the image resource after it's generated.
    # For simplicity, let's assume the UID is part of the image URL or can be used to construct it directly.
    # A more accurate Bannerbear API call would involve fetching a render and then getting the image_url from that response.
    # However, for the scope of just fetching 'using UID', let's assume a direct fetch by UID is possible or the UID is the identifier in a URL.
    
    # For this example, let's assume image_uid directly corresponds to the public image URL's identifier
    # A more robust solution would involve fetching a render record first to get its image_url.
    # Example: GET /v2/images/{image_uid}
    image_url = f"{BANNERBEAR_BASE_URL}/images/{image_uid}"

    try:
        # Step 1: Fetch image metadata from Bannerbear API
        metadata_response = requests.get(image_url, headers=headers, params={'project_id': PROJECT_ID})
        metadata_response.raise_for_status() # Raise an exception for HTTP errors
        image_metadata = metadata_response.json()
        
        # Step 2: Extract the direct image URL from the metadata
        direct_image_download_url = image_metadata.get("image_url")
        if not direct_image_download_url:
            print("Error: 'image_url' not found in Bannerbear API response.")
            return False

        update_data = {"media_image": direct_image_download_url}
        updated_record = airtable_crud.update_record('rec2KS0PMl9gEj5Mh', update_data)
        if updated_record:
            print("Record updated successfully:", updated_record)
        else:
            print("Failed to update record.")

        # Step 3: Download the image content directly
        # This request does not need Bannerbear API headers as it's a direct image URL
        image_content_response = requests.get(direct_image_download_url, stream=True)
        image_content_response.raise_for_status() # Raise an exception for HTTP errors

        with open(output_filepath, 'wb') as f:
            for chunk in image_content_response.iter_content(chunk_size=8192):
                f.write(chunk)
        print(f"Image successfully fetched and saved to {output_filepath}")
        return True
    except requests.exceptions.RequestException as e:
        print(f"Error fetching image from Bannerbear: {e}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"Bannerbear API Response: {e.response.text}")
        return False

if __name__ == "__main__":
    # Example Usage
    # Replace with your actual Bannerbear API Key and Image UID
    # You would get the Image UID after creating an image or a template render.
    sample_image_uid = "OA0Ekvge5YdKoPpXQKqRLpWxX" # This should be a real UID from Bannerbear
    output_file = "bannerbear_image.png"


    success = fetch_bannerbear_image(BANNERBEAR_API_KEY, sample_image_uid, output_file)
    if success:
        print("Image fetch process completed.")
    else:
        print("Image fetch process failed.")
