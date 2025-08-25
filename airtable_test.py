import os
from pyairtable import Api
from airtable_crud import AirtableCRUD

AIRTABLE_API_KEY = "patTltTOPC4BsRhqI.f57c7f19de10ea692b9cc0ded290a4d569670e6ece3bee805a6582f64379bbc4"
AIRTABLE_BASE_ID = "appDtlezbD4ebHqi6"
AIRTABLE_TABLE_NAME = "tblS9BOGas8yDS05t" # article

# Initialize the AirtableCRUD instance
airtable_crud = AirtableCRUD(AIRTABLE_API_KEY, AIRTABLE_BASE_ID, AIRTABLE_TABLE_NAME)

print(airtable_crud.list_records())
#
# # Test creating a record
# print("\n--- Creating Record ---")
# new_record_data = {'Title': 'Prediction: This Unstoppable Stock Will Join Nvidia, Microsoft, and Apple in the $3 Trillion Club Before 2029', 'Author': 'Prosper Junior Bakiny', 'Published Date': '2025-08-22', 'Article URL': 'https://www.fool.com/investing/2025/08/22/prediction-this-unstoppable-stock-will-join-nvidia/?source=iedfolrf0000001', 'Description': 'Meta Platforms is predicted to potentially reach a $3 trillion market cap by 2029, driven by its significant investments in AI technology, strong advertising revenue, and massive user base of over 3 billion daily active users.', 'Publisher Name': 'The Motley Fool', 'Publisher Homepage URL': 'https://www.fool.com/', 'Publisher Logo URL': 'https://s3.polygon.io/public/assets/news/logos/themotleyfool.svg', 'Publisher Favicon URL': 'https://s3.polygon.io/public/assets/news/favicons/themotleyfool.ico', 'Image URL': 'https://g.foolcdn.com/editorial/images/829250/person-working-at-a-desk.jpg', 'Tickers': ['Sample_Ticker'], 'Keywords': ['AI', 'market capitalization', 'technology', 'advertising', 'augmented reality'], 'Import Source ID': '50c39f5c3324eb742b9bb39b865426c72d594e2ea7d851a5b05861f392077371'}
# created_record = airtable_crud.create_record(new_record_data)
# if created_record:
#     print("Record created successfully:", created_record)
#     record_id = created_record.get("id")
# else:
#     print("Failed to create record. Response was:", created_record)
#     record_id = None
#
# # Test listing records
# print("\n--- Listing Records ---")
# all_records = airtable_crud.list_records()
# if all_records:
#     print("Records retrieved successfully:", all_records)
# else:
#     print("Failed to retrieve records.")
#
# # Test batch creating records
# print("\n--- Batch Creating Records ---")
# batch_data = [
#     {'Title': 'This Artificial Intelligence (AI) Stock Could Jump 27% at Least, According to Wall Street', 'Author': 'Harsh Chauhan', 'Published Date': '2025-08-22', 'Article URL': 'https://www.fool.com/investing/2025/08/22/this-artificial-intelligence-ai-stock-could-jump-2/?source=iedfolrf0000001', 'Description': 'Twilio, a cloud communications company, is experiencing growth through AI-powered communication tools. Despite recent stock volatility, analysts predict a potential 27% stock price increase, driven by expanding customer base and AI technology adoption.', 'Publisher Name': 'The Motley Fool', 'Publisher Homepage URL': 'https://www.fool.com/', 'Publisher Logo URL': 'https://s3.polygon.io/public/assets/news/logos/themotleyfool.svg', 'Publisher Favicon URL': 'https://s3.polygon.io/public/assets/news/favicons/themotleyfool.ico', 'Image URL': 'https://g.foolcdn.com/editorial/images/830256/arrow-symbol-grass.jpg', 'Tickers': ['Sample_Ticker'], 'Keywords': ['AI', 'cloud communications', 'stock analysis', 'technology', 'APIs'], 'Import Source ID': '31c52c1ea4fcba87ac9c3271d38f142673e50afcebf385d9fd1b07d14bd8ffb1'}
# ]
# batch_created_records = airtable_crud.batch_create_records(batch_data)
# if batch_created_records:
#     print("Records batch created successfully:", batch_created_records)
# else:
#     print("Failed to batch create records.")
#
# # Test reading a record
# if record_id:
#     print("\n--- Reading Record ---")
#     retrieved_record = airtable_crud.get_record(record_id)
#     if retrieved_record:
#         print("Record retrieved successfully:", retrieved_record)
#     else:
#         print("Failed to retrieve record.")
#
# # Test updating a record
# if record_id:
#     print("\n--- Updating Record ---")
#     update_data = {"Title": "Updated Test Record Title"}
#     updated_record = airtable_crud.update_record(record_id, update_data)
#     if updated_record:
#         print("Record updated successfully:", updated_record)
#     else:
#         print("Failed to update record.")
#
# # Test deleting a record
# if record_id:
#     print("\n--- Deleting Record ---")
#     deleted_record = airtable_crud.delete_record(record_id)
#     if deleted_record:
#         print("Record deleted successfully:", deleted_record)
#     else:
#         print("Failed to delete record.")