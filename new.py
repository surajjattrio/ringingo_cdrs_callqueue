import os, json, pymongo, uuid, time
from config import mongo_url
# This is my path
path_to_json_files = "/usr/local/freeswitch/log/json_cdr/"
path_to_move_json_files = "/usr/local/freeswitch/log/json_cdr_archive/"

#Mongo Connection
myclient = pymongo.MongoClient(mongo_url)
mydb = myclient["admin"]
cdrs_summary_collection = mydb["cdrs_summary"]

today_date = time.strftime("%Y-%m-%d")
# Define the filter to find the document
summary_filter = {
    'date': today_date,
    'tenant_uuid': '123456',
    'user_uuid': '123456'
}
fail_count = 0
success_count = 0
total_count = 1
# Define the update operation
summary_update = {
    '$inc': {
        'fail': fail_count,
        'success': success_count,
        'total': total_count
    }
}
# Find the document and perform the update
summaray_result = cdrs_summary_collection.find_one_and_update(summary_filter, summary_update, upsert=True)

if summaray_result:
    # The record exists, and the update was successful
    print('Record exists and was updated:', summaray_result)
else:
    # The record doesn't exist
    print('Record does not exist.')
#Mongo Connection close.    
myclient.close()
