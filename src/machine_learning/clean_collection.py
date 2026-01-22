import os
from urllib.parse import quote_plus
from dotenv import load_dotenv
from pymongo import MongoClient

# --- CONFIGURATION ---
DRY_RUN = False  # Set to False to actually delete data
# ---------------------

# Load environment variables
load_dotenv()

try:
    # 1. Setup Connection (using your provided logic)
    username = quote_plus(os.getenv("MONGO_USERNAME"))
    password = quote_plus(os.getenv("MONGO_PASSWORD"))
    database_name = os.getenv("DB_NAME")
    
    uri = f"mongodb+srv://{username}:{password}@ol-cluster.3agvwhk.mongodb.net/{database_name}?retryWrites=true&w=majority"
    
    client = MongoClient(uri)
    db = client[database_name]
    collection = db["stock_prices"]
    
    print("✅ MongoDB Connection Successful!")
    print(f"Total documents before cleaning: {collection.count_documents({})}")
    print("-" * 40)

    # 2. Identify Duplicates
    # We group by 'timestamp' because in stock data, a timestamp should be unique.
    # If you have duplicates based on other fields, add them to the '_id' dictionary below.
    pipeline = [
        {
            "$group": {
                # The field(s) that define a duplicate
                "_id": { "timestamp": "$timestamp" }, 
                # Collect all unique _ids for this timestamp
                "uniqueIds": { "$addToSet": "$_id" }, 
                # Count how many times this timestamp appears
                "count": { "$sum": 1 } 
            }
        },
        {
            # Filter to keep only groups that have duplicates (count > 1)
            "$match": { 
                "count": { "$gt": 1 } 
            }
        }
    ]

    print("running aggregation to find duplicates...")
    duplicates = list(collection.aggregate(pipeline))
    
    ids_to_delete = []

    for doc in duplicates:
        # doc['uniqueIds'] is a list of _ids for the duplicate records.
        # We pop the first one off the list to SAVE it, and delete the rest.
        ids = doc['uniqueIds']
        ids.pop(0) # Keep one original
        
        # Add the remaining IDs to our deletion list
        ids_to_delete.extend(ids)

    print(f"Found {len(duplicates)} timestamps with duplicate entries.")
    print(f"Total individual documents marked for deletion: {len(ids_to_delete)}")

    # 3. Delete Duplicates
    if len(ids_to_delete) > 0:
        if DRY_RUN:
            print("\n[DRY RUN] No data was deleted.")
            print("Set DRY_RUN = False in the script to execute deletion.")
        else:
            print("\nDeleting duplicates...")
            result = collection.delete_many({"_id": {"$in": ids_to_delete}})
            print(f"🗑️ Successfully deleted {result.deleted_count} documents.")
            print(f"Total documents remaining: {collection.count_documents({})}")
    else:
        print("\n✨ Collection is clean! No duplicates found.")

except Exception as e:
    print(f"❌ Error: {e}")

finally:
    if 'client' in locals():
        client.close()