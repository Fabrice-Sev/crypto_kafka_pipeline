import os
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
MONGO_DB = os.getenv("MONGO_DB")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION")

def setup_timeseries_collection():
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    
    # Check if collection already exists
    if MONGO_COLLECTION in db.list_collection_names():
        print(f"Collection '{MONGO_COLLECTION}' already exists.")
        return

    # Create native Time-Series Collection
    print(f"Creating Time-Series Collection '{MONGO_COLLECTION}'...")
    db.create_collection(
        MONGO_COLLECTION,
        timeseries={
            "timeField": "timestamp",
            "metaField": "symbol",
            "granularity": "seconds"
        }
    )
    print("Time-Series Collection successfully initialized!")

if __name__ == "__main__":
    setup_timeseries_collection()