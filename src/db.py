from http.client import HTTPException
import os

import pandas as pd
from pymongo.database import Database
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.errors import ServerSelectionTimeoutError, CollectionInvalid

MONGODB_DATABASE_URL = os.getenv("MONGO_URI")

def get_db() -> Database:
    """
    Establishes a connection to the MongoDB database and returns the database object.

    :return: MongoDB database object
    :raises ServerSelectionTimeoutError: If there is an issue connecting to the MongoDB server
    """
    try:
        client: MongoClient = MongoClient(MONGODB_DATABASE_URL)
        db: Database = client["lpdb"]
        return db
    except ServerSelectionTimeoutError as e:
        print(f"Failed to connect to the database: {e}")
        raise

def get_collection(collection_name: str) -> Collection:
    """
    Given a collection name, returns the specified collection object.

    :param collection_name: Name of the collection to retrieve
    :return: MongoDB collection object
    :raises CollectionInvalid: If the specified collection does not exist
    :raises ServerSelectionTimeoutError: If there is an issue connecting to the MongoDB server
    """
    try:
        db: Database = get_db()
        collection: Collection = db[collection_name]
        return collection
    except CollectionInvalid as e:
        print(f"Invalid collection name: {collection_name}. Error: {e}")
        raise
    except ServerSelectionTimeoutError as e:
        print(f"Failed to connect to the database or retrieve collection: {e}")
        raise

def query_by_puuid(puuid: str, data_source_collection: Collection) -> None:
    query_dict = {"puuid": puuid}
    data = list(data_source_collection.find(query_dict))
    df = pd.DataFrame(data)
    if not data:
        raise HTTPException(status_code=404, detail="Data not found for the given PUUID")
    return data