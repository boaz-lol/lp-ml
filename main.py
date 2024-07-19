import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


from src.db import get_collection, get_db


if __name__ == "__main__":
    # Get "data_source" collection
    data_source_collection =  get_collection("data_source")
    