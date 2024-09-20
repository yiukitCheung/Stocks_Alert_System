import pandas as pd
import numpy as np
import ta
import pymongo

# Fetch data from database

# Connect to MongoDB
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["stock_data_db"]
collection = db["stock_data"]
data=collection.find({})
df = pd.DataFrame(list(data))
