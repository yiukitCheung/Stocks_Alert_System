import pandas as pd
import pymongo

from data.utils.alert_strategy import Alert
from features_engineering import add_features

# Connect to MongoDB
client = pymongo.MongoClient("mongodb://localhost:27017/")
# Select the database
db = client["stock_data_db"]
collection = db["stock_data"]

# Fetch all data
data = collection.find({})
# Convert to pandas dataframe
df = pd.DataFrame(list(data))

total_symbols = len(df['symbol'].unique())
added_technical = 0

for symbol in df['symbol'].unique():
    # Filter the data based on the selected stock
    price_df = pd.DataFrame(df[df['symbol'] == symbol].iloc[0]['price_data'])

    # Add technical features
    try:
        filtered_df = add_features(price_df).apply()
        filtered_df = Alert(filtered_df).add_alert()
    except Exception as e:
        print(f"Error processing symbol {symbol}: {e}")
        continue

    # Convert the new technical data to a dictionary format (all rows, or you can change this to your needs)
    filtered_df_dict = filtered_df.iloc[:,:].to_dict(orient='records') # Use all rows if necessary, not just the last one
    # Structure the MongoDB document update for this symbol
    update_data = {
        "technical_data": filtered_df_dict
    }

    # Upsert (Update if exists, Insert if not) the document with technical_data in MongoDB
    try:
        collection.update_one(
            {"symbol": symbol},  # Filter by the symbol
            {"$set": update_data},  # Update only the technical_data field
            upsert=True  # Insert if the document doesn't exist
        )
    except Exception as e:
        print(f"Error updating MongoDB for symbol {symbol}: {e}")
        continue

    added_technical += 1
    print(f"{(added_technical/total_symbols) * 100:.2f}% Technical data added successfully for {symbol}!")