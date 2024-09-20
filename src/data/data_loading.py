from kafka import KafkaConsumer
from pymongo import MongoClient
import json
import pandas as pd
import schedule
import time

# Initialize MongoDB client
client = MongoClient('mongodb://localhost:27017/')
db = client['stock_data_db']
collection = db['stock_data']

# Process the data
def process(df):
    
    # Ensure lowercase columns
    df.columns = df.columns.str.lower()    
    
    # Filter out irrelevant features
    df = df.loc[:, ['symbol','date', 'open', 'high', 'low', 'close','volume']]
    return df

# Function to consume data and add technical indicators
def consume_and_store():
    
    # Initialize the Kafka consumer
    consumer = KafkaConsumer('stock_price', 
                            bootstrap_servers='localhost:9092',
                            value_deserializer=lambda v: json.loads(v.decode('utf-8')))

    # Consume and process the data
    for message in consumer:
        try:
            # Extract and convert the message to a DataFrame
            df = pd.DataFrame([message.value])
            
            # Clean and Filter the data
            processed_df = process(df)

            # Convert the processed DataFrame back to a dictionary for MongoDB
            processed_record = processed_df.to_dict(orient='records')[0]

            # Prepare the structure to store in MongoDB
            price_data_entry = {
                "date": processed_record['date'],
                "open": processed_record['open'],
                "high": processed_record['high'],
                "low": processed_record['low'],
                "close": processed_record['close'],
                "volume": processed_record['volume']
            }
            
            # Update the document in MongoDB
            collection.update_one(
                {"symbol": processed_record['symbol'], "price_data.date": {"$ne": processed_record['date']}},    
                {"$set": {"symbol": processed_record['symbol']},"$push": {"price_data": price_data_entry}},
                upsert=True  # Create document if it doesn't exist
            )
        
            print(f"Inserted {processed_record['symbol']} on {processed_record['date']}")
            
        except Exception as e:
            print(f"Error processing data: {e}")

# Schedule the fetching and producing of stock data
schedule.every().day.at("14:01").do(consume_and_store)
print("Scheduled consuming and process feteched stock data at 14:00")

while True:
    schedule.run_pending() 
    time.sleep(1)
    