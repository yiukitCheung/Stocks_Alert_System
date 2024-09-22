from kafka import KafkaConsumer
from pymongo import MongoClient
import json
import pandas as pd
import schedule
import time

class StockDataIngestor:
    def __init__(self,
                mongo_uri='mongodb://localhost:27017/', 
                kafka_server='localhost:9092', 
                kafka_topic='stock_price',
                schedule_time="14:01"):
        
        # Initialize MongoDB client
        self.client = MongoClient(mongo_uri)
        self.db = self.client['stock_data_db']
        self.collection = self.db['stock_data']
        self.schedule_time = schedule_time
        # Initialize the Kafka consumer
        self.consumer = KafkaConsumer(kafka_topic, 
                                    bootstrap_servers=kafka_server,
                                    value_deserializer=lambda v: json.loads(v.decode('utf-8')))
    
    def process(self, df):
        # Ensure lowercase columns
        df.columns = df.columns.str.lower()    
        
        # Filter out irrelevant features
        df = df.loc[:, ['symbol','date', 'open', 'high', 'low', 'close','volume']]
        return df

    def consume_and_ingest(self):
        # Consume and process the data
        for message in self.consumer:
            try:
                # Extract and convert the message to a DataFrame
                df = pd.DataFrame([message.value])
                
                # Clean and Filter the data
                processed_df = self.process(df)

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
                self.collection.update_one(
                    {"symbol": processed_record['symbol'], "price_data.date": {"$ne": processed_record['date']}},    
                    {"$set": {"symbol": processed_record['symbol']},"$push": {"price_data": price_data_entry}},
                    upsert=True  # Create document if it doesn't exist
                )
            
                print(f"Inserted {processed_record['symbol']} on {processed_record['date']}")
                
            except Exception as e:
                print(f"Error processing data: {e}")
                
    def schedule_data_data_consumption(self):
        if self.schedule_time:
            schedule.every().day.at(self.schedule_time).do(self.consume_and_ingest)
            print(f"Scheduled fetching and producing stock data at {self.schedule_time}")

            while True:
                schedule.run_pending()
                time.sleep(1)
        else:
            self.consume_and_ingest()

if __name__ == "__main__":
    ingestor = StockDataIngestor(schedule_time=None)
    ingestor.schedule_data_data_consumption()