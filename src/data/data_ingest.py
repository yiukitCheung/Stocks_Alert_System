from kafka import KafkaConsumer
from pymongo import MongoClient, ASCENDING, DESCENDING, UpdateOne
import json
import pandas as pd
import schedule
import time
import logging
from datetime import datetime

class StockDataIngestor:
    def __init__(self,schedule_time,
                mongo_uri="mongodb://localhost:27017/",
                db_name="local", 
                daily_collection_name="daily_stock_price",
                weekly_collection_name="weekly_stock_price"):

        self.current_date = pd.to_datetime('today').strftime('%Y-%m-%d')
        self.schedule_time = schedule_time
        
        # Initialize the MongoDB client
        self.client = MongoClient(mongo_uri)
        self.db = self.client[db_name]
        
        # set the collection and topics names
        self.daily_topic = daily_collection_name
        self.weekly_topic = weekly_collection_name
        
        # Initialize the Kafka consumer
        self.consumer = KafkaConsumer(bootstrap_servers='localhost:9092',
                                    value_deserializer=lambda v: json.loads(v.decode('utf-8')))
        
    def insert_data(self, collection_name, data):
        try:
            self.db[collection_name].insert_many(data)
        except Exception as e:
            logging.error(f"Error inserting data: {e}")
        
    def consume_kafka(self):
        self.consumer.subscribe(topics=set([self.daily_topic, self.weekly_topic]))
        try:
            while True:
                msg = self.consumer.poll(timeout_ms=1000)
                if len(msg) == 0:
                    print("No new messages") # Wait for new messages
                    continue
                
                # Extract data from ConsumerRecord
                for topic_partition, consumer_records in msg.items():
                    # Extract the value from the ConsumerRecord
                    records = [record.value for record in consumer_records] 
                    collection_name = topic_partition.topic # Topic name is the collection name
                    # Convert 'date' field to datetime
                    for record in records:
                        if 'date' in record:
                            record['date'] = datetime.strptime(record['date'], '%Y-%m-%d')
                
                # If the collection is empty, insert data
                self.insert_data(collection_name, records)
                print(f"Inserted {len(records)} records into {collection_name}")
                    
        except KeyboardInterrupt:
            logging.info("Closing consumer")
                
    def schedule_data_data_consumption(self):
        if self.schedule_time:
            schedule.every().day.at(self.schedule_time).do(self.consume_and_ingest)
            logging.info(f"Scheduled fetching and producing stock data at {self.schedule_time}")

            while True:
                schedule.run_pending()
                time.sleep(1)
        else:
            self.consume_kafka()

if __name__ == "__main__":
    ingestor = StockDataIngestor(schedule_time=None)
    ingestor.schedule_data_data_consumption()