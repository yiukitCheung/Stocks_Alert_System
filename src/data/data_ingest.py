from confluent_kafka import Consumer
from pymongo import MongoClient, ASCENDING, DESCENDING, UpdateOne
import json
import pandas as pd
import schedule
import time
import logging
from datetime import datetime
import sys, os
# Add project root to sys.path dynamically
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))
from config.mongdb_config import load_mongo_config
from config.kafka_config import load_kafka_config

# Configure logging
logging.basicConfig(
    level=logging.INFO,  # Set the logging level to INFO
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',  # Set the logging format
    handlers=[logging.StreamHandler()]  # Add a stream handler to print to console
)


class StockDataIngestor:
    def __init__(self,schedule_time,mongo_uri, db_name, topics, kafka_config):

        self.current_date = pd.to_datetime('today').strftime('%Y-%m-%d')
        self.schedule_time = schedule_time
        
        # Initialize the MongoDB client
        self.client = MongoClient(mongo_uri)
        self.db = self.client[db_name]
        
        # set the collection and topics names
        self.topics = topics
        
        # Initialize the Kafka consumer
        self.kafka_config = kafka_config
        self.kafka_config["group.id"] = "my-consumer-group"
        self.kafka_config["auto.offset.reset"] = "earliest"
        self.consumer = Consumer(self.kafka_config)
        
    def insert_data(self, collection_name, data):
        try:
            self.db[collection_name].insert_many(data)
        except Exception as e:
            logging.error(f"Error inserting data: {e}")
        
    def consume_kafka(self):
        self.consumer.subscribe(topics=self.topics)
        batch = []
        batch_size = 5000
        try:
            while True:
                msg = self.consumer.poll(0.1)
                if msg is None:
                    continue
                if msg.error():
                    logging.error(f"Consumer error: {msg.error()}")
                    continue
                try:
                    # Extract data from ConsumerRecord
                    deserialize_msg = json.loads(msg.value().decode('utf-8'))
                except Exception as e:
                    logging.error(f"Error deserializing message: {e}")
                    continue
                
                # Extract the value from the ConsumerRecord
                records = deserialize_msg if isinstance(deserialize_msg, list) else [deserialize_msg]
                collection_name = msg.topic() # Topic name is the collection name
                # Convert 'date' field to datetime
                for record in records:
                    if 'date' in record:
                        record['date'] = datetime.strptime(record['date'], '%Y-%m-%d')
                
                batch.extend(records)
                if len(batch) >= batch_size:
                    self.insert_data(collection_name, batch)
                    logging.info(f"Inserted {len(batch)} records into {collection_name, self.db}")
                    batch = []
                        
        except KeyboardInterrupt:
            logging.info("Closing consumer")
            
        finally:
            # Insert any remaining records in the batch
            if batch:
                self.insert_data(collection_name, batch)
                logging.info(f"Inserted {len(batch)} remaining records into {collection_name}")
            self.consumer.close()
                
    def schedule_data_data_consumption(self):
        if self.schedule_time:
            schedule.every().day.at(self.schedule_time).do(self.consume_kafka)
            logging.info(f"Scheduled fetching and producing stock data at {self.schedule_time}")

            while True:
                schedule.run_pending()
                time.sleep(1)
        else:
            self.consume_kafka()

if __name__ == "__main__": 
    # Load the MongoDB configuration once
    mongo_config = load_mongo_config()
    mongo_url = mongo_config['url']
    db_name = mongo_config["db_name"]
    warehouse_interval = mongo_config["warehouse_interval"]
    warehouse_topics = [f"{interval}_data" for interval in warehouse_interval]

    # Kafka configuration
    kafka_config = load_kafka_config()
    
    ingestor = StockDataIngestor(schedule_time=None, 
                                mongo_uri=mongo_url, 
                                db_name=db_name, 
                                topics=warehouse_topics,
                                kafka_config=kafka_config)
    
    ingestor.schedule_data_data_consumption()