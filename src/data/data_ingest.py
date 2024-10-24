from confluent_kafka import Consumer
from pymongo import MongoClient
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
from config.data_pipeline_config import load_pipeline_config

# Configure logging
logging.basicConfig(
    level=logging.INFO,  # Set the logging level to INFO
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',  # Set the logging format
    handlers=[logging.StreamHandler()]  # Add a stream handler to print to console
)


class StockDataIngestor:
    def __init__(self,schedule_time,mongo_url, db_name, topics, kafka_config, catch_up):  

        self.current_date = pd.to_datetime('today').strftime('%Y-%m-%d')
        self.schedule_time = schedule_time
        self.catch_up = catch_up
        # Initialize the MongoDB client
        self.client = MongoClient(mongo_url)
        self.db = self.client[db_name]
        
        # set the collection and topics names
        self.topics = topics
        
        # Initialize the Kafka consumer
        self.kafka_config = kafka_config
        self.kafka_config["group.id"] = "my-consumer-group"
        self.kafka_config["auto.offset.reset"] = "latest"
        self.consumer = Consumer(self.kafka_config)
        
    def insert_data(self, collection_name, data):
        try:
            self.db[collection_name].insert_many(data)
        except Exception as e:
            logging.error(f"Error inserting data: {e}")
        
    def run(self):
        self.consumer.subscribe(topics=self.topics)
        batch = {}
        batch = {interval: batch.get(interval, []) for interval in self.topics}                
        batch_size = 5000
        ingesting = True
        try:
            while ingesting:
                    
                # Stops the consumer if trading is closed
                if datetime.now().time() > datetime.strptime("14:10", "%H:%M").time() and not self.catch_up:
                    ingesting = False
                    break
                
                msg = self.consumer.poll(1)
                if msg is None:
                    # Insert any remaining records in the batch
                    if any(len(records) > 0 for records in batch.values()):
                        for collection_name, records in batch.items():
                            if batch[collection_name]:
                                self.insert_data(collection_name, records)
                                logging.info(f"Inserted {len(records)} records into {collection_name}")
                                batch[collection_name] = []
                    else:
                        # Wait for new messages
                        logging.info("Waiting for ingesting data...")        
                        continue
                else:
                    try:
                        # Extract data from ConsumerRecord
                        deserialize_msg = json.loads(msg.value().decode('utf-8'))
                    except Exception as e:
                        logging.error(f"Error deserializing message: {e}")
                        continue
                    
                    # Extract the value from the ConsumerRecord
                    records = deserialize_msg if isinstance(deserialize_msg, list) else [deserialize_msg]
                    collection_name = msg.topic() # Kafka Topic name = collection name in mongdb database
                    # Convert 'date' field to datetime
                    for record in records:
                        if 'date' in record:
                            record['date'] = datetime.strptime(record['date'], '%Y-%m-%d')
                        elif 'datetime' in record:
                            record['datetime'] = datetime.strptime(record['datetime'], '%Y-%m-%d %H:%M:%S')
                            
                    # Append the records to the batch in the corresponding collection
                    batch[collection_name].extend(records)
                    # Insert the batch into the database if it reaches the batch size for the collection
                    if len(batch[collection_name]) >= batch_size:
                        self.insert_data(collection_name, batch[collection_name])
                        logging.info(f"Inserted {len(batch[collection_name])} records into {collection_name}")
                        batch[collection_name] = []

        except KeyboardInterrupt:
            logging.info("Closing consumer")
            
if __name__ == "__main__": 
    # Load the MongoDB configuration once
    mongo_config = load_mongo_config()
    mongo_url = mongo_config['url']
    db_name = mongo_config["db_name"]
    warehouse_interval = mongo_config["warehouse_interval"]
    warehouse_topics = [f"{interval}_data" for interval in warehouse_interval]
    streaming_topics = [f"{interval}_stock_datastream" for interval in mongo_config['streaming_interval']]
                                                                                    
    # Kafka configuration
    kafka_config = load_kafka_config()
    
    # Load data pipeline configuration
    catch_up = load_pipeline_config()['data_extract']['catch_up']
    ingestor = StockDataIngestor(schedule_time=None, 
                                mongo_url=mongo_url, 
                                db_name=db_name, 
                                topics=warehouse_topics,
                                kafka_config=kafka_config,
                                catch_up=catch_up)
    
    ingestor.run()