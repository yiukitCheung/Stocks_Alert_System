from confluent_kafka import Consumer
import json, pandas as pd
from utils.real_time_alert import CandlePattern
from utils.batch_alert import trend_pattern
import logging
from pymongo import MongoClient, DESCENDING, IndexModel
from datetime import datetime, timezone
import os, sys
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

class DataStreamProcess:
    def __init__(self, lookback, mongo_url, db_name, kafka_config, 
                datastream_topics, live_alert_collection, batch_alert_collection):
        
        # Initialize the batch
        self.batch = {}
        self.candle = {}
        self.last_processed_index = {}
        self.last_processed_interval = None
        self.window = lookback
        self.pointer_a = 0
        self.pointer_b = 1
        
        # Initialize the MongoDB client
        self.client = MongoClient(mongo_url)
        self.db = self.client[db_name]
        self.live_alert_collection = live_alert_collection
        self.batch_alert_collection = batch_alert_collection
        
        # Initialize the Kafka consumer
        self.kafka_config = kafka_config
        self.kafka_config["group.id"] = "my-consumer-group"
        self.kafka_config["auto.offset.reset"] = "latest"
        self.consumer = Consumer(self.kafka_config)
        
        # Set the topic name 
        self.datastream_topics = datastream_topics
        # Create collections if they do not exist
        self.create_time_series_collection(self.live_alert_collection)
        self.create_time_series_collection(self.batch_alert_collection)

    def create_time_series_collection(self, collection_name):
        if collection_name not in self.db.list_collection_names():
            self.db.create_collection(
                collection_name,
                timeseries={
                    "timeField": "datetime",  
                    "metaField": "symbol",  
                    "granularity": "hours"
                }
            )
            logging.info(f"Time Series Collection {collection_name} created successfully")
            
    def store_datastream(self, symbol, value, topic):
        
        # Ensure the date is in pandas datetime format for comparsion
        value['datetime'] = pd.to_datetime(value['datetime'])   
        # Fetch the last record for the given symbol
        last_record = list(self.db[topic].find({"symbol": symbol}).sort("datetime", DESCENDING).limit(1))
        # Insert tprint(last_record)he new record if the collection is empty or the new date is greater than the last date
        logging.info(f"Last record: {last_record} | Value: {value['datetime']}")
        if not last_record or last_record[0]['datetime'] < value['datetime']:
            self.db[topic].insert_one(value)
            logging.info(f"Inserted new record for {symbol} in {topic}")
        else:
            logging.info(f"Record for {symbol} in {topic} is not newer than the last record")
            
    def store_live_alert(self,symbol,value,interval):
        

        # Ensure the date is in datetime format
        value['datetime'] = pd.to_datetime(value['datetime'])

        # Fetch the last record for the given symbol
        last_record = list(self.db[self.live_alert_collection].find({"symbol": symbol, 
                                                                    "interval": interval})\
                                                                        .sort("date", DESCENDING).limit(1))
        
        # Ensure last_record[0]['date'] is timezone-aware
        if last_record:
            if last_record[0]['datetime'].tzinfo is None:
                last_record[0]['datetime'] = last_record[0]['datetime'].replace(tzinfo=timezone.utc)

        # Ensure value['date'] is timezone-aware
        if value['datetime'].tzinfo is None:
            value['datetime'] = value['datetime'].replace(tzinfo=timezone.utc)
            
        # Insert the new record if the collection is empty or the new date is greater than the last date
        if not last_record or last_record[0]['datetime'] < value['datetime']:
            self.db[self.live_alert_collection].insert_one(value)
            logging.info(f"Inserted new record for {symbol} in {self.live_alert_collection}")
        else:
            logging.info(f"Record for {symbol} in {self.live_alert_collection} is not newer than the last record")
            
    def store_batch_alert(self,symbol,value):
        
        # Ensure the date is in datetime format
        value['datetime'] = pd.to_datetime(value['datetime'])

        # Fetch the last record for the given symbol
        last_record = list(self.db[self.batch_alert_collection].find({"symbol": symbol}).sort("datetime", DESCENDING).limit(1))
        
        # Ensure last_record[0]['date'] is timezone-aware
        if last_record:
            if last_record[0]['datetime'].tzinfo is None:
                last_record[0]['datetime'] = last_record[0]['datetime'].replace(tzinfo=timezone.utc)
                last_record[0]['datetime'] = pd.to_datetime(last_record[0]['datetime'])
                
            print(last_record[0]['datetime'], value['datetime'])
                # Ensure value['datetime'] is timezone-aware
                
        if value['datetime'].tzinfo is None:
            value['datetime'] = value['datetime'].replace(tzinfo=timezone.utc)
        
        # Insert the new record if the collection is empty or the new date is greater than the last date
        if not last_record or last_record[0]['datetime'] < value['datetime']:
            self.db[self.batch_alert_collection].insert_one(value)
            logging.info(f"Inserted new record for {symbol} in {self.batch_alert_collection}")
        else:
            logging.info(f"Record for {symbol} in {self.batch_alert_collection} is not newer than the last record")
    
    def batch_process(self, symbol, records, interval):
        # Store price subsequently in a dict
        if symbol not in self.batch:
            self.batch[symbol] = []
            
        self.batch[symbol].append(records)
        
        # Check if we have enough data for batch processing
        if len(self.batch[symbol]) >= self.window:
            df = pd.DataFrame(self.batch[symbol])
        
            # Extract the strong support and resistance
            support = trend_pattern(lookback=self.window, batch_data=df).strong_support()
            resistance = trend_pattern(lookback=self.window, batch_data=df).strong_resistance()
            
            # Store Alert to MongoDB
            if len(support) != 0:
                for i in support.itertuples():
                    self.store_batch_alert(symbol, {'symbol': symbol,'interval': interval, 'datetime': i.datetime, 'support': i.low})    
            if len(resistance) != 0:
                for i in resistance.itertuples():
                    self.store_batch_alert(symbol, {'symbol': symbol,'interval': interval, 'datetime':  i.datetime, 'resistance': i.high})
            
            logging.info(f"Batch Alert for {symbol} sent to Kafka")
            
            # Clear the batch for the symbol after processing
            self.batch[symbol] = []
            
    def streaming_process(self, data, interval):
        # Store the processed index in a dict
        if data['symbol'] not in self.last_processed_index:
            self.last_processed_index[data['symbol']] = -1
            self.last_processed_interval = interval 
        
        # Store the candle data in a dict
        if data['symbol'] not in self.candle:
            self.candle[data['symbol']] = []
            
        self.candle[data['symbol']].append(data)

        # If interval changes, reset the last processed index
        if self.last_processed_interval != interval:
            self.last_processed_interval = interval
            self.pointer_a = 0
            self.pointer_b = 1
            for symbol in self.last_processed_index:
                self.last_processed_index[symbol] = -1
            
        # Loop through the candle data from the last processed index
        for i in range(self.last_processed_index[data['symbol']] + 1, len(self.candle[data['symbol']])):
            if self.last_processed_index[data['symbol']] == -1:
                
                self.pointer_a = 0
                self.pointer_b = 1
                curr_candle = self.candle[data['symbol']][self.pointer_a]
                candle_pattern = CandlePattern(curr_candle)
                hammer, hammer_date = candle_pattern.hammer_alert()
    
                if not candle_pattern.no_volume():
                    if hammer:
                        # Print the hammer candle
                        self.store_live_alert(data['symbol'], {'symbol': data['symbol'],'interval': interval, 'datetime': hammer_date, 'alert_type':'hammer'},
                                            interval)
                        logging.info(f"Hammer Alert for {data['symbol']} in interval {interval} sent to Kafka")
                else:
                    continue
            curr_candle = self.candle[data['symbol']][i]
            candle_pattern = CandlePattern(curr_candle)
            # Check if there are enough candles to compare
            
            if len(self.candle[data['symbol']]) > 1 and self.pointer_b < len(self.candle[data['symbol']]):
                pre_candle = self.candle[data['symbol']][self.pointer_a]
                curr_candle = self.candle[data['symbol']][self.pointer_b]
                
                # Check for bullish 0.382 candle patterns
                bullish_382, bullish_382_date = candle_pattern.hammer_alert()

                # Check for engulfing patterns
                bullish_engulfing, bearish_engulfing, date = candle_pattern.engulf_alert(pre_candle, curr_candle)
                
                # Store the alert to the mongoDB
                if not candle_pattern.no_volume():
                    if bearish_engulfing:
                        value =  {'symbol': data['symbol'],'interval': interval, 'datetime': date, 'alert_type':'bearish_engulfer'}
                        self.store_live_alert(data['symbol'], value, interval)
                        logging.info(f"Bearish Engulfing Alert for {data['symbol']} in interval {interval}  at {date} sent to Kafka")
                    elif bullish_engulfing:
                        value = {'symbol': data['symbol'],'interval': interval, 'datetime': date, 'alert_type':'bullish_engulfer'}
                        self.store_live_alert(data['symbol'], value, interval)
                        logging.info(f"Bullish Engulfing Alert for {data['symbol']} in interval {interval} at {date} sent to Kafka")
                    elif bullish_382:
                        value = {'symbol': data['symbol'],'interval': interval, 'datetime': bullish_382_date, 'alert_type':'bullish_382'}
                        self.store_live_alert(data['symbol'], value , interval)
                        logging.info(f"Bullish 0.382 Alert for {data['symbol']} in interval {interval} at {bullish_382_date} sent to Kafka")
                        
                # Update the last processed index for the symbol
                self.last_processed_index[data['symbol']] = self.pointer_b

            # Update the last processed index
            self.pointer_a += 1
            self.pointer_b += 1

            break
        
    def run(self):
        self.consumer.subscribe(topics=self.datastream_topics)
        transforming = True
        while transforming:
            msg = self.consumer.poll(0.1)
            if msg is None:
                logging.info("No new messages")
                continue
            deseralize_msg = json.loads(msg.value().decode('utf-8'))
            if msg.error():
                logging.error(f"Consumer error: {msg.error()}")
                continue
            
            # Extract the key and value from the ConsumerRecord
            symbol = deseralize_msg['symbol']
            value = deseralize_msg
            topic = msg.topic()
            interval = topic.split('_')[0]
            
            logging.info(f"Processing symbol: {symbol} in interval: {interval} at {value['datetime']}")
            # Store the data in MongoDB
            self.store_datastream(symbol, value, topic)
            # Process the record
            self.streaming_process(value, interval)
            self.batch_process(symbol=symbol, records=value, interval=interval)

if __name__ == '__main__':
    
    # Load the MongoDB configuration
    mongo_db_config = load_mongo_config()
    url = mongo_db_config['url']
    db_name = mongo_db_config['db_name']
    datastream_topics =  [f"{interval}_stock_datastream" for interval in mongo_db_config['streaming_interval']]
    live_alert_collection = mongo_db_config['alert_collection_name']['live']
    batch_alert_collection = mongo_db_config['alert_collection_name']['batch']
    
    # Load the Kafka configuration
    kafka_config = load_kafka_config()
    
    datastream = DataStreamProcess(lookback=15, mongo_url=url, 
                                db_name=db_name, 
                                kafka_config=kafka_config, 
                                datastream_topics=datastream_topics,
                                live_alert_collection=live_alert_collection,
                                batch_alert_collection=batch_alert_collection)
    
    datastream.run()