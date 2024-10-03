import streamlit
from confluent_kafka import Consumer, KafkaException
import json, pandas as pd
from utils.real_time_alert import CandlePattern
from utils.batch_alert import trend_pattern
import logging
from pymongo import MongoClient, DESCENDING
from datetime import datetime, timezone
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,  # Set the logging level to INFO
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',  # Set the logging format
    handlers=[logging.StreamHandler()]  # Add a stream handler to print to console
)
class kafka_config:
    @staticmethod
    def read_config():
        config = {}
        root_dir = os.path.dirname(os.path.abspath(__file__))
        client_properties_path = os.path.join(root_dir,"client.properties")
        print(client_properties_path)
        with open(client_properties_path) as fh:
            for line in fh:
                line = line.strip()
                if len(line) != 0 and line[0] != "#":
                    parameter, value = line.strip().split('=', 1)
                    config[parameter] = value.strip()
        return config

class mongo_config:
    @staticmethod
    def read_config():
        config = {}
        with open('mongo.properties') as fh:
            for line in fh:
                line = line.strip()
                if len(line) != 0 and line[0] != "#":
                    parameter, value = line.strip().split('=', 1)
                    config[parameter] = value.strip()
        return config
class DataStreamProcess:
    def __init__(self, lookback, 
                mongo_uri=mongo_config.read_config()['mongo_uri'],
                db_name="streaming_data"):
        
        # Initialize the batch
        self.batch = {}
        self.candle = {}
        self.last_processed_index = {}
        self.last_processed_interval = None
        self.window = lookback
        self.pointer_a = 0
        self.pointer_b = 1
        
        # Initialize the MongoDB client
        self.client = MongoClient(mongo_uri, maxPoolSize=50, minPoolSize=1)
        self.db = self.client[db_name]
        
        # Initialize the Kafka consumer
        self.kafka_config = kafka_config.read_config()
        self.kafka_config["group.id"] = "my-consumer-group"
        self.kafka_config["auto.offset.reset"] = "latest"
        self.consumer = Consumer(self.kafka_config)
        
        # Set the topic name 
        self.stock_live_alert_topic = 'stock_live_alert'
        self.stock_batch_alert_topic = 'stock_batch_alert'
        self.datastream_topics = ['5m_stock_datastream',
                                '30m_stock_datastream',
                                '60m_stock_datastream']
        
    def store_datastream(self, symbol, value, topic):
        if topic not in self.db.list_collection_names():
            self.db.create_collection(
                topic,
                timeseries={
                    "timeField": "date",  
                    "metaField": "symbol",  
                    "granularity": "hours"
                }
            )
            logging.info(f"Time Series Collection {topic} created successfully")
            
        # Ensure the date is in datetime format and timezone-aware
        value['datetime'] = pd.to_datetime(value['datetime']).tz_localize('UTC')

        # Ensure the 'date' field is present and contains a valid BSON UTC datetime value
        if 'date' not in value or not isinstance(value['date'], datetime):
            value['date'] = value['datetime'].to_pydatetime()
        # Fetch the last record for the given symbol
        last_record = list(self.db[topic].find({"symbol": symbol}).sort("date", DESCENDING).limit(1))
        
        # Ensure last_record[0]['date'] is timezone-aware
        if last_record:
            if last_record[0]['date'].tzinfo is None:
                last_record[0]['date'] = last_record[0]['date'].replace(tzinfo=timezone.utc)

        # Ensure value['date'] is timezone-aware
        if value['date'].tzinfo is None:
            value['date'] = value['date'].replace(tzinfo=timezone.utc)
            
        # Insert tprint(last_record)he new record if the collection is empty or the new date is greater than the last date
        logging.info(f"Last record: {last_record} | Value: {value['date']}")
        if not last_record or last_record[0]['date'] < value['date']:
            
            self.db[topic].insert_one(value)
            logging.info(f"Inserted new record for {symbol} in {topic}")
        else:
            logging.info(f"Record for {symbol} in {topic} is not newer than the last record")
            
    def store_live_alert(self,symbol,value,interval):
        
        if "stock_live_alert" not in self.db.list_collection_names():
            self.db.create_collection(
                "stock_live_alert",
                timeseries={
                    "timeField": "date",  
                    "metaField": "symbol",  
                    "granularity": "hours"
                }
            )
            logging.info(f"Time Series Collection stock_live_alert created successfully")
        
        # Ensure the date is in datetime format
        value['datetime'] = pd.to_datetime(value['datetime'])

        # Ensure the 'date' field is present and contains a valid BSON UTC datetime value
        if 'date' not in value or not isinstance(value['datetime'], datetime):
            value['date'] = value['datetime'].to_pydatetime()

        # Fetch the last record for the given symbol
        last_record = list(self.db["stock_live_alert"].find({"symbol": symbol, 
                                                            "interval": interval}).sort("date", DESCENDING).limit(1))
        
        # Ensure last_record[0]['date'] is timezone-aware
        if last_record:
            if last_record[0]['date'].tzinfo is None:
                last_record[0]['date'] = last_record[0]['date'].replace(tzinfo=timezone.utc)

        # Ensure value['date'] is timezone-aware
        if value['date'].tzinfo is None:
            value['date'] = value['date'].replace(tzinfo=timezone.utc)
            
        # Insert the new record if the collection is empty or the new date is greater than the last date
        if not last_record or last_record[0]['date'] < value['date']:
            self.db["stock_live_alert"].insert_one(value)
            logging.info(f"Inserted new record for {symbol} in stock_live_alert")
        else:
            logging.info(f"Record for {symbol} in stock_live_alert is not newer than the last record")
            
    def store_batch_alert(self,symbol,value):
        if "stock_batch_alert" not in self.db.list_collection_names():
            self.db.create_collection(
                "stock_batch_alert",
                timeseries={
                    "timeField": "date",  
                    "metaField": "symbol",  
                    "granularity": "hours"
                }
            )
            logging.info(f"Time Series Collection stock_batch_alert created successfully")
        
        # Ensure the date is in datetime format
        value['datetime'] = pd.to_datetime(value['datetime'])

        # Ensure the 'date' field is present and contains a valid BSON UTC datetime value
        if 'date' not in value or not isinstance(value['datetime'], datetime):
            value['date'] = value['datetime'].to_pydatetime()

        # Fetch the last record for the given symbol
        last_record = list(self.db["stock_batch_alert"].find({"symbol": symbol}).sort("date", DESCENDING).limit(1))
        
        # Ensure last_record[0]['date'] is timezone-aware
        if last_record:
            if last_record[0]['date'].tzinfo is None:
                last_record[0]['date'] = last_record[0]['date'].replace(tzinfo=timezone.utc)

        # Ensure value['date'] is timezone-aware
        if value['date'].tzinfo is None:
            value['date'] = value['date'].replace(tzinfo=timezone.utc)
            
        # Insert the new record if the collection is empty or the new date is greater than the last date
        if not last_record or last_record[0]['date'] < value['date']:
            self.db["stock_batch_alert"].insert_one(value)
            logging.info(f"Inserted new record for {symbol} in stock_batch_alert")
        else:
            logging.info(f"Record for {symbol} in stock_batch_alert is not newer than the last record")
    
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
        
    def fetch_and_transform_datastream(self):
        self.consumer.subscribe(topics=self.datastream_topics)
        while True:
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
            
            # Store the data in MongoDB
            self.store_datastream(symbol, value, topic)
            # Process the record
            self.streaming_process(value, interval)
            self.batch_process(symbol=symbol, records=value, interval=interval)
            
if __name__ == '__main__':
    datastream = DataStreamProcess(lookback=15)
    datastream.fetch_and_transform_datastream()