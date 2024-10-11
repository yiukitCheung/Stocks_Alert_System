from confluent_kafka import Producer, Consumer
import confluent_kafka.admin
import yfinance as yf
import json, schedule, time
import pandas as pd
from pymongo import MongoClient, DESCENDING
import traceback, logging
import sys, os, yaml
# Add project root to sys.path dynamically
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))
from config.mongdb_config import load_mongo_config
from config.kafka_config import load_kafka_config
from config.data_pipeline_config import load_pipeline_config
from confluent_kafka import admin
import confluent_kafka

# Configure logging
logging.basicConfig(
    level=logging.INFO,  # Set the logging level to INFO
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',  # Set the logging format
    handlers=[logging.StreamHandler()]  # Add a stream handler to print to console
)

class StockDataExtractor:
    
    def __init__(self,symbols,mongo_url,db_name,streaming_interval,warehouse_interval,kafka_config,data_pipeline_config):

        # Set the reset and catch up flags
        self.reset = data_pipeline_config['data_extract']['reset']
        self.catch_up = data_pipeline_config['data_extract']['catch_up']
        
        # Set the class variables
        self.symbols = symbols
        self.current_date = pd.to_datetime("today")
        self.last_fetch = {symbol : {interval: None for interval in streaming_interval} for symbol in symbols} if self.reset else data_pipeline_config['data_extract']['last_fetch_records']
        self.streaming_interval = streaming_interval
        self.window_size = data_pipeline_config['data_extract']['window_size']
        self.expireAfterSeconds = data_pipeline_config['data_extract']['expireAfterSeconds']
        
        # Initialize the MongoDB client
        self.client = MongoClient(mongo_url)
        self.db = self.client[db_name]

        self.warehouse_collection_name = self.warehouse_topics = [f"{interval}_data" for interval in warehouse_interval]
        self.streaming_collection_name = self.streaming_topics = [f"{interval}_stock_datastream" for interval in streaming_interval]
        
        # Initialize the Kafka producer
        self.producer = Producer(kafka_config)
        
        # Create Time Series Collection if it does not exist
        self.createTimeSeriesCollection(self.streaming_collection_name, expireAfterSeconds=self.expireAfterSeconds)
        self.createTimeSeriesCollection(self.warehouse_collection_name)
        
        # Delete kafka topics
        self.delete_kafka_topics(self.streaming_collection_name) if self.reset else None
        # Create kafka topics
        self.create_kafka_topic(self.streaming_collection_name) if self.reset else None
        
        # Collection to be stored in MongoDB
        self.warehouse_collection_dict = {collection_name: (self.db[collection_name], collection_name.split('_')[0]) for collection_name in self.warehouse_collection_name}
        self.streaming_collection_dict = {collection_name: (self.db[collection_name], collection_name.split('_')[0]) for collection_name in self.streaming_collection_name}    
        
    def delete_kafka_topics(self, topics):
        # Initialize the Kafka Admin Client
        admin_client = confluent_kafka.admin.AdminClient(kafka_config)
        
        # Delete topics
        fs = admin_client.delete_topics(topics, operation_timeout=30)
        
        # Wait for each operation to finish
        for topic, f in fs.items():
            try:
                f.result()  # The result itself is None
                logging.info(f"Kafka topic {topic} deleted successfully")
            except Exception as e:
                logging.error(f"Failed to delete topic {topic}: {e}")
            time.sleep(1)
            
    def create_kafka_topic(self, topics):
        # Initialize the Kafka Admin Client
        admin_client = confluent_kafka.admin.AdminClient(kafka_config)
        new_topics = [confluent_kafka.admin.NewTopic(topic, num_partitions=6, replication_factor=3) for topic in topics]
        # Create topics
        fs = admin_client.create_topics(new_topics)
        # Wait for each operation to finish
        for topic, f in fs.items():
            try:
                f.result()  # The result itself is None
                logging.info(f"Kafka topic {topic} created successfully")
            except Exception as e:
                logging.error(f"Failed to create topic {topic}: {e}")
            time.sleep(1)
            
    def createTimeSeriesCollection(self, collection_names: list, expireAfterSeconds=None):
        
        for collection_name in collection_names:
            if collection_name not in self.db.list_collection_names():
                self.db.create_collection(
                    collection_name,
                    timeseries={
                        "timeField": "date" if 'datastream' not in collection_name else "datetime",  
                        "metaField": "symbol",  
                        "granularity": "hours" if 'datastream' not in collection_name else "minutes"
                    },
                    expireAfterSeconds = expireAfterSeconds if 'datastream' not in collection_name else None
                )
                logging.info(f"Time Series Collection {collection_name} created successfully")
                time.sleep(3)
            
    def fetch_and_produce_stock_data(self):
        for collection_name, (collection, interval) in self.warehouse_collection_dict.items():
            for symbol in self.symbols:
                try:
                    # Fetch data from Yahoo Finance
                    ticker = yf.Ticker(symbol)
                    
                    # Fetch all data if symbol does not exist in the collection
                    if not collection.find_one({"symbol": symbol}):
                        data = ticker.history(period='max', interval=interval).reset_index()
                    else:
                        # If the symbol exists, fetch data from the last date in the database
                        logging.info(f"{symbol} already exists in the database")
                        latest_record = collection.find_one({'symbol': symbol}, sort=[("date", DESCENDING)])
                        if latest_record:
                            last_date_in_db = pd.to_datetime(latest_record['date']).strftime('%Y-%m-%d')
                            if last_date_in_db == self.current_date:
                                logging.info(f"Data for {symbol} is up to date")
                                continue
                            else:
                                # Fetch only the missing data
                                data = ticker.history(start=last_date_in_db, interval=interval).reset_index()
                                
                                logging.info(f"Fetching data for {symbol} from {last_date_in_db}")
                        else:
                            logging.error(f"Error fetching data for {symbol}")
                            continue

                    # Produce data to Kafka
                    if not data.empty: 
                        for _, record in data.iterrows():
                            stock_record = {
                                'symbol': symbol,
                                'date': record['Date'].strftime('%Y-%m-%d'),
                                'open': record['Open'],
                                'high': record['High'],
                                'low': record['Low'],
                                'close': record['Close'],
                                'volume': record['Volume']
                            }
                            # Serialize the record
                            serilized_record = json.dumps(stock_record)
                            # Send data to the respective topic
                            self.producer.produce(topic=collection_name, value=serilized_record)
            
                        # Flush the producer after all messages have been sent
                        self.producer.flush()
                        # Log the success message
                        logging.info(f"Data for {symbol} until {stock_record['date']} sent successfully to {collection_name}")                     
                except Exception as e:
                    logging.error(f"Error fetching data for {symbol}: {e}")
                    traceback.print_exc()
                # Check the last reocrd date in the symbol
                logging.info(f"Fetching data for {symbol} completed!")
    
    def fetch_and_produce_datastream(self):
        for topic_name, (_, interval) in self.streaming_collection_dict.items():
            for symbol in self.symbols:
                try:
                    ticker = yf.Ticker(symbol)
                    if self.reset:
                        start_date = self.current_date - pd.Timedelta(days=self.window_size[interval]) 
                        start_date = pd.to_datetime(start_date).strftime('%Y-%m-%d')
                    elif not self.catch_up:
                        # Set the start date to prescribed window size
                        start_date = self.current_date - pd.Timedelta(days=self.window_size[interval]) \
                            if self.last_fetch[symbol][interval] is None \
                                else self.last_fetch[symbol][interval]
                        # Increment the start date by one day to fetch today data
                        start_date = (pd.to_datetime(start_date))
                    elif self.catch_up:
                        start_date = self.current_date - pd.Timedelta(days=self.window_size[interval])
                        
                    # Fetch data from Yahoo Finance
                    data = ticker.history(start=start_date, interval=interval).reset_index()
                    if self.reset:
                        data = data[pd.to_datetime(data['Datetime']) >= start_date]
                    elif not self.catch_up:
                        # Fetch only the new data compared to the last fetch time
                        last_fetch_time = pd.to_datetime(self.last_fetch[symbol][interval]).tz_localize('America/New_York')
                        data = data[data['Datetime'] >= last_fetch_time]
                        
                    # Produce data to Kafka
                    if not data.empty:
                        for _, record in data.iterrows():
                            stock_record = {
                                'symbol': symbol,
                                'datetime': record['Datetime'].strftime('%Y-%m-%d %H:%M:%S'),
                                'open': record['Open'],
                                'high': record['High'],
                                'low': record['Low'],
                                'close': record['Close'],
                                'volume': record['Volume']
                            }
                            
                            serialized_record = json.dumps(stock_record)
                            serialized_key = symbol.encode('utf-8')
                            # Send data to the respective topic
                            self.producer.produce(topic=topic_name, 
                                                    key=serialized_key,
                                                    value=serialized_record)
                            # Log the success message
                            logging.info(f"Data for {symbol} at {stock_record['datetime']} sent successfully to {topic_name}")
                            
                        # Update the last fetch time within data fetching loop
                        self.last_fetch[symbol][interval] = stock_record['datetime']
                        
                        # Flush the producer after all messages have been sent
                        self.producer.flush()
                        
                except Exception as e:
                    logging.error(f"Error fetching data for {symbol}: {e}")
                    traceback.print_exc()

                # Write the last fetch record to data pipeline config file for future reference
                with open('config/data_pipeline_config.yaml', 'r') as f:
                    config = yaml.safe_load(f) or {}
                    
                config['data_pipeline']['data_extract']['last_fetch_records'] = self.last_fetch
                
                with open('config/data_pipeline_config.yaml', 'w') as f:
                    yaml.safe_dump(config, f)
                    
        # Set Catch up to False after the first fetch
        self.catch_up = False
        
    def run(self):
        trading = True
        current_hour = pd.to_datetime('now').hour
        # self.fetch_and_produce_datastream()
        # self.fetch_and_produce_stock_data()
        # Only set up schedules if before 14:00
        if current_hour < 14:
            # Loop through the minute intervals starting at 05 and incrementing by 5
            for minute in range(5, 60, 5):
                schedule.every().hour.at(f":{minute:02d}").do(self.fetch_and_produce_datastream)
            while trading:
                schedule.run_pending()
                time.sleep(1)
                if pd.to_datetime('now').hour >= 14:
                    trading = False 
            
            logging.info("Trading hour is over!")
            time.sleep(5)
            # Consume and Ingest daily and weekly stock data
            self.fetch_and_produce_stock_data()
            logging.info(f"Scheduled fetching and producing stock data at {self.current_date} completed!")
        else:
            logging.info("Trading hour is over! Wait for the next trading day")
            time.sleep(1)
            
if __name__ == "__main__": 
    # Load Data Pipeline Configuration
    data_pipeline_config = load_pipeline_config()
    
    # Load the MongoDB configuration
    mongo_url = load_mongo_config()['url']
    db_name = load_mongo_config()["db_name"]
    
    # Load the streaming and warehouse interval
    streaming_interval = load_mongo_config()["streaming_interval"]
    warehouse_interval = load_mongo_config()["warehouse_interval"]
    
    # Load the Kafka configuration
    kafka_config = load_kafka_config()
    # List of stock symbols
    stock_symbols = load_pipeline_config()['data_ingest']['desried_symbols']  # List of stock symbols
    
    # Initialize the StockDataExtractor
    extractor = StockDataExtractor(symbols=stock_symbols,
                                mongo_url=mongo_url,
                                db_name=db_name,
                                streaming_interval=streaming_interval,
                                warehouse_interval=warehouse_interval,
                                kafka_config=kafka_config,
                                data_pipeline_config=data_pipeline_config)

    extractor.run()

