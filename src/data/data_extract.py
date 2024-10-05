from confluent_kafka import Producer, Consumer
import yfinance as yf
import json, schedule, time
import pandas as pd
from pymongo import MongoClient, DESCENDING
import traceback, logging
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

class StockDataExtractor:
    
    def __init__(self,symbols,mongo_url,db_name,streaming_interval,warehouse_interval,kafka_config):

        # Set the class variables
        self.symbols = symbols
        self.current_date = (pd.to_datetime("today")).strftime('%Y-%m-%d')
        self.last_fetch = {symbol: {interval: None for interval in streaming_interval} for symbol in self.symbols}
        self.streaming_interval = streaming_interval
        # Initialize the MongoDB client
        self.client = MongoClient(mongo_url)
        self.db = self.client[db_name]

        self.warehouse_collection_name = self.warehouse_topics = [f"{interval}_data" for interval in warehouse_interval]
        self.streaming_collection_name = self.streaming_topics = [f"{interval}_stock_datastream" for interval in streaming_interval]
        
        # Initialize the Kafka producer
        self.producer = Producer(kafka_config)
        
        # Create Time Series Collection if it does not exist
        self.create_collection_if_not_exists(self.warehouse_collection_name+self.streaming_collection_name)

        # Collection to be stored in MongoDB
        self.warehouse_collection_dict = {collection_name: (self.db[collection_name], collection_name.split('_')[0]) for collection_name in self.warehouse_collection_name}
        self.streaming_collection_dict = {collection_name: (self.db[collection_name], collection_name.split('_')[0]) for collection_name in self.streaming_collection_name}    
            
    def create_collection_if_not_exists(self, collection_names: list):
        
        for collection_name in collection_names:
            if collection_name not in self.db.list_collection_names():
                self.db.create_collection(
                    collection_name,
                    timeseries={
                        "timeField": "date" if 'datastream' not in collection_name else "datetime",  
                        "metaField": "symbol",  
                        "granularity": "hours" if 'datastream' not in collection_name else "minutes"
                    },
                    expireAfterSeconds= 60*60*24*365*5 if 'datastream' not in collection_name else None
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
                    
                    start_date = self.last_fetch[symbol][interval] or self.current_date
                    # Fetch data from Yahoo Finance
                    data = ticker.history(start=start_date, interval=interval).reset_index()
                    
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
                        # Update the last fetch time
                        self.last_fetch[symbol][interval] = pd.to_datetime('now')
                        # Flush the producer after all messages have been sent
                        self.producer.flush()
                        logging.info(f"Data for {symbol} until {stock_record['datetime']} sent successfully to {interval} stock_datastream")
                except Exception as e:
                    logging.error(f"Error fetching data for {symbol}: {e}")
                    traceback.print_exc()
                    
    def start_scheduled_datastream_consuming(self):
        
        # # Fetch data immediately for all intervals
        # self.fetch_and_produce_datastream()
            
        # trading = True
        # if pd.to_datetime('now').hour < 14:
        #     # Schedule datastream consuming tasks for different intervals
        #     for mintue in range(5, 60, 5):
        #         schedule.every().hour.at(f":{mintue:02d}").do(self.fetch_and_produce_datastream, interval='5m')
        #     for mintue in range(30, 60, 30):
        #         schedule.every().hour.at(f":{mintue:02d}").do(self.fetch_and_produce_datastream, interval='30m')
                
        #     schedule.every().hour.at(":00").do(self.fetch_and_produce_datastream, interval='60m')
            
        #     while trading:
        #         schedule.run_pending()
        #         time.sleep(1)
        #         if pd.to_datetime('now').hour >= 14:
        #             trading = False 
            
        #         #  End if trading hour is over
                
        #     if pd.to_datetime('now').hour >= 14:
        #         print("Trading hour is over!")
        #         time.sleep(5)
        #         # Consume and Ingest daily and weekly stock data
        self.fetch_and_produce_stock_data()
        logging.info(f"Scheduled fetching and producing stock data at {self.current_date} completed!")

if __name__ == "__main__": 
    # Load the MongoDB configuration
    mongo_url = load_mongo_config()['url']
    db_name = load_mongo_config()["db_name"]
    
    # Load the streaming and warehouse interval
    streaming_interval = load_mongo_config()["streaming_interval"]
    warehouse_interval = load_mongo_config()["warehouse_interval"]
    
    # Load the Kafka configuration
    kafka_config = load_kafka_config()
    # List of stock symbols
    stock_symbols = ["QQQ", "SPY", "SOXX", "DJIA", "IWM","NVDA", "AAPL", "TSLA", "MSFT", "AMZN"]
    
    # Initialize the StockDataExtractor
    extractor = StockDataExtractor(symbols=stock_symbols,
                                mongo_url=mongo_url,
                                db_name=db_name,
                                streaming_interval=streaming_interval,
                                warehouse_interval=warehouse_interval,
                                kafka_config=kafka_config)
    
    extractor.start_scheduled_datastream_consuming()

