from confluent_kafka import Producer, Consumer
import yfinance as yf
import json, schedule, time
import pandas as pd
from pymongo import MongoClient, DESCENDING
import traceback, logging


# Configure logging
logging.basicConfig(
    level=logging.INFO,  # Set the logging level to INFO
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',  # Set the logging format
    handlers=[logging.StreamHandler()]  # Add a stream handler to print to console
)

class kafka_config:
    def read_config():
        config = {}
        with open("client.properties") as fh:
            for line in fh:
                line = line.strip()
                if len(line) != 0 and line[0] != "#":
                    parameter, value = line.strip().split('=', 1)
                    config[parameter] = value.strip()
        return config
    
class StockDataExtractor:
    
    def __init__(self,symbols,
                mongo_uri="mongodb://localhost:27017/",
                db_name="local", 
                daily_collection_name="daily_stock_price",
                weekly_collection_name="weekly_stock_price"
                ):

        # Set the class variables
        self.symbols = symbols
        self.current_date = (pd.to_datetime("today")).strftime('%Y-%m-%d')
        self.last_fetch = {symbol: {interval: None for interval in ['5m', '30m', '60m']} for symbol in self.symbols}
        
        # Initialize the MongoDB client
        self.client = MongoClient(mongo_uri)
        self.db = self.client[db_name]
        self.daily_collection_name = self.daily_topic_name = daily_collection_name
        self.weekly_collection_name = self.weekly_topic_name = weekly_collection_name
        
        # Initialize the Kafka producer
        self.kafka_config = kafka_config.read_config()
        self.producer = Producer(self.kafka_config)
        
        # Create Time Series Collection if it does not exist
        self.create_collection_if_not_exists(self.daily_collection_name)
        self.create_collection_if_not_exists(self.weekly_collection_name)

        # Set the collection
        self.daily_collection = self.db[self.daily_collection_name]
        self.weekly_collection = self.db[self.weekly_collection_name]
        
        # Collection to be stored in MongoDB
        self.collection_dict = {self.daily_collection_name: (self.daily_collection, '1d'), 
                                self.weekly_collection_name: (self.weekly_collection, '1wk')}
        
        print(f"Connected to MongoDB: {mongo_uri}")
        
    def create_collection_if_not_exists(self, collection_name):
        if collection_name not in self.db.list_collection_names():
            self.db.create_collection(
                collection_name,
                timeseries={
                    "timeField": "date",  
                    "metaField": "symbol",  
                    "granularity": "hours"
                }
            )
            logging.info(f"Time Series Collection {collection_name} created successfully")
            time.sleep(3)
            
    def fetch_and_produce_stock_data(self):
        for collection_name, (collection, interval) in self.collection_dict.items():
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
                            serilized_record = json.dumps(stock_record)
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
    
    def fetch_and_produce_datastream(self, interval):
        for symbol in self.symbols:
            try:
                start_date = self.last_fetch[symbol][interval] or self.current_date
                # Fetch data from Yahoo Finance
                ticker = yf.Ticker(symbol)
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
                        self.producer.produce(topic=f'{interval}_stock_datastream', 
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
                    
    def close_producer(self):
        if self.producer:
            
            self.producer.close()
            logging.info("Kafka producer closed successfully")
            
    def start_scheduled_datastream_consuming(self):
        
        # # Fetch data immediately for all intervals
        # for interval in ['5m', '30m', '60m']:
        #     self.fetch_and_produce_datastream(interval)
            
        trading = True
        if pd.to_datetime('now').hour < 14:
            # Schedule datastream consuming tasks for different intervals
            for mintue in range(5, 60, 5):
                schedule.every().hour.at(f":{mintue:02d}").do(self.fetch_and_produce_datastream, interval='5m')
            for mintue in range(30, 60, 30):
                schedule.every().hour.at(f":{mintue:02d}").do(self.fetch_and_produce_datastream, interval='30m')
                
            schedule.every().hour.at(":00").do(self.fetch_and_produce_datastream, interval='60m')
            
            while trading:
                schedule.run_pending()
                time.sleep(1)
                if pd.to_datetime('now').hour >= 14:
                    trading = False 
            
                #  End if trading hour is over
                
        if pd.to_datetime('now').hour >= 14:
            print("Trading hour is over!")
            time.sleep(5)
            # Consume and Ingest daily and weekly stock data
            self.fetch_and_produce_stock_data()
            logging.info(f"Scheduled fetching and producing stock data at {self.current_date} completed!")
    
            
# Usage example
if __name__ == "__main__":
    
    stock_symbols = [
        "NVDA", "TSLA", "META", "GOOGL", "PLTR", "GOOG", 
        "FSLR", "BSX", "GOLD", "EA", "INTU", "SHOP", "ADI", "RMD", 
        "ISRG", "ANET", "VRTX", "WELL", "WAB", "O", "AEM", "VICI", 
        "XYL", "IR", "AME", "VEEV", "FTV", "INFY", "KHC", "SAP", 
        "GRMN", "CP", "TCOM", "ALC", "TW", "EQR", "FAST", "ERIE",
        "TRI", "GIB", "CHT"]
    
    extractor = StockDataExtractor(symbols=stock_symbols)
    extractor.start_scheduled_datastream_consuming()