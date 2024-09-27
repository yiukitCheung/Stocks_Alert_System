from kafka import KafkaProducer
import yfinance as yf
import json, schedule, time
import pandas as pd
from pymongo import MongoClient, DESCENDING
import traceback
import logging

logging.basicConfig(level=logging.INFO)

class StockDataExtractor:
    
    def __init__(self,symbols,schedule_time,
                mongo_uri="mongodb://localhost:27017/",
                db_name="local", 
                daily_collection_name="daily_stock_price",
                weekly_collection_name="weekly_stock_price"):

        # Set the class variables
        self.symbols = symbols
        self.schedule_time = schedule_time
        self.current_date = pd.to_datetime('today').strftime('%Y-%m-%d')
        
        # Initialize the MongoDB client
        self.client = MongoClient(mongo_uri)
        self.db = self.client[db_name]
        self.daily_collection_name = self.daily_topic_name = daily_collection_name
        self.weekly_collection_name = self.weekly_topic_name = weekly_collection_name
        
        # Initialize the Kafka producer
        self.producer = KafkaProducer(bootstrap_servers='localhost:9092',
                                    value_serializer=lambda v: json.dumps(v).encode('utf-8'))
        
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
                    
                    # If the symbol does not exist, fetch all data
                    if not collection.find_one({"symbol": symbol}):
                        data = ticker.history(period='max', interval=interval).reset_index()
                    else:
                        # If the symbol exists, fetch data from the last date in the database
                        logging.info(f"{symbol} already exists in the database")
                        latest_record = collection.find_one({'symbol': symbol}, sort=[("date", DESCENDING)])
                        if latest_record:
                            last_date_in_db = pd.to_datetime(latest_record['date'])
                            if last_date_in_db.strftime('%Y-%m-%d') == self.current_date:
                                logging.info(f"Data for {symbol} is up to date")
                                continue
                            else:
                                days = last_date_in_db  - self.current_date
                                data = ticker.history(start=last_date_in_db + pd.Timedelta(days=days), interval=interval).reset_index()
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
                            self.producer.send(topic=collection_name, value=stock_record)
                        
                        # Flush the producer after all messages have been sent
                        self.producer.flush()
                        logging.info(f"Data for {symbol} sent successfully to {collection_name}")
                        
                except Exception as e:
                    logging.error(f"Error fetching data for {symbol}: {e}")
                    traceback.print_exc()

    def fetch_and_produce_datastream(self):
        for interval in ['5m', '30m', '60m']:
            for symbol in self.symbols:
                try:
                    # Fetch data from Yahoo Finance
                    ticker = yf.Ticker(symbol)
                    data = ticker.history(start=self.current_date, 
                                        interval=interval)\
                                            .reset_index()

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
                                'volume': record['Volume']}
                            
                            # Send data to the respective topic
                            self.producer.send(topic=f'{interval}_stock_datastream', 
                                                keys = symbol.encdoe('utf-8'),
                                                value=stock_record)
                        
                        # Flush the producer after all messages have been sent
                        self.producer.flush()
                        logging.info(f"Data for {symbol} sent successfully to {interval} stock_datastream")
                        
                except Exception as e:
                    logging.error(f"Error fetching data for {symbol}: {e}")
                    traceback.print_exc()
                    
    def close_producer(self):
        if self.producer:
            
            self.producer.close()
            logging.info("Kafka producer closed successfully")
            
    def schedule_data_fetching(self):
        if self.schedule_time:
            schedule.every().day.at(self.schedule_time).do(self.fetch_and_produce_stock_data)
            logging.info(f"Scheduled fetching and producing stock data at {self.schedule_time}")

            while True:
                schedule.run_pending()
                time.sleep(1)
        else:
            self.fetch_and_produce_stock_data()
            
    def realtime_data_fetching(self):
        # Fetch data every 5 seconds
        while True:
            # assert pd.to_datetime('now').hour < 14, "Trading hour is over"
            
            self.fetch_and_produce_datastream()
            time.sleep(5)

            # End if trading hour is over
            if pd.to_datetime('now').hour >= 14:
                break
# Usage example
if __name__ == "__main__":
    
    stock_symbols = [
        "NVDA", "TSLA", "META", "GOOGL", "PLTR", "GOOG", 
        "FSLR", "BSX", "GOLD", "EA", "INTU", "SHOP", "ADI", "RMD", 
        "ISRG", "ANET", "VRTX", "WELL", "WAB", "O", "AEM", "VICI", 
        "XYL", "IR", "AME", "VEEV", "FTV", "INFY", "KHC", "SAP", 
        "GRMN", "CP", "TCOM", "ALC", "TW", "EQR", "FAST", "ERIE",
        "TRI", "GIB", "CHT"  
    ]
    
    extractor = StockDataExtractor(symbols=stock_symbols, schedule_time=None)
    
    try:
        extractor.realtime_data_fetching()
        extractor.schedule_data_fetching()
    finally:
        extractor.close_producer()