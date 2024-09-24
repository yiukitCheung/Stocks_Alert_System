from kafka import KafkaProducer
import yfinance as yf
import json, schedule, time
import pandas as pd
from pymongo import MongoClient, DESCENDING

import logging

logging.basicConfig(level=logging.INFO)

class StockDataExtractor:
    
    def __init__(self, 
                symbols,
                mongo_uri='mongodb://localhost:27017/',
                kafka_server='localhost:9092',
                kafka_topic='stock_price',
                schedule_time="14:00",
                db_name='local',
                collection_name='historic_stock_price'):
        
        self.producer = KafkaProducer(bootstrap_servers=kafka_server,
                                    value_serializer=lambda v: json.dumps(v).encode('utf-8'))
        self.mongo_client = MongoClient(mongo_uri)
        self.db_name = db_name
        self.collection_name = collection_name
        self.topic = kafka_topic
        self.symbols = symbols  
        self.schedule_time = schedule_time

    def fetch_and_produce_stock_data(self):
        
        for symbol in self.symbols:
            try:
                collection = self.mongo_client[self.db_name][self.collection_name]
                ticker = yf.Ticker(symbol)
                
                if self.collection_name not in self.mongo_client[self.db_name].list_collection_names():
                    self.mongo_client[self.db_name].create_collection(self.collection_name,
                                                                    timeseries={
                                                                        "timeField": "date",
                                                                        "metaField": "symbol",
                                                                        "granularity": "hours"
                                                                    })
                    logging.info(f"Time Series Collection {self.collection_name} created successfully")
                    
                    time.sleep(3)
                    
                    collection = self.mongo_client[self.db_name][self.collection_name]
                                                                
                if not collection.find_one({"symbol": symbol}):
                    data = ticker.history(period='max').reset_index()
                else:
                    latest_record = collection.find_one({'symbol': symbol}, sort=[("date", DESCENDING)])
                    if latest_record:
                        last_date_in_db = pd.to_datetime(latest_record['date'])
                        data = ticker.history(start=last_date_in_db + pd.Timedelta(days=1)).reset_index()
                    else:
                        data = pd.DataFrame()

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
                        self.producer.send(self.topic, value=stock_record)

                    self.producer.flush()
                    logging.info(f"Data for {symbol} sent successfully")
                else:
                    logging.info(f"No new data available for {symbol}")

            except Exception as e:
                logging.error(f"Error fetching data for {symbol}: {e}")
    
    def schedule_data_fetching(self):
        if self.schedule_time:
            schedule.every().day.at(self.schedule_time).do(self.fetch_and_produce_stock_data)
            logging.info(f"Scheduled fetching and producing stock data at {self.schedule_time}")

            while True:
                schedule.run_pending()
                time.sleep(1)
        else:
            self.fetch_and_produce_stock_data()
            
# Usage example
if __name__ == "__main__":
    stock_symbols = [
        "NVDA", "TSLA", "META", "GOOGL", "PLTR", "GOOG", "BRK.B", "BRK.A", 
        "FSLR", "BSX", "GOLD", "EA", "INTU", "SHOP", "ADI", "RMD", 
        "ISRG", "ANET", "VRTX", "WELL", "WAB", "O", "AEM", "VICI", 
        "XYL", "IR", "AME", "VEEV", "FTV", "INFY", "KHC", "SAP", 
        "GRMN", "CP", "TCOM", "ALC", "TW", "EQR", "FAST", "ERIE",
        "TRI", "GIB", "CHT"  
    ]

    extractor = StockDataExtractor(symbols=stock_symbols, schedule_time=None)
    extractor.schedule_data_fetching()