
import pymongo
import pandas as pd
import logging
from utils.alert_strategy import Alert
from utils.features_engineering import add_features
import sys,os
# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
# Add project root to sys.path dynamically
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))
from config.mongdb_config import load_mongo_config
from config.data_pipeline_config import load_pipeline_config
class DataPreprocess:
    def __init__(self, mongo_url, db_name, collection_name : list, tech_collection_name, new_intervals):
    
        # Initialize the MongoDB client
        self.client = pymongo.MongoClient(mongo_url)
        self.db = self.client[db_name]
        
        # Set the collection names
        self.tech_collection_name = tech_collection_name
        self.collection_name = collection_name
        
        # Set the new interval desried
        self.new_intervals = new_intervals
        # Set the current date
        self.current_date = pd.to_datetime('today').strftime('%Y-%m-%d')
        
        # Initialize the batch list
        self.batch = []
        
        # Ensure technical data collection is time-series
        if self.tech_collection_name not in self.db.list_collection_names():
            self.db.create_collection(
                self.tech_collection_name,
                timeseries={
                    "timeField": "timestamp",  
                    "metaField": "symbol",  
                    "granularity": "hours"
                }
            )
        
        logging.info(f"{self.tech_collection_name} collection exists")
        
    def fetch_data(self, symbol, collection):
        query = self.db[collection].find({"symbol": symbol},{'_id': 0})
        df = pd.DataFrame(list(query))        
        df = df.sort_values(by='date')
        return df
    
    def process_data(self, df):
        df = add_features(df).apply()
        df = Alert(df).add_alert()
        return df
    
    def create_new_interval_data(self,df, interval):
        # Ensure date in datetime format
        df['date'] = pd.to_datetime(df['date'])
        # Set the date as the index before grouping
        df = df.set_index('date')
        # Group the data by the new interval
        new_df = df.groupby([pd.Grouper(freq=interval), 'symbol']).agg({
            'open': 'first',
            'high': 'max', 
            'low': 'min', 
            'close': 'last', 
            'volume': 'sum'
        })
        # Reset the index
        new_df.reset_index(inplace=True)
        new_df = new_df.dropna()
        
        return new_df
    
    def insert_technical_data(self, symbol, df, interval):

        # Check for last record in the collection
        last_record = self.db[self.tech_collection_name].find_one({"symbol": symbol, "interval": interval},
                                                                sort=[('timestamp', pymongo.DESCENDING)])
        if last_record:
            last_date_in_db = pd.to_datetime(last_record['date']).strftime('%Y-%m-%d')
            if last_date_in_db == self.current_date:
                logging.info(f"Data for {symbol} is up to date")
                return
            new_records_df = df[df['date'] > last_date_in_db]
        else:
            new_records_df = df

        if not new_records_df.empty:
            new_records_df["symbol"] = symbol
            new_records_df["interval"] = interval
            new_records_df["timestamp"] = pd.to_datetime(new_records_df["date"])
            self.db[self.tech_collection_name].insert_many(new_records_df.to_dict(orient='records'))
            logging.info(f"Inserted {len(new_records_df)} records into {self.tech_collection_name} collection")

    def process_fetched(self):
        
        for collection in self.collection_name:
            
            all_symbols = self.db[collection].distinct('symbol')
            total_symbols = len(all_symbols)
            
            added_technical = 0
            for symbol in all_symbols:
                logging.info(f"Processing symbol {symbol}...")
                stock_df = self.fetch_data(symbol, collection)
                processed_df = self.process_data(stock_df)
                logging.info(f"{symbol} data processing completed!")
                # Check latest record in the technical collection
                latest_record = self.db[self.tech_collection_name]\
                    .find_one({"symbol": symbol, "interval": collection.split("_")[0]}, 
                            sort=[('timestamp', pymongo.DESCENDING)])
                logging.info(f"The latest record for {symbol} in the database is {latest_record['timestamp'] if latest_record else None}")
                
                if latest_record:
                    # Ensure the date is in pandas datetime format
                    last_date_in_db = pd.to_datetime(latest_record['timestamp'])
                    current_date_dt = pd.to_datetime(self.current_date)
                    
                    # Check if the data is up to date
                    if last_date_in_db == current_date_dt:
                        logging.info(f"Data for {symbol} is up to date")
                        continue
                    else:
                        # Take only the fetched record is newer
                        new_records = processed_df[processed_df['date'] > last_date_in_db]
                # If the symbol does not exist in the technical collection
                else:
                    new_records = processed_df
                if not new_records.empty:
                    self.insert_technical_data(symbol, new_records, collection.split("_")[0])
                    added_technical += 1
                    logging.info(f"{(added_technical/total_symbols) * 100:.2f}% Technical data added successfully for {symbol}!")
                    
    def process_made(self):
        distinct_symbols = self.db[self.tech_collection_name].distinct('symbol')
        for symbol in distinct_symbols:
            df = self.fetch_data(symbol, self.collection_name[0])       
            for interval in self.new_intervals:
                logging.info(f"Adding new interval {interval} for symbol {symbol}...")
                new_df = self.create_new_interval_data(df,interval)
                new_df = self.process_data(new_df)
                logging.info(f"Processing completed for {symbol} in {interval} interval")
                
                # Check if the data is already in the collection
                last_record = self.db[self.tech_collection_name].find_one({"symbol": symbol, "interval": interval},
                                                                    sort=[('timestamp', pymongo.DESCENDING)])
                
                if last_record:
                    # Ensure the date is in pandas datetime format
                    last_date_in_db = pd.to_datetime(last_record['timestamp']).strftime('%Y-%m-%d')
                    current_date_dt = pd.to_datetime(self.current_date)
                    # Check if the data is up to date
                    if last_date_in_db == current_date_dt:
                        logging.info(f"The made {interval} Data for {symbol} is up to date")
                        continue
                    else:
                        # Take only the fetched record is newer
                        new_records_df = new_df[new_df['date'] > last_date_in_db]
                # If the symbol does not exist in the technical collection
                else:
                    new_records_df = new_df
                # Insert the new interval data accordingly
                if not new_records_df.empty:
                    self.insert_technical_data(symbol, new_records_df, interval)
                    logging.info(f"Inserted new interval data for {symbol} in {interval} interval")
    def run(self):
        # Step 1: Process the fetched data
        # self.process_fetched()
        # Step 2: Process the made data
        self.process_made()

if __name__ == "__main__":
    # Load the MongoDB configuration once
    mongo_config = load_mongo_config()
    data_pipeline_config = load_pipeline_config()
    mongo_url = mongo_config['url']
    db_name = mongo_config["db_name"]
    collection_name = [f"{interval}_data" for interval in mongo_config["warehouse_interval"]]
    processed_collection_name = mongo_config["process_collection_name"]
    new_intervals = data_pipeline_config['data_ingest']['new_intervals']
    
    dp = DataPreprocess(mongo_url=mongo_url, 
                        db_name=db_name, 
                        collection_name=collection_name, 
                        tech_collection_name=processed_collection_name,
                        new_intervals=new_intervals)
    dp.run()