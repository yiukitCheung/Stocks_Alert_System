import pymongo
import time
from pymongo import UpdateOne
import pandas as pd
import logging
from utils.alert_strategy import AddAlert
from utils.features_engineering import add_features
import sys,os,yaml
# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
# Add project root to sys.path dynamically
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))
from config.mongdb_config import load_mongo_config
from config.data_pipeline_config import load_pipeline_config

class DataPreprocess:
    def __init__(self, mongo_config, data_pipeline_config):
        # Load configurations
        self.mongo_url = mongo_config['url']
        self.db_name = mongo_config['db_name']
        self.collection_name = [f"{interval}_data" for interval in mongo_config['warehouse_interval']]
        self.tech_collection_name = mongo_config['process_collection_name']
        self.alert_collection_name = mongo_config['alert_collection_name']['long_term']
        
        self.data_pipeline_config = data_pipeline_config
        self.new_intervals = data_pipeline_config['data_preprocess']['new_intervals']
        self.velocity_dict = {}
        
        # Initialize the MongoDB client
        self.client = pymongo.MongoClient(self.mongo_url)
        self.db = self.client[self.db_name]
        
        # Set the current date
        self.current_date = pd.to_datetime('today').strftime('%Y-%m-%d')
        
        # Initialize the dict to store the processed data
        self.processed_data_dict = {}
        
        # Initialize the dict to store the alerts
        self.alert_dict = {}

        if self.tech_collection_name not in self.db.list_collection_names():
            self.create_collection(self.tech_collection_name)
        if self.alert_collection_name not in self.db.list_collection_names():
            self.create_collection(self.alert_collection_name)
            
    def create_collection(self, collection_name):
        """
        Create a new collection in the MongoDB database.
        
        :param collection_name: The name of the collection to create.
        """
        self.db.create_collection(collection_name)
        logging.info(f"Collection {collection_name} created.")
        time.sleep(2)
        
    def fetch_data(self, symbol, collection):
        query = self.db[collection].find({"symbol": symbol},{'_id': 0})
        df = pd.DataFrame(list(query))        
        df = df.sort_values(by='date')
        return df
    
    def process_data(self, df: pd.DataFrame):
        df = add_features(df).apply()
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
        new_df = new_df.reset_index()
        new_df = new_df.dropna()
        new_df['interval'] = interval
        
        return new_df
    
    def insert_technical_data(self, collection, symbol, df, interval):

        # Check for last record in the collection
        last_record = self.db[collection].find_one({"symbol": symbol, "interval": interval},
                                                                sort=[('date', pymongo.DESCENDING)])
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
            new_records_df["date"] = pd.to_datetime(new_records_df["date"])
            self.db[collection].insert_many(new_records_df.to_dict(orient='records'))
            logging.info(f"Inserted {len(new_records_df)} records into {self.tech_collection_name} collection")
            
    def insert_alert_dict(self, collection_name, symbol, symbol_alert_dict, interval):
        """
        Insert technical data into MongoDB, ensuring alerts are stored as an array, skipping duplicates, and inserting only missing dates.

        :param collection_name: The name of the MongoDB collection where data will be inserted.
        :param symbol: The stock symbol (e.g., "NVDA").
        :param symbol_alert_dict: The dictionary containing the alerts and other technical data.
        :param interval: The interval of the data (e.g., daily, weekly).
        """
        collection = self.db[collection_name]  # Access the collection
        
        # Step 1: Fetch existing dates for the given symbol and interval from the collection
        existing_dates = collection.distinct(
            'alerts.date',  # Extract distinct dates
            {
                'symbol': symbol,
                'alerts.interval': interval  # Match documents by symbol and interval
            }
        )

        # Step 2: Prepare the data for insert_many
        documents_to_insert = []
        for record in symbol_alert_dict[symbol]:
            date = record['date']
            # If the date doesn't already exist in the database for this symbol and interval, add it to the insert list
            if date not in existing_dates:
                record['symbol'] = symbol  # Add symbol to the record
                record['interval'] = interval  # Add interval to the record (if not already)
                documents_to_insert.append(record)

        # Step 3: Insert the list of documents using insert_many (only if there are new documents to insert)
        if documents_to_insert:
            result = collection.insert_many(documents_to_insert)
            print(f"Inserted {len(result.inserted_ids)} new documents for {symbol}.")
        else:
            print(f"No new data to insert for {symbol}.")
            
    def run(self):
        # Step 1. Create new interval data base on the daily data, process and store in a dict for each symbol
        distinct_symbols = self.db[self.collection_name[0]].distinct('symbol')
        for symbol in distinct_symbols:
            df = self.fetch_data(symbol, self.collection_name[0])
            for interval in self.new_intervals:
                new_df = self.create_new_interval_data(df, f"{interval}D")  
                new_df = self.process_data(new_df)    
                # Step 2. Insert the processed data into the alert collection
                self.insert_technical_data( self.tech_collection_name, symbol, new_df, interval)                
                # Store the processed data in a dictionary
                if symbol not in self.processed_data_dict:
                    self.processed_data_dict[symbol] = [new_df]
                elif symbol in self.processed_data_dict:
                    self.processed_data_dict[symbol].append(new_df)
                
                logging.info(f"Created new data for {symbol} on interval {interval}")
        
        # Step 3. Create alerts for each symbol on the new intervals
        for symbol, symbol_list in self.processed_data_dict.items():
            df = pd.concat(symbol_list)
            df.reset_index(drop=True, inplace=True)
            symbol_dict = df.to_dict(orient='records')
            for interval in self.new_intervals:
                logging.info(f"Creating alerts for {symbol} on interval {interval}...")
                symbol_alert_dict = AddAlert(symbol_dict, interval=interval, symbol=symbol).apply()
                # Step 4. Insert the alerts into the alert collection
                self.insert_alert_dict(self.alert_collection_name, symbol, symbol_alert_dict, interval)
                
if __name__ == "__main__":
    # Load the MongoDB configuration once
    mongo_config = load_mongo_config()
    data_pipeline_config = load_pipeline_config()

    dp = DataPreprocess(mongo_config, data_pipeline_config)
    dp.run()