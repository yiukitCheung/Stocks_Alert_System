
import pymongo
import pandas as pd
import logging
from utils.alert_strategy import AddAlert, VelocityFinder
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
        self.alert_df = pd.DataFrame()
        
        # Ensure technical data collection is time-series
        if self.tech_collection_name not in self.db.list_collection_names():
            logging.info(f"{self.tech_collection_name} collection does not exist")
            self.db.create_collection(
                self.tech_collection_name,
                timeseries={
                    "timeField": "date",  
                    "metaField": "symbol",  
                    "granularity": "hours"
                }
            )
            logging.info(f"{self.tech_collection_name} collection created")
            
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
            
    def run(self):
        # Step 1. Create new interval data base on the daily data, process and store in a dict for each symbol
        distinct_symbols = self.db[self.collection_name[0]].distinct('symbol')
        for symbol in distinct_symbols:
            df = self.fetch_data(symbol, self.collection_name[0])
            for interval in self.new_intervals:
                new_df = self.create_new_interval_data(df, f"{interval}D")  
                new_df = self.process_data(new_df)    
                self.insert_technical_data( self.tech_collection_name, symbol, new_df, interval)                
                # Store the processed data in a dictionary
                if symbol not in self.processed_data_dict:
                    self.processed_data_dict[symbol] = [new_df]
                elif symbol in self.processed_data_dict:
                    self.processed_data_dict[symbol].append(new_df)
                
                logging.info(f"Created new data for {symbol} on interval {interval}")
        
        # Step 2. Create alerts for each symbol on the new intervals
        for symbol, symbol_list in self.processed_data_dict.items():
            
            df = pd.concat(symbol_list)
            df.reset_index(drop=True, inplace=True)
            df = VelocityFinder(df).run()
            
            for interval in self.new_intervals:
                logging.info(f"Creating alerts for {symbol} on interval {interval}...")
                alert_df = AddAlert(df, interval=interval).apply()
                self.alert_df = pd.concat([self.alert_df, alert_df])
                logging.info(f"Added alerts for {symbol} on interval {interval}") 
                
                # Step 3. Insert the alerts into the alert collection
                self.insert_technical_data(self.alert_collection_name,symbol, alert_df, interval)
                logging.info(f"Inserted alerts for {symbol} on interval {interval}")
                
if __name__ == "__main__":
    # Load the MongoDB configuration once
    mongo_config = load_mongo_config()
    data_pipeline_config = load_pipeline_config()

    dp = DataPreprocess(mongo_config, data_pipeline_config)
    dp.run()