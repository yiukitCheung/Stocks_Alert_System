
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

class DataPreprocess:
    def __init__(self, mongo_url, db_name, collection_name : list, tech_collection_name):
    
        # Initialize the MongoDB client
        self.client = pymongo.MongoClient(mongo_url)
        self.db = self.client[db_name]
        
        # Set the collection names
        self.tech_collection_name = tech_collection_name
        self.collection_name = collection_name
        
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
        query = self.db[collection].find({"symbol": symbol})
        df = pd.DataFrame(list(query))        
        df = df.sort_values(by='date')
        return df
    
    def process_data(self, df):
        df = add_features(df).apply()
        df = Alert(df).add_alert()
        return df

    def insert_technical_data(self, symbol, df, interval):
        # Convert the DataFrame to a dictionary
        df_to_dict = df.to_dict(orient='records')
        
        # Check for last record in the collection
        last_record = self.db[self.tech_collection_name].find_one({"symbol": symbol, "interval": interval},
                                                                sort=[('timestamp', pymongo.DESCENDING)])
        if last_record:
            last_date_in_db = last_record['date']
            if last_date_in_db.strftime('%Y-%m-%d') == self.current_date:
                logging.info(f"Data for {symbol} is up to date")
                return
            new_records_df = df[df['date'] > last_date_in_db]
        else:
            new_records_df = df

        if not new_records_df.empty:
            new_records_df["symbol"] = symbol
            new_records_df["interval"] = interval
            new_records_df["timestamp"] = pd.to_datetime(new_records_df["date"]).to_pydatetime()
            self.db[self.tech_collection_name].insert_many(new_records_df.to_dict(orient='records'))
            logging.info(f"Inserted {len(new_records_df)} records into {self.tech_collection_name} collection")

    def run(self):
        
        for collection in self.collection_name:
            
            all_symbols = self.db[collection].distinct('symbol')
            total_symbols = len(all_symbols)
            
            added_technical = 0
            for symbol in all_symbols:
                logging.info(f"Processing symbol {symbol}...")
                stock_df = self.fetch_data(symbol, collection)
                processed_df = self.process_data(stock_df)
                logging.info(f"Processing symbol {symbol} completed!")
                # Check latest record in the technical collection
                latest_record = self.db[self.tech_collection_name]\
                    .find_one({"symbol": symbol, "interval": collection.split("_")[0]}, 
                            sort=[('timestamp', pymongo.DESCENDING)])
                if latest_record:
                    last_date_in_db = latest_record['date']
                    # Check if the data is up to date
                    if last_date_in_db.strftime('%Y-%m-%d') == self.current_date:
                        logging.info(f"Data for {symbol} is up to date")
                        continue
                    new_records = processed_df[processed_df['date'] > last_date_in_db]
                # If the symbol does not exist in the technical collection
                else:
                    new_records = processed_df
                if not new_records.empty:
                    self.insert_technical_data(symbol, new_records, collection.split("_")[0])
                    added_technical += 1
                    logging.info(f"{(added_technical/total_symbols) * 100:.2f}% Technical data added successfully for {symbol}!")

# Example usage
if __name__ == "__main__":
    # Load the MongoDB configuration once
    mongo_config = load_mongo_config()
    mongo_url = mongo_config['url']
    db_name = mongo_config["db_name"]
    collection_name = [f"{interval}_data" for interval in mongo_config["warehouse_interval"]]
    processed_collection_name = mongo_config["process_collection_name"]
    dp = DataPreprocess(mongo_url=mongo_url, 
                        db_name=db_name, 
                        collection_name=collection_name, 
                        tech_collection_name=processed_collection_name)
    dp.run()