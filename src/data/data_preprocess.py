import pymongo
import pandas as pd
import logging
from utils.alert_strategy import Alert
from utils.features_engineering import add_features

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class DataPreprocess:
    def __init__(self, mongo_uri="mongodb://localhost:27017/", 
                db_name="local", 
                price_collection_name=["daily_stock_price", "weekly_stock_price"],
                tech_collection_name="technical_stock_data"):
    
        # Initialize the MongoDB client
        self.client = pymongo.MongoClient(mongo_uri)
        self.db = self.client[db_name]
        
        # Set the collection names
        self.tech_collection_name = tech_collection_name
        self.price_collection_name = price_collection_name
        
        # Set the current date
        self.current_date = pd.to_datetime('today').strftime('%Y-%m-%d')
        
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
        logging.info(f"Connected to MongoDB: {mongo_uri}")
        
    def fetch_data(self, symbol, collection):
        query = self.db[collection].find({"symbol": symbol})
        df = pd.DataFrame(list(query))        
        return df
    
    def process_data(self, df):
        df = add_features(df).apply()
        df = Alert(df).add_alert()
        return df

    def insert_technical_data(self, symbol, df, interval):
        # Convert the DataFrame to a dictionary
        df_to_dict = df.to_dict(orient='records')
        # Add the symbol and interval metadata to each record and insert into MongoDB
        for record in df_to_dict:
            record["symbol"] = symbol
            record["interval"] = interval  # Add interval field
            record["timestamp"] = pd.to_datetime(record["date"]).to_pydatetime()  # Convert to Python datetime
            if isinstance(record["timestamp"], pd.Timestamp):
                record["timestamp"] = record["timestamp"].to_pydatetime()
            # Insert the record into the collection
            self.db[self.tech_collection_name].insert_one(record)
            logging.info(f"Inserted record for {symbol} at {record['timestamp']} with interval {interval}")

    def run(self):
        for collection in self.price_collection_name:
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
                print(latest_record)
                break
                if latest_record:
                    last_date_in_db = latest_record['date']
                    if last_date_in_db.strftime('%Y-%m-%d') == self.current_date:
                        logging.info(f"Data for {symbol} is up to date")
                        continue
                    else:
                        # Insert only the new records
                        new_records = processed_df[processed_df['date'] > last_date_in_db]
                        self.insert_technical_data(symbol, new_records, collection.split("_")[0])
                        
                if processed_df is not None:
                    self.insert_technical_data(symbol, processed_df, collection.split("_")[0])
                    added_technical += 1
                    logging.info(f"{(added_technical/total_symbols) * 100:.2f}% Technical data added successfully for {symbol}!")

# Example usage
if __name__ == "__main__":
    dp = DataPreprocess()
    dp.run()