import pandas as pd
import pymongo

from pymongo.errors import BulkWriteError

from utils.alert_strategy import Alert
from utils.features_engineering import add_features

class TrainDataMaker:
    def __init__(self, mongo_uri="mongodb://localhost:27017/", 
                db_name="local", 
                price_collection_name="historic_stock_price",
                tech_collection_name="technical_stock_data"):
        
        self.client = pymongo.MongoClient(mongo_uri)
        self.db = self.client[db_name]
        self.tech_collection_name = tech_collection_name
        self.price_collection_name = price_collection_name
        
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
        
        print(f"Connected to MongoDB: {mongo_uri}")
        
    def fetch_data(self,symbol):
        query = self.db[self.price_collection_name].find({"symbol": symbol})
        df = pd.DataFrame(list(query))        
        return df
    
    def process_data(self, df):
        
        df = add_features(df).apply()
        df = Alert(df).add_alert()
        
        return df

    def insert_technical_data(self, symbol, df):

        # Convert the DataFrame to a dictionary
        df_to_dict = df.to_dict(orient='records')
        # Add the symbol metadata to each record
        for record in df_to_dict:
            record["symbol"] = symbol
            record["timestamp"] = record["date"].to_pydatetime()  # Convert to Python datetime
            self.db[self.tech_collection_name].insert_one(record) 
            
    def run(self):
        
        all_symbols = self.db[self.price_collection_name].distinct('symbol')
        total_symbols = len(all_symbols)
        added_technical = 0
        
        for symbol in all_symbols:
            print(f"Processing symbol {symbol}...")
            stock_df = self.fetch_data(symbol)
            processed_df = self.process_data(stock_df)
            print(f"Processing symbol {symbol} completed!")
            if processed_df is not None:
                self.insert_technical_data(symbol, processed_df)
                added_technical += 1
                print(f"{(added_technical/total_symbols) * 100:.2f}% Technical data added successfully for {symbol}!")

if __name__ == "__main__":
    maker = TrainDataMaker()
    maker.run()