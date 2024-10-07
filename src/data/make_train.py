from utils.train_data_loading import fetch_and_split_data, prepare_data
import sys, os
# Add project root to sys.path dynamically
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))
from config.mongdb_config import load_mongo_config
from config.data_pipeline_config import load_pipeline_config

class MakeTrainTestData:
    def __init__(self, mongo_config, data_pipeline_config):
        self.symbols = data_pipeline_config['data_ingest']['desried_symbols']
        self.mongodb_config = mongo_config
        self.data_pipeline_config = data_pipeline_config

    def run(self):
        for symbol in self.symbols:
            print(f"Making Train/Test Data for {symbol}")
            data_loader = fetch_and_split_data(symbol=symbol, 
                                            mongodb_config=self.mongodb_config)
            print(f"Fetching {symbol} Data")
            data_loader.fetch_data()
            print("Splitting Data")
            data_loader.split_data()
            print("Getting Train Data")
            train_df = data_loader.get_train_data()
            print("Getting Test Data")
            test_df = data_loader.get_test_data()
            data_preparation = prepare_data(symbol=symbol, 
                                            data_pipeline_config=self.data_pipeline_config, 
                                            mongodb_config=self.mongodb_config)
            print("Saving train and test set to MongoDB")
            data_preparation.preprocess(train_df)
            data_preparation.preprocess(test_df, train=False)
            print(f"{symbol} Data Saved to MongoDB Successfully")

if __name__ == "__main__":
    mongo_config = load_mongo_config()
    data_pipeline_config = load_pipeline_config()
    
    train_data_maker = MakeTrainTestData(mongo_config, data_pipeline_config)
    train_data_maker.run()