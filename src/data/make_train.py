from utils.train_data_loading import fetch_and_split_data, prepare_data
import sys, os
# Add project root to sys.path dynamically
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))
from config.mongdb_config import load_mongo_config
from config.data_pipeline_config import load_pipeline_config

def main():
    symbols = load_pipeline_config()['data_ingest']['desried_symbols']
    mongodb_config = load_mongo_config()
    data_pipeline_config = load_pipeline_config()
    
    for symbol in symbols:
        print(f"Making Train/Test Data for {symbol}")
        data_loader = fetch_and_split_data(symbol=symbol, 
                                        mongodb_config=mongodb_config)
        data_loader.fetch_data()
        data_loader.split_data()
        train_df = data_loader.get_train_data()
        
        data_preparation = prepare_data(symbol=symbol,data_pipeline_config=data_pipeline_config)
        data_preparation.preprocess(train_df)
    
if __name__ == "__main__":
    main()