from data_extract import StockDataExtractor
from data_ingest import StockDataIngestor
from data_preprocess import DataPreprocess
from datastream_transform import DataStreamProcess
import sys, os
# Add project root to sys.path dynamically
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))
from config.mongdb_config import load_mongo_config
from config.kafka_config import load_kafka_config
from config.data_pipeline_config import load_pipeline_config

def main():
    # Load the MongoDB configuration once
    mongo_config = load_mongo_config()
    mongo_url = mongo_config['url']
    db_name = mongo_config["db_name"]
    warehouse_interval = mongo_config["warehouse_interval"]
    warehouse_topics = [f"{interval}_data" for interval in warehouse_interval]

    # Kafka configuration
    kafka_config = load_kafka_config()
    
    ingestor = StockDataIngestor(schedule_time=None, 
                                kafka_config=kafka_config, 
                                mongo_config=mongo_config)
    ingestor.schedule_data_data_consumption()

    extractor = StockDataExtractor(mongo_url=mongo_url, 
                                db_name=db_name, 
                                collection_name=warehouse_topics)
    extractor.run()

    dp = DataPreprocess(mongo_uri=mongo_url, 
                        db_name=db_name, 
                        collection_name=warehouse_topics, 
                        tech_collection_name=mongo_config["process_collection_name"])
    dp.run()

    dsp = DataStreamProcess(mongo_uri=mongo_url, 
                            db_name=db_name, 
                            tech_collection_name=mongo_config["process_collection_name"])
    dsp.run()

if __name__ == "__main__":
    main()