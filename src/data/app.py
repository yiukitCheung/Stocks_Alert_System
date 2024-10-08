import concurrent.futures
from data_extract import StockDataExtractor
from data_ingest import StockDataIngestor
from data_preprocess import DataPreprocess
from make_train import MakeTrainTestData
from datastream_transform import DataStreamProcess
import sys, os
# Add project root to sys.path dynamically
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))
from config.mongdb_config import load_mongo_config
from config.kafka_config import load_kafka_config   
from config.data_pipeline_config import load_pipeline_config

def main():
    # Load the MongoDB, Kafka, and data pipeline configuration
    mongo_config = load_mongo_config()
    data_pipeline_config = load_pipeline_config()
    kafka_config = load_kafka_config()
    
    # Extract the MongoDB configuration
    mongo_url = mongo_config['url']
    db_name = mongo_config["db_name"]
    
    # Extract the streaming and warehouse interval
    streaming_interval = mongo_config["streaming_interval"]
    warehouse_interval = mongo_config["warehouse_interval"]
    datastream_topics =  [f"{interval}_stock_datastream" for interval in mongo_config['streaming_interval']]
    live_alert_collection = mongo_config['alert_collection_name']['live']
    batch_alert_collection = mongo_config['alert_collection_name']['batch']
    
    # List of stock symbols
    stock_symbols = load_pipeline_config()['data_ingest']['desried_symbols']
    
    # Initialize the StockDataExtractor
    extractor = StockDataExtractor(symbols=stock_symbols,
                                mongo_url=mongo_url,
                                db_name=db_name,
                                streaming_interval=streaming_interval,
                                warehouse_interval=warehouse_interval,
                                kafka_config=kafka_config)
    
    # Initialize the StockDataIngestor
    ingestor = StockDataIngestor(schedule_time='15:00',
                                mongo_url=mongo_url,
                                db_name=db_name,
                                topics=stock_symbols,
                                kafka_config=kafka_config)
    
    # Initialize the DataPreprocess
    collection_name = [f"{interval}_data" for interval in mongo_config["warehouse_interval"]]
    processed_collection_name = mongo_config["process_collection_name"]
    
    pre_processor = DataPreprocess(mongo_url=mongo_url, 
                        db_name=db_name, 
                        collection_name=collection_name, 
                        tech_collection_name=processed_collection_name)
    
    # Initialize the MakeTrainTestData
    make_train_test = MakeTrainTestData(mongo_config=load_mongo_config, data_pipeline_config=data_pipeline_config)
    
    # Initalize the DataStreamProcess
    datastream_live_process = DataStreamProcess(lookback=15, mongo_url=mongo_url, 
                                                db_name=db_name, 
                                                kafka_config=kafka_config, 
                                                datastream_topics=datastream_topics,
                                                live_alert_collection=live_alert_collection,
                                                batch_alert_collection=batch_alert_collection)
    
    # Execute functions in parallel and run the rest after extractor finishes
    with concurrent.futures.ThreadPoolExecutor() as executor:
        # Start the extractor, ingestor, and datastream_live_process in parallel
        futures = [
            executor.submit(datastream_live_process.fetch_and_transform_datastream()),
            executor.submit(extractor.start_scheduled_datastream_consuming())
            ]
        # Wait for the extractor to finish
        for future in concurrent.futures.as_completed(futures):
            # Check if the completed future is the extractor
            if future.result() is None:  # Assuming the extractor function doesn't return anything
                # Extractor is done, so run the remaining steps
                ingestor.schedule_data_data_consumption()
                pre_processor.run()    
                make_train_test.run()
                break
if __name__ == "__main__":
    main()