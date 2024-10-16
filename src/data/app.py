import concurrent.futures
from data_extract import StockDataExtractor
from datastream_process import DataStreamProcess
from data_ingest import StockDataIngestor
from data_preprocess import DataPreprocess
from make_train import MakeTrainTestData
import sys, os, datetime, logging
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
    
    # Load run configurations
    catch_up = data_pipeline_config['data_extract']['catch_up']
    
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
                                kafka_config=kafka_config,
                                data_pipeline_config=data_pipeline_config)
    
    # Initialize the StockDataIngestor
    ingestor = StockDataIngestor(schedule_time=None,
                                mongo_url=mongo_url,
                                db_name=db_name,
                                topics=stock_symbols,
                                kafka_config=kafka_config,
                                catch_up=catch_up)
    
    # Initialize the DataPreprocess
    pre_processor = DataPreprocess(mongo_config, data_pipeline_config)
    
    # Initialize the MakeTrainTestData
    make_train_test = MakeTrainTestData(mongo_config, data_pipeline_config)
    
    # Initalize the DataStreamProcess
    datastream_live_process = DataStreamProcess(lookback=15, mongo_url=mongo_url, 
                                                db_name=db_name, 
                                                kafka_config=kafka_config, 
                                                datastream_topics=datastream_topics,
                                                live_alert_collection=live_alert_collection,
                                                batch_alert_collection=batch_alert_collection,
                                                catch_up=catch_up)
    
    
    # Execute functions in parallel and run the rest after extractor finishes
    with concurrent.futures.ThreadPoolExecutor() as executor:
        # Start the extractor, ingestor, and datastream_live_process in parallel
        futures = [
            executor.submit(datastream_live_process.run),
            executor.submit(extractor.run)
            
            ]
        # Wait for the extractor to finish
        for future in concurrent.futures.as_completed(futures):
            # Check if the completed future is the extractor
            if future.result() is None:  # Assuming the extractor function doesn't return anything
                # EL is done, so run the Transformation Steps
                ingestor.run()
                pre_processor.run()    
                make_train_test.run()
                logging.info(f"{current_time}: Data pipeline completed successfully!")
                break

if __name__ == "__main__":
    # Initialize the flag outside the loop
    message_printed = False
    catch_up = load_pipeline_config()['data_extract']['catch_up']
    if not catch_up:
        logging.info("Running the data pipeline in real-time mode...")
        while True:
            # Get the current time
            current_time = datetime.datetime.now().time()
            non_trading_hours = (current_time.hour >= 14 and current_time.minute >= 0 and current_time.second >= 0)
            
            # if (current_time.hour >= 7 and current_time.minute >= 29 and current_time.second >= 0) and not non_trading_hours:
            main()
            # # Check if the message has not been printed and print it once
            # if not message_printed:
            #     print("Waiting for the time to reach 7:29 AM...")
            #     message_printed = True 
                
                
    elif catch_up:
        logging.info("Running the data pipeline in catch-up mode...")
        main()