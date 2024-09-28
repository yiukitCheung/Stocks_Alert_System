import streamlit
from kafka import KafkaConsumer,KafkaProducer
import json, pandas as pd
from utils.real_time_alert import CandlePattern
from utils.batch_alert import trend_pattern
import logging

class DataStreamProcess:
    def __init__(self, lookback):
        # Initialize the batch
        self.batch = {}

        self.window = lookback
        # Initialize the Kafka consumer
        self.consumer = KafkaConsumer(bootstrap_servers='localhost:9092',
                                    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
                                    key_deserializer=lambda v: v.decode('utf-8'))
        # Initalize the Kafka producer
        self.producer = KafkaProducer(bootstrap_servers='localhost:9092',
                                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                                    key_serializer=lambda v: v.encode('utf-8'))
        # Set the topic name 
        self.stock_live_alert_topic = 'stock_live_alert'
        self.stock_batch_alert_topic = 'stock_batch_alert'
        self.datastream_topics = ['processed_5m_stock_datastream',
                                'processed_30m_stock_datastream',
                                'processed_60m_stock_datastream']
        
    def batch_process(self, symbol, records):
        # Store price subsequently in a dict
        if symbol not in self.batch:
            self.batch[symbol] = []
        self.batch[symbol].append(records)
        
        # Check if we have enough data for batch processing
        if len(self.batch[symbol]) >= self.window:
            df = pd.DataFrame(self.batch[symbol])
            print(df)
            # Extract the strong support and resistance
            support = trend_pattern(lookback=self.window, batch_data=df).strong_support()
            resistance = trend_pattern(lookback=self.window, batch_data=df).strong_resistance()
            
            # Produce the alert to the Kafka topic
            self.producer.send(self.stock_batch_alert_topic, 
                            value={'symbol': symbol, 'support': support, 'resistance': resistance})
            logging.info(f"Batch Alert for {symbol} sent to Kafka")
            # Clear the batch for the symbol after processing
            self.batch[symbol] = []
        
    def streaming_process(self,data):
        
        # Convert the data to a DataFrame
        candle = pd.DataFrame(data)
        # Extract the previous and current candle
        if candle['datetime'].dt.hour == 9 and candle['datetime'].dt.minute == 30:
            pre_candle = candle
            
        else:
            curr_candle = candle
            candle_pattern = CandlePattern(candle)
            bullish_382 = candle_pattern.bullish_382_alert()
            bullish_engulfing, bearish_engulfing = candle_pattern.engulf_alert(pre_candle, curr_candle)
            pre_candle = curr_candle
            
            # Produce the alert to the Kafka topic
            if bullish_382:
                self.producer.send(self.stock_live_alert_topic, 
                                value={'bullish_382': True})
            elif bullish_engulfing:
                self.producer.send(self.stock_live_alert_topic, 
                                value={'bullish_engulfing': True})
            elif bearish_engulfing:
                self.producer.send(self.stock_live_alert_topic, 
                                value={'bearish_engulfing': True})
        
    def fetch_and_transform_datastream(self):
        self.consumer.subscribe(topics=set(self.datastream_topics))
        while True:
            messages = self.consumer.poll(timeout_ms=1000)
            for topic_partition, consumer_records in messages.items():
                # Extract the value from the ConsumerRecord
                symbol = topic_partition.key
                records = [record.value for record in consumer_records]
                
                # self.streaming_process(data=records, symbol=symbol)
                self.batch_process(symbol=symbol, records=records)
                
if __name__ == '__main__':
    datastream = DataStreamProcess(lookback=15)
    datastream.fetch_and_transform_datastream()