import streamlit
from kafka import KafkaConsumer,KafkaProducer
import json, pandas as pd
from utils.real_time_alert import CandlePattern
from utils.batch_alert import trend_pattern

class DataStreamProcess:
    def __init__(self, lookback):
        # Initialize the batch
        self.batch = []

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
        self.topics = [interval for interval in ['5m_stock_datastream',
                                                '30m_stock_datastream',
                                                '60m_stock_datastream']]
    def batch_process(self):
    
        df = pd.DataFrame(self.batch)
        # Extract the strong support and resistance
        support = trend_pattern(lookback=self.window, batch_data=df).strong_support()
        resistance = trend_pattern(lookback=self.window, batch_data=df).strong_resistance()

        # Produce the alert to the Kafka topic
        self.produicer.send(self.stock_batch_alert_topic, 
                        value={'support': support, 'resistance': resistance})
        
    def streaming_process(self,data):
        
        # Convert the data to a DataFrame
        candle = pd.DataFrame(data)
        # Extract the previous and current candle
        candle_pattern = CandlePattern(candle)
        bullish_382 = candle_pattern.bullish_382_alert()
        
        # Extract the previous and current candle
        if candle['datetime'].dt.hour == 9 and candle['datetime'].dt.minute == 30:
            pre_candle = candle
            
        else:
            curr_candle = candle
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
        self.consumer.subscribe(topics=self.topics)
        while True:
            messages = self.consumer.poll(timeout_ms=1000)
            for topic_partition, consumer_records in messages.items():
                # Extract the value from the ConsumerRecord
                records = [record.value for record in consumer_records]
                self.streaming_process(data=records)
                if len(self.batch) == self.window :
                    self.batch_process()
                    # Clear the batch
                    self.batch = []
                else:                   
                    self.batch.extend(records) # Append the records to the batch
                    
                self.producer.send(topic_partition.topic, key=topic_partition.key, value=topic_partition.value)
                
if __name__ == '__main__':
    datastream = DataStreamProcess(lookback=15)
    datastream.fetch_datastream()