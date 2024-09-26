import streamlit
from kafka import KafkaConsumer

class DataStreamProcess:
    def __init__(self):
        self.batch = []
        self.topics = [for interval in ['5m_stock_datastream',
                                        '30m_stock_datastream',
                                        '60m_stock_datastream']]
        # Initialize the Kafka consumer
        self.consumer = KafkaConsumer(bootstrap_servers='localhost:9092',
                                    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
                                    key_deserializer=lambda v: v.decode('utf-8'))
    def batch_process(self):
        if len(self.batch) > 10:
            self.display_data(self.batch)
            self.batch = []
        else:
            pass
    
    def streaming_process(self):
        pass
    def fetch_datastream(self):
        self.consumer.subscribe(topics=self.topics)
        while True:
            messages = self.consumer.poll(timeout_ms=1000)
            for topic_partition, consumer_records in messages.items():
                # Extract the value from the ConsumerRecord
                records = [record.value for record in consumer_records]
                self.batch.extend(records) # Append the records to the batch

            self.batch_process()
            self.streaming_process()
        
    def display_data(self, data):
        streamlit.write(data)