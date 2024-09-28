from kafka import KafkaConsumer
import json
import time
class datastream_viz:
    def __init__(self):        
        self.consumer = KafkaConsumer(bootstrap_servers='localhost:9092',
                                value_deserializer = lambda v: json.load(v.decode('utf-8')))    
    def run(self):
        self.consumer.subscribe()
        for message in self.consumer:
            print(message)
            print('\n')
            time.sleep(3)

if __name__ == '__main__':
    datastream_viz().run()    