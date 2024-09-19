from kafka import KafkaConsumer
import pandas as pd
import ta
import schedule
import time
import yfinance as yf
from features_engineering import add_features


# Process the data
def process(df):
    # Filter out irrelevant features
    df = df.loc[:, ['Date', 'Open', 'High', 'Low', 'Close','Volume']]
    # Reset Index as date
    df = df.set_index('Date')
    
    return df

# Initialize the Kafka consumer
consumer = KafkaConsumer('my_topic', bootstrap_servers='localhost:9092')

# Function to consume data and add technical indicators
def consume_and_add_technical():
    
    # Initialize the Kafka consumer
    consumer = KafkaConsumer('my_topic', 
                            bootstrap_servers='localhost:9092',
                            value_deserializer=lambda v: pd.loads(v.decode('utf-8'))
                            )
    
    # Collect and add technical indicators to the data
    stock_data = []
    
    for message in consumer:
        stock_data.append(message.value)
        
        # Convert the data to a DataFrame
        df = pd.DataFrame(stock_data)
        
        # Process the data
        df = process(df)

        # Add features
        df = add_features(df).apply()
    
# Schedule the fetching and producing of stock data
schedule.every().day.at("14:01").do(consume_and_add_technical)
print("Scheduled consuming and process feteched stock data at 14:00")

while True:
    schedule.run_pending() 
    time.sleep(1)