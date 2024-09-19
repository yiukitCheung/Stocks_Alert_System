from kafka import KafkaProducer
import yfinance as yf
import json
import schedule
import time

# Initialize the Kafka producer
producer = KafkaProducer(bootstrap_servers='localhost:9092',
                        value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# Fundemental Good Stocks
stock_symbols = [
    "NVDA", "TSLA", "META", "GOOGL", "PLTR", "GOOG", "BRK.B", "BRK.A", 
    "FSLR", "BSX", "GOLD", "EA", "INTU", "SHOP", "ADI", "RMD", 
    "ISRG", "ANET", "VRTX", "WELL", "WAB", "O", "AEM", "VICI", 
    "XYL", "IR", "AME", "VEEV", "FTV", "INFY", "KHC", "SAP", 
    "GRMN", "CP", "TCOM", "ALC", "TW", "EQR", "FAST", "ERIE",
    "TRI", "GIB", "CHT"  
]

# Function to get the stock data
def fetch_and_produce_stock_data(symbols,kafka_producer):
    # Fetch the stock data
    for symbol in symbols:
        
        try:
            # Fetch the stock data
            x = yf.Ticker(f"{symbol}")
            x = x.history(period='max').reset_index()
            
            # Produce the stock data by row
            for record in x.iterrows():
                
                stock_record = {
                    'symbol': symbol,
                    'date': record[1]['Date'].strftime('%Y-%m-%d'),
                    'open': record[1]['Open'],
                    'high': record[1]['High'],
                    'low': record[1]['Low'],
                    'close': record[1]['Close'],
                    'volume': record[1]['Volume']
                    }
                # Send the stock data to Kafka
                kafka_producer.send('stock_price', value = stock_record)
                
            # Ensure all message are sent
            kafka_producer.flush()
    
            print(f"Data for {symbol} sent successfully")   
            
        except Exception as e:
            print(f"Error fetching data for {symbol}: {e}")
            

# Schedule the fetching and producing of stock data
schedule.every().day.at("14:00").do(fetch_and_produce_stock_data, 
                                    symbol=stock_symbols, 
                                    kafka_producer=producer)

print("Scheduled fetching and producing of stock data at 14:00")

# keep the script running
fetching = True
while fetching:
    # schedule.run_pending()
    fetch_and_produce_stock_data(stock_symbols,producer)
    fetching = False
