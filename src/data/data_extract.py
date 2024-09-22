from kafka import KafkaProducer
import yfinance as yf
import json
import schedule
import time

class StockDataExtractor:
    def __init__(self, kafka_servers, topic, symbols, schedule_time="14:00"):
        self.producer = KafkaProducer(bootstrap_servers=kafka_servers,
                                    value_serializer=lambda v: json.dumps(v).encode('utf-8'))
        self.topic = topic
        self.symbols = symbols  
        self.schedule_time = schedule_time

    def fetch_and_produce_stock_data(self):
        for symbol in self.symbols:
            try:
                x = yf.Ticker(f"{symbol}")
                x = x.history(period='max').reset_index()
                
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
                    self.producer.send(self.topic, value=stock_record)
                
                self.producer.flush()
                print(f"Data for {symbol} sent successfully")
                
            except Exception as e:
                print(f"Error fetching data for {symbol}: {e}")
    
    def schedule_data_fetching(self):
        if self.schedule_time:
            schedule.every().day.at(self.schedule_time).do(self.fetch_and_produce_stock_data)
            print(f"Scheduled fetching and producing stock data at {self.schedule_time}")

            while True:
                schedule.run_pending()
                time.sleep(1)
        else:
            self.fetch_and_produce_stock_data()
            
# Usage example
if __name__ == "__main__":
    stock_symbols = [
        "NVDA", "TSLA", "META", "GOOGL", "PLTR", "GOOG", "BRK.B", "BRK.A", 
        "FSLR", "BSX", "GOLD", "EA", "INTU", "SHOP", "ADI", "RMD", 
        "ISRG", "ANET", "VRTX", "WELL", "WAB", "O", "AEM", "VICI", 
        "XYL", "IR", "AME", "VEEV", "FTV", "INFY", "KHC", "SAP", 
        "GRMN", "CP", "TCOM", "ALC", "TW", "EQR", "FAST", "ERIE",
        "TRI", "GIB", "CHT"  
    ]

    extractor = StockDataExtractor(kafka_servers='localhost:9092', 
                                topic='stock_price',
                                symbols = stock_symbols,
                                schedule_time=None)
    
    extractor.schedule_data_fetching()