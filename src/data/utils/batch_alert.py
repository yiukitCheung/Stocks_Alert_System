import pandas as pd

class trend_pattern:
    def __init__(self, lookback, batch_data):
        self.lookback = lookback
        self.batch_data = batch_data
    
    def strong_support(self):
        support_line =  self.batch_data['low'].rolling(5, center=True).min()
        support_df = self.batch_data[self.batch_data['low'] == support_line]
        return support_df
    
    def strong_resistance(self):
        resistance_line = self.batch_data['high'].rolling(5, center=True).max()
        resistance_df = self.batch_data[self.batch_data['high'] == resistance_line]
        return resistance_df