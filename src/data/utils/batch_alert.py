import pandas as pd

class trend_pattern:
    def __init__(self, lookback, batch_data):
        self.lookback = lookback
        self.batch_data = batch_data
        
    def strong_support(self):
        support = self.batch_data[self.batch_data['low'] == self.batch_data['low'].rolling(5, center=True).min()][['low']]
        return support

    def strong_resistance(self):
        resistance = self.batch_data[self.batch_data['high'] == self.batch_data['high'].rolling(5, center=True).max()][['high']]
        return resistance