import ta
import numpy as np
# Add bull and bear features in the dataframe
class add_features:
    def __init__(self, df):
        self.df = df.copy()

    def add_candlestick(self):
        self.df["bodydiff"] = abs(self.df["open"] - self.df["close"])
        self.df["candlesticktype"] = np.where(self.df["open"] < self.df["close"], "green", "red")
        return self.df
    
    # Add Technical Indicators
    def add_technical(self):
        # Add ema dual channels technical indicators
        self.df['8ema'] = ta.trend.ema_indicator(self.df['close'], window=8)
        self.df['13ema'] = ta.trend.ema_indicator(self.df['close'], window=13)
        self.df['144ema'] = ta.trend.ema_indicator(self.df['close'], window=144)
        self.df['169ema'] = ta.trend.ema_indicator(self.df['close'], window=169)

        # Add MACD technical indicator
        self.df['macd'] = ta.trend.macd(self.df['close'], window_slow=26, window_fast=12)
        self.df['macd_signal'] = ta.trend.macd_signal(self.df['close'], window_slow=26, window_fast=12, window_sign=9)
        self.df['macd_hist'] = ta.trend.macd_diff(self.df['close'], window_slow=26, window_fast=12, window_sign=9)
        
        # Add ATR technical indicator
        self.df["atr"] = ta.volatility.AverageTrueRange(high=self.df.high, low=self.df.low, close=self.df.close).average_true_range()
        self.df["atr"] = self.df.atr.rolling(window=30).mean()
        
        return self.df

    def macd_golden_cross(self):
        self.df['macd_golden_cross'] = (self.df['macd'] > self.df['macd_signal']) & (self.df['macd'] < 0)
        return self.df

    def apply(self):
        self.add_technical()
        self.add_candlestick()
        self.macd_golden_cross()
        # Take last 252 * 5 rows
        self.df = self.df.iloc[-252*5:].reset_index(drop=True)
        
        return self.df

