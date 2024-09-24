import ta
import numpy as np

# Add bull and bear features in the dataframe
class add_features:
    def __init__(self, df):
        self.df = df.copy()

    def add_candlestick(self):
        self.df["BodyDiff"] = abs(self.df["open"] - self.df["close"])
        self.df["CandleStickType"] = np.where(self.df["open"] < self.df["close"], "green", "red")
        return self.df

    def continuous_increase(self, windows=3):
        for i in range(1, windows + 1):
            self.df[f"close_t-{i}"] = self.df["close"].shift(i)

        self.df['Incremental_High'] = (self.df['close'] > self.df['close_t-1']) \
                                        & (self.df['close_t-1'] > self.df['close_t-2']) \
                                        & (self.df['close_t-2'] > self.df['close_t-3'])
        return self.df

    def macd_golden_cross(self):
        self.df['MACD_GOLDEN_CROSS'] = (self.df['MACD'] > self.df['MACD_SIGNAL']) & (self.df['MACD'] < 0)
        return self.df

    def add_ema_band(self, threshold=0.05):
        self.df['169EMA_Upper'] = self.df['169EMA'] * (1 + threshold)
        self.df['169EMA_Lower'] = self.df['169EMA'] * (1 - threshold)
        return self.df
    
    # Add Technical Indicators
    def add_technical(self):
        
        # Add ema dual channels technical indicators
        self.df['8EMA'] = ta.trend.ema_indicator(self.df['close'], window=8)
        self.df['13EMA'] = ta.trend.ema_indicator(self.df['close'], window=13)
        self.df['144EMA'] = ta.trend.ema_indicator(self.df['close'], window=144)
        self.df['169EMA'] = ta.trend.ema_indicator(self.df['close'], window=169)

        # Add MACD technical indicator
        self.df['MACD'] = ta.trend.macd(self.df['close'], 
                                        window_slow=26, 
                                        window_fast=12)
        
        self.df['MACD_SIGNAL'] = ta.trend.macd_signal(self.df['close'], 
                                                    window_slow=26, 
                                                    window_fast=12, 
                                                    window_sign=9)
        
        self.df['MACD_HIST'] = ta.trend.macd_diff(self.df['close'], 
                                                window_slow=26, 
                                                window_fast=12, 
                                                window_sign=9)
        
        # Add ATR technical indicator
        self.df["atr"] = ta.volatility.AverageTrueRange(high=self.df.high, 
                                                        low=self.df.low, 
                                                        close=self.df.close).average_true_range()
        
        self.df["atr"] = self.df.atr.rolling(window=30).mean()
        
        return self.df
    
    def apply(self):
        self.add_technical()
        self.add_candlestick()
        self.continuous_increase()
        self.macd_golden_cross()
        self.add_ema_band()
        
        # Take last 252 * 3 rows
        self.df = self.df.iloc[-252*3:]
        
        return self.df
