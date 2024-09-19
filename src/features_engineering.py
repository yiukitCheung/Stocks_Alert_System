import numpy as np
import ta

class add_features:
    def __init__(self, df):
        self.df = df.copy()

    def add_candlestick(self):
        self.df["BodyDiff"] = abs(self.df["Open"] - self.df["Close"])
        self.df["CandleStickType"] = np.where(self.df["Open"] < self.df["Close"], "Green", "Red")
        return self.df

    def continuous_increase(self, windows=3):
        for i in range(1, windows + 1):
            self.df[f"Close_t-{i}"] = self.df["Close"].shift(i)

        self.df['Incremental_High'] = (self.df['Close'] > self.df['Close_t-1']) \
                                        & (self.df['Close_t-1'] > self.df['Close_t-2']) \
                                        & (self.df['Close_t-2'] > self.df['Close_t-3'])
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
        self.df['8EMA'] = ta.trend.ema_indicator(self.df['Close'], window=8)
        self.df['13EMA'] = ta.trend.ema_indicator(self.df['Close'], window=13)
        self.df['144EMA'] = ta.trend.ema_indicator(self.df['Close'], window=144)
        self.df['169EMA'] = ta.trend.ema_indicator(self.df['Close'], window=169)

        # Add MACD technical indicator
        self.df['MACD'] = ta.trend.macd(self.df['Close'], 
                                        window_slow=26, 
                                        window_fast=12)
        
        self.df['MACD_SIGNAL'] = ta.trend.macd_signal(self.df['Close'], 
                                                    window_slow=26, 
                                                    window_fast=12, 
                                                    window_sign=9)
        
        self.df['MACD_HIST'] = ta.trend.macd_diff(self.df['Close'], 
                                                window_slow=26, 
                                                window_fast=12, 
                                                window_sign=9)
        
        # Add ATR technical indicator
        self.df["atr"] = ta.volatility.AverageTrueRange(high=self.df.High, 
                                                        low=self.df.Low, 
                                                        close=self.df.Close).average_true_range()
        
        self.df["atr"] = self.df.atr.rolling(window=30).mean()
        
        return self.df
    
    def apply(self):
        self.add_technical()
        self.add_candlestick()
        self.continuous_increase()
        self.macd_golden_cross()
        self.add_ema_band()

        return self.df