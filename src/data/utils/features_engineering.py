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
    
    def continuous_increase(self, windows=3):
        for i in range(1, windows + 1):
            self.df[f"close_t-{i}"] = self.df["close"].shift(i)

        self.df['Incremental_High'] = (self.df['close'] > self.df['close_t-1']) \
                                        & (self.df['close_t-1'] > self.df['close_t-2']) \
                                        & (self.df['close_t-2'] > self.df['close_t-3'])
        self.df['Incremental_Low'] = (self.df['close'] < self.df['close_t-1']) \
                                        & (self.df['close_t-1'] < self.df['close_t-2']) \
                                        & (self.df['close_t-2'] < self.df['close_t-3'])
                                        
        self.df.drop([f"close_t-{i}" for i in range(1, windows + 1)], axis=1, inplace=True)
        
        
        return self.df

    def macd_golden_cross(self):
        self.df['MACD_GOLDEN_CROSS'] = (self.df['MACD'] > self.df['MACD_SIGNAL']) & (self.df['MACD'] < 0)
        return self.df
    
    def apply(self):
        self.add_technical()
        self.add_candlestick()
        self.continuous_increase()
        self.macd_golden_cross()        
        # Take last 252 * 3 rows
        self.df = self.df.iloc[-252*3:]
        
        return self.df

class VelocityFinder:
    def __init__(self, data):
        self.data = data

    def support_loss(self, closing_prices, ema_13):
        """
        Custom loss function to find intervals where the 13 EMA acts as support.
        Penalizes any instance where the closing price falls below the 13 EMA.
        """
        # Calculate the difference between the closing price and the 13 EMA
        differences = closing_prices - ema_13

        # Penalize cases where the closing price is below the 13 EMA (negative values)
        penalties = np.where(differences < 0, np.abs(differences), 0)

        # Sum up the penalties to form the loss
        loss = np.sum(penalties)

        return loss

    def find_best_interval(self):
        """
        Find the interval with the lowest time-weighted 'price velocity', 
        i.e., the one where the closing price fits the 13 EMA line the best.
        """
        intervals = self.data['interval'].unique()
        best_interval = None
        min_loss = float('inf')

        for interval in intervals:
            if 'D' not in interval:
                continue  # Skip non-numeric intervals
            interval_data = self.data[self.data['interval'] == interval].iloc[-60:]
            closing_prices = interval_data['close'].values
            ema_13 = interval_data['13EMA'].values

            # Compute time-weighted loss for this interval
            loss = self.support_loss(closing_prices, ema_13)

            if loss < min_loss:
                min_loss = loss
                best_interval = interval

        return best_interval, min_loss