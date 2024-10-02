import pandas as pd

class CandlePattern:
    def __init__(self, candle: pd.DataFrame):
        self.candle = candle
        self.body_size = abs(self.candle['close'] - self.candle['open'])
        self.lower_shadow = min(self.candle['open'], self.candle['close']) - self.candle['low']
        self.upper_shadow = self.candle['high'] - max(self.candle['open'], self.candle['close']) 
    
    def no_volume(self):
        return self.candle['close'] == self.candle['open'] == self.candle['high'] == self.candle['low']
    
    def hammer_alert(self):
        # Hammer candle pattern: small body, long lower shadow, little or no upper shadow
        is_hammer = (
            (self.body_size <= (self.candle['high'] - self.candle['low']) * 0.3) &  # Small body
            (self.lower_shadow >= (self.candle['high'] - self.candle['low']) * 0.7) &  # Long lower shadow
            (self.upper_shadow <= (self.candle['high'] - self.candle['low']) * 0.1)  # Little or no upper shadow
        )
        
        date = pd.to_datetime(self.candle['datetime']).strftime('%Y-%m-%d %H:%M:%S')
        return is_hammer, date

    def engulf_alert(self, prev_candle: pd.DataFrame, current_candle: pd.DataFrame):
        # Previous Candle
        prev_open = prev_candle['open']
        prev_close = prev_candle['close']

        # Current Candle
        current_open = current_candle['open']
        current_close = current_candle['close']
        current_high = current_candle['high']
        
        # Bullish Engulfing: current green candle engulfs previous candle
        bullish_engulfing = (
            (current_close > current_open) &  # Current candle is green
            (current_open < min(prev_open, prev_close)) & 
            (current_close > max(prev_open, prev_close)) &
            ((current_close - current_open) > abs(prev_close - prev_open) * 1.5) &  # Current candle significantly larger
            ((current_high - current_close) * 0.7 <= (current_close - current_open)) # Little or no upper shadow
        )
        
        # Bearish Engulfing: current red candle engulfs previous candle
        bearish_engulfing = (
            (current_close < current_open) &  # Current candle is red
            (current_open > max(prev_open, prev_close)) & 
            (current_close < min(prev_open, prev_close))
        )

        date = pd.to_datetime(current_candle['datetime']).strftime('%Y-%m-%d %H:%M:%S')
        
        return bullish_engulfing, bearish_engulfing, date

