import pandas as pd

class CandlePattern:
    def __init__(self, candle: pd.DataFrame):
        self.candle = candle

    def bullish_382_alert(self):
        # Calculate Fibonacci 38.2% retracement level
        diff = self.candle['high'] - self.candle['low']
        fib_382_level = self.candle['high'] - 0.382 * diff
        close_above_fib_382 = self.candle['close'] > fib_382_level

        return close_above_fib_382

    def engulf_alert(self, prev_candle: pd.DataFrame, current_candle: pd.DataFrame):
        # Previous Candle
        prev_open = prev_candle['open']
        prev_close = prev_candle['close']

        # Current Candle
        current_open = current_candle['open']
        current_close = current_candle['close']

        # Bullish Engulfing: current green candle engulfs previous red candle
        bullish_engulfing = (
            (prev_close < prev_open) &  # Previous candle was red
            (current_close > current_open) &  # Current candle is green
            (current_open < prev_close) & 
            (current_close > prev_open)
        )

        # Bearish Engulfing: current red candle engulfs previous green candle
        bearish_engulfing = (
            (prev_close > prev_open) &  # Previous candle was green
            (current_close < current_open) &  # Current candle is red
            (current_open > prev_close) & 
            (current_close < prev_open)
        )
        
        return bullish_engulfing, bearish_engulfing