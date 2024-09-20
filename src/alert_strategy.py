class Alert:
    
    def __init__(self, df):
        self.df = df.copy()
        self.df['MACD_Alert'] = -1  
        self.df['Engulf_Alert'] = -1 
        self.df['dual_channel_Alert'] = -1  
        self.df['382_Alert'] = -1  
        self.window = 3  # Number of days to compare the stock price
        
    def engulf_alert(self):
        
        # Previous Candle
        prev_open = self.df['open'].shift(1)
        prev_close = self.df['close'].shift(1)
        
        # Current Candle
        current_open = self.df['open']
        current_close = self.df['close']

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

        # Apply the conditions to the DataFrame
        self.df.loc[bullish_engulfing, 'Engulf_Alert'] = 1
        self.df.loc[bearish_engulfing, 'Engulf_Alert'] = 0
        
        return self.df

    def macd_alert(self):
        
        # Pre-compute conditions
        macd_above_signal = self.df['MACD'] > self.df['MACD_SIGNAL']
        macd_increasing = self.df['MACD'].diff() > 0
        macd_below_zero = self.df['MACD'] >= 0

        # Bullish condition
        bullish_macd = (
            macd_above_signal &
            macd_increasing&
            macd_below_zero
        )
        
        # Bearish condition
        bearish_macd = (
            (self.df['MACD'] < self.df['MACD_SIGNAL']) &
            (self.df['MACD'] > self.df['MACD'].shift(-1))  
        )

        # Apply the conditions to the DataFrame
        self.df.loc[bullish_macd, 'MACD_Alert'] = 1
        self.df.loc[bearish_macd, 'MACD_Alert'] = 0

        return self.df

    def dual_channel(self):
        
        # Confirm Technical Indicators Conditions
        ema_8_gt_ema_13 = self.df['8EMA'] > self.df['13EMA']
        ema_13_gt_ema_169 = self.df['13EMA'] > self.df['169EMA']
        ema_8_gt_ema_144 = self.df['8EMA'] > self.df['144EMA']
        
        # Confirm Close price action
        close_ge_ema_13 = self.df['close'] >= self.df['13EMA']
        close_ge_ema_144 = self.df['close'] >= self.df['144EMA']
        
        # Confirm Candle fully above 13 EMA
        low_ge_ema_13 = self.df['low'] >= self.df['13EMA'] 
        green_candle = self.df['CandleStickType'] == 'green'    
        
        # Confirm Candle in selected range
        open_in_slow_ema = self.df['open'].between(self.df['169EMA_Lower'], self.df['169EMA_Upper'])
        
        # Confirm Candle type
        green_candle = self.df['CandleStickType'] == 'green'
        
        # Bullish Conditions
        
        bullish_scenario_1 = (
            ema_8_gt_ema_13 & 
            ema_13_gt_ema_169 & 
            close_ge_ema_13 &
            low_ge_ema_13 &
            green_candle
        )
        
        # Bearish Condition
        bearish_scenario_1 = (
            (self.df['open'] < self.df['13EMA']) &
            (self.df['close'] < self.df['13EMA']) &
            (self.df['13EMA'] < self.df['8EMA'])
        )
        # Bullish Conditions 2
        bullish_scenario_2 = ( 
            ema_8_gt_ema_144 &
            ema_13_gt_ema_169 &
            close_ge_ema_144 &
            open_in_slow_ema &
            green_candle
            )
        
        # Bearish Condition 2
        bearish_scenario_2 = (
            (self.df['open'] < self.df['13EMA']) &
            (self.df['close'] < self.df['13EMA']) &
            (self.df['8EMA'] < self.df['13EMA']) &
            ~ open_in_slow_ema
        )

        # Apply conditions on different price scenarios
        
        self.df.loc[bullish_scenario_1 | bullish_scenario_2, 'dual_channel_Alert'] = 1
        self.df.loc[bearish_scenario_1 | bearish_scenario_2, 'dual_channel_Alert'] = 0
        
        return self.df

    def bullish_382_alert(self):
        # Calculate Fibonacci 38.2% retracement level
        diff = self.df['high'] - self.df['low']
        fib_382_level = self.df['high'] - 0.382 * diff
        open_above_fib_382 = self.df['open'] > fib_382_level
        
        # Apply condition
        self.df.loc[open_above_fib_382, '382_Alert'] = 1
        self.df.loc[~open_above_fib_382, '382_Alert'] = 0
        
        return self.df

    def add_alert(self):
        self.engulf_alert()
        self.macd_alert()
        self.dual_channel()
        self.bullish_382_alert()
        return self.df
    
# # Apply the strategy
# testing_df = Alert(testing_df).add_alert()

# print(f"Bullish MACD Count: {len(testing_df[testing_df['MACD_Alert'] == 1])} | Bearish Count: {len(testing_df[testing_df['MACD_Alert'] == 0])}")
# print(f"Bullish Engulf Count: {len(testing_df[testing_df['Engulf_Alert'] == 1])} | Bearish Engulf Count: {len(testing_df[testing_df['Engulf_Alert'] == 0])}")
# print(f"Bullish 382 Count: {len(testing_df[testing_df['382_Alert'] == 1])} | Bearish 382 Count: {len(testing_df[testing_df['382_Alert'] == 0])}")
# print(f"Bullish Dual Channel Count: {len(testing_df[testing_df['dual_channel_Alert'] == 1])} | Bearish Dual Channel Count: {len(testing_df[testing_df['dual_channel_Alert'] == 0])}")
