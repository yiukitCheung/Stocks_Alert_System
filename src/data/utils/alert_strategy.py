import pandas as pd
import numpy as np
class AddAlert:
    def __init__(self, df, interval):
        self.df = df[df['interval'] == interval].copy()
        self.window = 3 # Number of days to compare the stock price
        self.interval = interval
        # Create a new column to store the alert
        self.df['velocity_alert'] = pd.Series([None] * len(self.df), dtype='object')
        self.df['candle_alert'] = pd.Series([None] * len(self.df), dtype='object')
        self.df['macd_alert'] = pd.Series([None] * len(self.df), dtype='object')
        
        # Initialize velocity finder
        self.velocity_finder = VelocityFinder(self.df)
        
    def engulf_alert(self):
        prev_open = self.df['open'].shift(1)
        prev_close = self.df['close'].shift(1)
        current_open = self.df['open']
        current_close = self.df['close']
        current_high = self.df['high']
        
        # Bullish Engulfing: current green candle engulfs previous candle
        bullish_engulfing = (
            (current_close > current_open) & 
            (current_open < prev_open.combine(prev_close, min)) & 
            (current_close > prev_open.combine(prev_close, max)) &
            ((current_close - current_open) > abs(prev_close - prev_open) * 1.5) &  # Current candle significantly larger
            ((current_high - current_close) * 0.7 <= (current_close - current_open)) # Little or no upper shadow
        )

        # Bearish Engulfing: current red candle engulfs previous candle
        bearish_engulfing = (
            (current_close < current_open) &  # Current candle is red
            (current_open > prev_open.combine(prev_close, max)) & 
            (current_close < prev_open.combine(prev_close, min))
        )
        
        self.df.loc[bullish_engulfing, 'candle_alert'] = 'bullish_engulf'
        self.df.loc[bearish_engulfing, 'candle_alert'] = 'bearish_engulf'

    def macd_alert(self):
        
        macd_above_signal = self.df['MACD'] > self.df['MACD_SIGNAL']
        macd_increasing = self.df['MACD'].diff() > 0
        macd_below_zero = self.df['MACD'] >= 0

        bullish_macd = macd_above_signal & macd_increasing & macd_below_zero
        bearish_macd = (self.df['MACD'] < self.df['MACD_SIGNAL']) & (self.df['MACD'] > self.df['MACD'].shift(-1))

        self.df.loc[bullish_macd, 'macd_alert'] = 'bullish_macd'
        self.df.loc[bearish_macd, 'macd_alert'] = 'bearish_macd'
        
    def hammer_alert(self):
        self.df['body_size'] = abs(self.df['close'] - self.df['open'])
        self.df['upper_shadow'] = self.df['high'] - self.df[['close', 'open']].max(axis=1)
        self.df['lower_shadow'] = self.df[['close', 'open']].min(axis=1) - self.df['low']

        is_hammer = (
            (self.df['body_size'] <= (self.df['high'] - self.df['low']) * 0.3) & 
            (self.df['lower_shadow'] >= (self.df['high'] - self.df['low']) * 0.7) & 
            (self.df['upper_shadow'] <= (self.df['high'] - self.df['low']) * 0.1)
        )

        inverse_hammer = (
            (self.df['body_size'] <= (self.df['high'] - self.df['low']) * 0.3) & 
            (self.df['upper_shadow'] >= (self.df['high'] - self.df['low']) * 0.7) & 
            (self.df['lower_shadow'] <= (self.df['high'] - self.df['low']) * 0.1)
        )

        self.df.loc[is_hammer, 'candle_alert'] = 'hammer'
        self.df.loc[inverse_hammer, 'candle_alert'] = 'inverse_hammer'

        self.df.drop(columns=['body_size', 'upper_shadow', 'lower_shadow'], inplace=True)

    def ema_support_alert(self, judgement_days=6, decision_days=3, epilson=0.05):
        ema_periods = [13, 169]  # Example EMA periods

        for ema_period in ema_periods:
            self.df[f'{ema_period}EMA_Lower'] = self.df[f'{ema_period}EMA'] * (1 - epilson)
            self.df[f'{ema_period}EMA_Upper'] = self.df[f'{ema_period}EMA'] * (1 + epilson)

            self.df[f'above_{ema_period}_emas'] = self.df['close'] > self.df['169EMA']
            self.df[f'below_{ema_period}_emas'] = self.df['close'] < self.df['169EMA']
            self.df[f'inside_{ema_period}_ema_band'] = (self.df['close'] > self.df[f'{ema_period}EMA_Lower']) & (self.df['close'] < self.df[f'{ema_period}EMA_Upper'])

            ema_column = f'{ema_period}EMA'
            self.df[f'{ema_period}EMA_Lower'] = self.df[ema_column] * (1 - epilson)
            self.df[f'{ema_period}EMA_Upper'] = self.df[ema_column] * (1 + epilson)

        ema13_alert_date = 0
        ema169_alert_date = 0
            
        for i in self.df.index:
            if i - judgement_days < self.df.index[0]:
                continue  # Skip the iteration if not enough history is available for comparison

            # Ensure you are operating on matching ranges for indices
            close_range = self.df.loc[i - decision_days: i, 'close']
            open_range = self.df.loc[i - decision_days: i, 'open']
            ema13_range = self.df.loc[i - decision_days: i, '13EMA']
            ema169_range = self.df.loc[i - decision_days: i, '169EMA']
            
            # Only proceed if the required ranges have identical lengths and indices
            if len(close_range) == len(ema13_range):
                # Check conditions for alerts
                if (self.df.loc[i - judgement_days: i - decision_days, 'inside_13_ema_band']).any() \
                    and self.df.loc[i - judgement_days: i - decision_days, 'above_169_emas'].all() \
                    and self.df.loc[i - judgement_days: i - decision_days, '13EMA_Upper'].any():
                    
                    # Condition for "13ema_recovery"
                    if (close_range >= ema13_range).all() \
                        and (open_range >= ema13_range).all() \
                        and (self.df.loc[i, 'close'] >= self.df.loc[i - decision_days: i, 'close']).all() \
                        and (self.df.loc[i, 'open'] >= self.df.loc[i - decision_days: i, 'open']).all():
                        
                        if ema13_alert_date == 0 or not (self.df.loc[ema13_alert_date : i, 'velocity_alert'] == '13ema_recovery').any():
                            self.df.loc[i, 'velocity_alert'] = '13ema_recovery'
                            ema13_alert_date = i

                    # Condition for "13ema_recover_failed"
                    elif (close_range <= self.df.loc[i - decision_days : i, '13EMA']).all() \
                        and (open_range <= self.df.loc[i - decision_days : i, '13EMA']).all():
                        
                        if ema13_alert_date == 0 or not (self.df.loc[ema13_alert_date : i, 'velocity_alert'] == '13ema_recover_failed').any():
                            self.df.loc[i, 'velocity_alert'] = '13ema_recover_failed'
                            ema13_alert_date = i
            else:
                print(f"Mismatched lengths for index range at {i}")

            if len(close_range) == len(ema169_range):
                if (self.df.loc[i - judgement_days : i - decision_days, 'inside_169_ema_band']).any() \
                    and self.df.loc[i - judgement_days : i - decision_days, 'above_169_emas'].any()\
                        and self.df.loc[i - judgement_days : i - decision_days, '169EMA_Upper'].any():

                    if (close_range >= ema169_range).all() \
                        and (open_range >= ema169_range).all() \
                        and (self.df.loc[i, 'close'] >= self.df.loc[i - decision_days : i, 'close']).all() \
                        and (self.df.loc[i, 'open'] >= self.df.loc[i - decision_days : i, 'open']).all():
                        
                        if ema169_alert_date == 0 or not (self.df.loc[ema169_alert_date : i, 'velocity_alert'] == '169ema_recovery').any():
                            self.df.loc[i, 'velocity_alert'] = '169ema_recovery'
                            ema169_alert_date = i

                    elif (close_range <= self.df.loc[i - decision_days : i, '169EMA']).all() \
                        and (open_range <= self.df.loc[i - decision_days : i, '169EMA']).all():
                        
                        if ema169_alert_date == 0 or not (self.df.loc[ema169_alert_date : i, 'velocity_alert'] == '169ema_recover_failed').any():
                            self.df.loc[i, 'velocity_alert'] = '169ema_recover_failed'
                            ema169_alert_date = i

        for ema_period in ema_periods:
            self.df.drop(columns=[f'{ema_period}EMA_Lower', f'{ema_period}EMA_Upper', f'above_{ema_period}_emas', f'below_{ema_period}_emas', f'inside_{ema_period}_ema_band'], inplace=True)

    def velocity_alert(self):
        # Condition where closing price above the 13 EMA and 169 EMA at the same time
        self.df['above_13EMA'] = self.df['close'] > self.df['13EMA']
        self.df['above_169EMA'] = self.df['close'] > self.df['169EMA']

        # Condition where closing price below the 13 EMA and 169 EMA
        self.df['below_13EMA'] = self.df['close'] < self.df['13EMA']
        self.df['below_169EMA'] = self.df['close'] < self.df['169EMA']

        # Condition where closing price is sandwiched between the 13 EMA and 169 EMA
        self.df['between_13_169EMA'] = (self.df['close'] <= self.df['13EMA']) & (self.df['close'] >= self.df['169EMA'])

        # Loop through the rows to calculate the velocity alerts
        for i in self.df.index:
            # Skip rows before the window starts to avoid indexing errors
            if i - self.window < self.df.index[0]:
                continue
            
            # Skip rows that already have an EMA support alert (e.g., '13ema_recovery', '13ema_recover_failed', etc.)
            if pd.notna(self.df.loc[i, 'velocity_alert']):
                continue
            # Check for the 'velocity_maintained' alert (above both EMAs)
            if self.df.loc[i, 'above_13EMA'] and self.df.loc[i, 'above_169EMA']:
                self.df.loc[i, 'velocity_alert'] = 'velocity_maintained'

            # If price was below 13EMA in any of the past `window` periods and now it's above, set 'velocity_negotiating'
            elif (self.df.loc[i - self.window: i - 1, 'below_13EMA'].any() and self.df.loc[i, 'above_13EMA'])\
                or (self.df.loc[i - self.window: i - 1, 'below_169EMA'].any() and self.df.loc[i, 'above_169EMA']):
                self.df.loc[i, 'velocity_alert'] = 'velocity_negotiating'

            # Check for the 'velocity_loss' alert (below either 13EMA or 169EMA)
            elif self.df.loc[i, 'below_13EMA'] or self.df.loc[i, 'below_169EMA']:
                self.df.loc[i, 'velocity_alert'] = 'velocity_loss'

            # Check for the 'velocity_weak' alert (sandwiched between 13EMA and 169EMA)
            elif self.df.loc[i, 'Incremental_Low'] == 1:
                if self.df.loc[i - self.window: i - 1, 'above_13EMA'].any():
                    self.df.loc[i, 'velocity_alert'] = 'velocity_weak'
            else:
                self.df.loc[i, 'velocity_alert'] = None
        # Drop intermediate columns used for conditions
        self.df.drop(columns=['above_13EMA', 'above_169EMA', 'below_13EMA', 'below_169EMA', 'between_13_169EMA'], inplace=True)

    def fit_best_velocity(self):
        pass
    
    def apply(self):
        alert_methods = [
            self.engulf_alert,
            self.macd_alert,
            self.hammer_alert,
            self.ema_support_alert,
            self.velocity_alert
        ]

        for alert_method in alert_methods:
            alert_method()

        self.df = self.df[['symbol', 'date', 'velocity_alert', 'candle_alert', 'macd_alert','best_velocity']]
        return self.df
    
class VelocityFinder:
    def __init__(self, data):
        self.data = data

    def support_loss(self, closing_prices, ema_13):
        """
        Custom loss function to find intervals where the 13 EMA acts as support.
        Penalizes any instance where the closing price falls below the 13 EMA.
        """
        differences = closing_prices - ema_13
        penalties = np.where(differences < 0, np.abs(differences), 0)
        return np.sum(penalties)

    def find_best_interval(self):
        """
        Find the interval with the lowest time-weighted 'price velocity', 
        i.e., the one where the closing price fits the 13 EMA line the best, for the last 60 days.
        """
        intervals = self.data['interval'].unique()

        min_loss = float('inf')
        best_interval = None

        # Loop through each interval to find the best fitting one
        for interval in intervals:
            interval_data = self.data[self.data['interval'] == interval].tail(60)
            if not interval_data.empty:
                closing_prices = interval_data['close'].values
                ema_13 = interval_data['13EMA'].values
                loss = self.support_loss(closing_prices, ema_13)

                if loss < min_loss:
                    min_loss = loss
                    best_interval = interval

        # Find the date range of 60 candles in the best interval
        interval_date_start = self.data[self.data['interval'] == best_interval]['date'].values[-60]
        interval_date_end = self.data[self.data['interval'] == best_interval]['date'].values[-1]

        # Attach the best interval as the best_velocity for the rows with dates between interval_date_start and interval_date_end
        self.data.loc[
            (self.data['date'] >= interval_date_start) & 
            (self.data['date'] <= interval_date_end),
            'best_velocity'
        ] = best_interval

    def run(self):
        self.find_best_interval()
        
        for interval in self.data['interval'].unique():
            self.data.loc[self.data['interval'] == interval] = self.data.loc[self.data['interval'] == interval].ffill()
        return self.data