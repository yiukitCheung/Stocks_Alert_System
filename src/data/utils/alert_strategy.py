import pandas as pd
class AddAlert:
    def __init__(self, df, interval):
        self.df = df[df['interval'] == interval].copy()
        self.window = 3 # Number of days to compare the stock price
        self.interval = interval
        # Create a new column to store the alert
        self.df['velocity_alert'] = pd.Series([None] * len(self.df), dtype='object')
        self.df['candle_alert'] = pd.Series([None] * len(self.df), dtype='object')
        self.df['macd_alert'] = pd.Series([None] * len(self.df), dtype='object')
        
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

    def ema_support_alert(self, recovery_days=5, epilson=0.05):
        
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

        alert_date = 0

        for i in self.df.index:
            if i - recovery_days < self.df.index[0]:
                continue
            else:
                if (self.df.loc[i - recovery_days : i - 1, 'inside_13_ema_band']).any() \
                    and self.df.loc[i - recovery_days : i - 1, 'above_169_emas'].all()\
                    and self.df.loc[i - recovery_days : i - 1, 'above_13_emas'].any():

                    current_price = self.df.loc[i, 'close']

                    if (current_price >= self.df.loc[i - recovery_days : i - 1, '13EMA']).all():
                        if alert_date == 0 or not (self.df.loc[alert_date : i, 'velocity_alert'] == '13ema_recovery').any():
                            self.df.loc[i, 'velocity_alert'] = '13ema_recovery'
                            alert_date = i

                    if (current_price <= self.df.loc[i - recovery_days : i - 1, '13EMA']).all():
                        if alert_date == 0 or not (self.df.loc[alert_date : i, 'velocity_alert'] == '13ema_recover_failed').any():
                            self.df.loc[i, 'velocity_alert'] = '13ema_recover_failed'
                            alert_date = i

        for ema_period in ema_periods:
            self.df = self.df.drop(columns=[f'{ema_period}EMA_Lower', f'{ema_period}EMA_Upper', f'above_{ema_period}_emas', f'below_{ema_period}_emas', f'inside_{ema_period}_ema_band'])

    def velocity_alert(self):
        # Condition where closing price above the 13 EMA and 169 EMA at the same time
        self.df['above_13EMA'] = self.df['close'] > self.df['13EMA']
        self.df['above_169EMA'] = self.df['close'] > self.df['169EMA']

        # Condition where closing price below the 13 EMA and 169 EMA
        self.df['below_13EMA'] = self.df['close'] < self.df['13EMA']
        self.df['below_169EMA'] = self.df['close'] < self.df['169EMA']

        # Condition where closing price sandwiched between the 13 EMA and 169 EMA
        self.df['between_13_169EMA'] = (self.df['close'] > self.df['13EMA']) & (self.df['close'] < self.df['169EMA'])

        for i in self.df.index:
            if i - self.window < self.df.index[0]:
                continue
            if self.df.loc[i, 'above_13EMA'] and self.df.loc[i, 'above_169EMA']:
                self.df.loc[i, 'velocity_alert'] = 'velocity_maintained'
                if self.df.loc[i - self.window, 'below_13EMA'] and self.df.loc[i, 'above_13EMA']:
                    self.df.loc[i, 'velocity_alert'] = 'velocity_negotiating'

            elif self.df.loc[i, 'below_13EMA'] or self.df.loc[i, 'below_169EMA']:
                self.df.loc[i, 'velocity_alert'] = 'velocity_loss'

            elif self.df.loc[i, 'between_13_169EMA']:
                if self.df.loc[i-self.window, 'above_13EMA']:
                    self.df.loc[i, 'velocity_alert'] = 'velocity_weak'

        self.df.drop(columns=['above_13EMA', 'above_169EMA', 'below_13EMA', 'below_169EMA', 'between_13_169EMA'], inplace=True)

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

        self.df = self.df[['symbol', 'date', 'velocity_alert', 'candle_alert', 'macd_alert']]
        return self.df
