import pandas as pd

class AddAlert:
    def __init__(self, df, interval):
        self.df = df[df['interval'] == interval].copy()
        self.window = 3  # Number of days to compare the stock price
        self.interval = interval

        self.df[f'{self.interval}_MACD_Alert'] = -1  
        self.df[f'{self.interval}_Engulf_Alert'] = -1 
        self.df[f'{self.interval}_Hammer_Alert'] = -1
        self.df[f'{self.interval}_Inverse_Hammer_Alert'] = -1

    def engulf_alert(self):
        prev_open = self.df['open'].shift(1)
        prev_close = self.df['close'].shift(1)
        current_open = self.df['open']
        current_close = self.df['close']

        bullish_engulfing = (
            (prev_close < prev_open) & 
            (current_close > current_open) & 
            (current_open < prev_close) & 
            (current_close > prev_open)
        )

        bearish_engulfing = (
            (prev_close > prev_open) & 
            (current_close < current_open) & 
            (current_open > prev_close) & 
            (current_close < prev_open)
        )

        self.df.loc[bullish_engulfing, f'{self.interval}_Engulf_Alert'] = 1
        self.df.loc[bearish_engulfing, f'{self.interval}_Engulf_Alert'] = 0

    def macd_alert(self):
        macd_above_signal = self.df['MACD'] > self.df['MACD_SIGNAL']
        macd_increasing = self.df['MACD'].diff() > 0
        macd_below_zero = self.df['MACD'] >= 0

        bullish_macd = macd_above_signal & macd_increasing & macd_below_zero
        bearish_macd = (self.df['MACD'] < self.df['MACD_SIGNAL']) & (self.df['MACD'] > self.df['MACD'].shift(-1))

        self.df.loc[bullish_macd, f'{self.interval}_MACD_Alert'] = 1
        self.df.loc[bearish_macd, f'{self.interval}_MACD_Alert'] = 0

    def ema_support_alert(self, ema_periods=[13,169], epsilon=0.05):
        
        for ema_period in ema_periods:
            ema_column = f'{ema_period}EMA'
            self.df[f'{ema_period}EMA_Lower'] = self.df[ema_column] * (1 - epsilon)
            self.df[f'{ema_period}EMA_Upper'] = self.df[ema_column] * (1 + epsilon)

            self.df['inside_ema_band'] = (self.df['close'] > self.df[f'{ema_period}EMA_Lower']) & (self.df['close'] < self.df[f'{ema_period}EMA_Upper'])
            self.df[f'{self.interval}_{ema_period}_recovery'] = 0
            self.df[f'{self.interval}_{ema_period}_recover_failed'] = 0

            for i in self.df.index:
                if i + self.window < self.df.index[-1]:
                    if self.df.loc[i+self.window, 'inside_ema_band']:
                        future_prices = self.df.loc[i:i+self.window, 'close']
                        if (future_prices > self.df.loc[i, f'{ema_period}EMA_Upper']).all():
                            self.df.loc[i, f'{self.interval}_{ema_period}_recovery'] = 1
                        if (future_prices < self.df.loc[i, ema_column]).all():
                            self.df.loc[i, f'{self.interval}_{ema_period}_recover_failed'] = 1

            self.df.drop(columns=[f'{ema_period}EMA_Lower', f'{ema_period}EMA_Upper', 'inside_ema_band'], inplace=True)

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

        self.df.loc[is_hammer, f'{self.interval}_Hammer_Alert'] = 1
        self.df.loc[inverse_hammer, f'{self.interval}_Inverse_Hammer_Alert'] = 0
        
        self.df.drop(columns=['body_size', 'upper_shadow', 'lower_shadow'], inplace=True)

    def apply(self):
        alert_methods = [
            self.engulf_alert,
            self.macd_alert,
            self.ema_support_alert,
            self.ema_support_alert,
            self.hammer_alert
        ]

        for alert_method in alert_methods:
            alert_method()

        alert_columns = [
            f'{self.interval}_MACD_Alert',
            f'{self.interval}_Engulf_Alert',
            f'{self.interval}_Hammer_Alert',
            f'{self.interval}_Inverse_Hammer_Alert'
        ]

        ema_recovery_alert_columns = [f'{self.interval}_{ema_period}_recovery' for ema_period in [13, 169]]
        ema_recovery_fail_alert_columns = [f'{self.interval}_{ema_period}_recover_failed' for ema_period in [13, 169]]
        
        self.df = self.df[['symbol', 'date'] + alert_columns + ema_recovery_alert_columns + ema_recovery_fail_alert_columns]
        return self.df
