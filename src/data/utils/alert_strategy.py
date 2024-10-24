import numpy as np

class AddAlert:
    def __init__(self, df_dict, interval, symbol):
        self.df_dict = [entry for entry in df_dict if entry['interval'] == interval]
        self.window = 15  # Number of days to compare the stock price
        self.interval = interval
        self.symbol = symbol
        self.dict = {}
    def velocity_accel_decel_alert(self, data, obs_window=15):
        """
        Adds alerts to the stock data when velocity accelerates or decelerates based on the crossing of EMA8, EMA13, and EMA169.
        Uses an observation window to determine if the stock is accelerating based on the count of 'velocity_weak' vs 'velocity_maintained'.
        
        :param data: List of stock data dictionaries. Each dictionary must contain '8EMA', '13EMA', and '169EMA'.
        :param obs_window: The number of periods to observe for determining acceleration or deceleration (default: 15).
        :return: List of dictionaries with velocity alerts ('velocity_accelerated', 'velocity_decelerated', or 'trend_change') added.
        """
        previous_velocity_status = None

        for i in range(len(data)):
            entry = data[i]
            if 'alerts' not in entry:
                entry['alerts'] = {}

            if i < obs_window:
                continue

            previous_window = data[i-obs_window:i]
            current_velocity_status = None
            
            count_velocity_loss = sum(1 for row in previous_window if 'velocity_alert' in row['alerts'] and row['alerts']['velocity_alert']['alert_type'] in ['velocity_loss', 'velocity_weak'])
            count_velocity_maintained = sum(1 for row in previous_window if 'velocity_alert' in row['alerts'] and row['alerts']['velocity_alert']['alert_type'] == 'velocity_maintained')

            if entry['8ema'] > entry['169ema'] and entry['13ema'] > entry['169ema']:
                if count_velocity_loss > count_velocity_maintained:
                    if 'velocity_accelerated' not in entry['alerts']:
                        current_velocity_status = 'accelerated'
            elif entry['8ema'] < entry['13ema']:
                if count_velocity_maintained > count_velocity_loss:
                    if 'velocity_decelerated' not in entry['alerts']:
                        current_velocity_status = 'decelerated'
            
            if current_velocity_status and current_velocity_status != previous_velocity_status:
                entry['alerts']['momentum_alert'] = {
                    "date": entry['date'],
                    "alert_type": current_velocity_status,
                    "details": f"Velocity status changed: {current_velocity_status}"
                }
                previous_velocity_status = current_velocity_status

        return data

    def velocity_alert_dict(self, data, window=3):
        """
        Adds 'velocity_alert' to the 'alerts' dictionary in each stock data entry based on the relationship between the close price, 13EMA, and 169EMA.
        Avoids adding duplicate consecutive alerts.
        
        :param data: List of stock data dictionaries. Each dictionary must contain 'close', '13EMA', and '169EMA'.
        :param window: The number of periods to look back for identifying velocity changes.
        :return: List of dictionaries with 'velocity_alert' added in the 'alerts' sub-dictionary, avoiding duplicate consecutive alerts.
        """
        for i in range(len(data)):
            if i < window:
                continue
            
            entry = data[i]
            if 'alerts' not in entry:
                entry['alerts'] = {}

            above_13EMA = entry['close'] > entry['13ema']
            above_169EMA = entry['close'] > entry['169ema']
            below_13EMA = entry['close'] < entry['13ema']
            below_169EMA = entry['close'] < entry['169ema']
            between_13_169EMA = entry['close'] <= entry['13ema'] and entry['close'] >= entry['169ema']

            current_alert_type = None

            if above_13EMA and above_169EMA:
                current_alert_type = 'velocity_maintained'
            elif any(data[j]['close'] < data[j]['13ema'] for j in range(i - window, i)) and above_13EMA:
                current_alert_type = 'velocity_negotiating'
            elif any(data[j]['close'] < data[j]['169ema'] for j in range(i - window, i)) and above_169EMA:
                current_alert_type = 'velocity_negotiating'
            elif below_13EMA and below_169EMA:
                current_alert_type = 'velocity_loss'
            elif between_13_169EMA and below_13EMA:
                current_alert_type = 'velocity_weak'
            
            if current_alert_type:
                entry['alerts']['velocity_alert'] = {
                    "date": entry['date'],
                    "alert_type": current_alert_type,
                    "details": f"Velocity alert triggered: {current_alert_type}"
                }

        return data

    def add_169ema_touch_alert(self, data, tolerance=.01):
        """
        Adds a '169ema_touched' alert to the stock data if the closing price touches the 169 EMA.
        :param data: List of stock data dictionaries.
        :param tolerance: Defines how close the closing price must be to EMA169 to count as a "touch".
        :return: List of dictionaries with alerts added.
        """
        for entry in data:
        
            # Define the upper and lower bounds for the tolerance range
            upper_bound = entry['169ema'] * (1 + tolerance)
            lower_bound = entry['169ema'] * (1 - tolerance)
            
            # Determine if the touch is from above (support) or below (resistance) using 13EMA > 169EMA
            if entry['13ema'] > entry['169ema']:
                alert_type = 'support'  # Touched from above
            elif entry['13ema'] < entry['169ema']:
                alert_type = 'resistance'  # Touched from below
            else:
                alert_type = 'neutral'

            # Check if the closing price is within the tolerance range of the 169 EMA
            if (lower_bound <= entry['close'] <= upper_bound) \
                or (lower_bound <= entry['low'] <= upper_bound) \
                or (lower_bound <= entry['high'] <= upper_bound) \
                or (lower_bound <= entry['open'] <= upper_bound):
                
                # Add the 169EMA touch alert to the entry
                if 'alerts' not in entry:
                    entry['alerts'] = {}

                entry['alerts']['169ema_touched'] = {
                    "date": entry['date'],
                    "close_price": entry['close'],
                    "13ema": entry['13ema'],
                    "169ema": entry['169ema'],
                    "type": alert_type,  # 'support' or 'resistance'
                    "details": f"Close price touched EMA169 from {alert_type} within {tolerance * 100}% tolerance"
                }
                    
            # Check for the 13 ema touch for high interval
            
            if entry['interval'] >= 7:
                # Define the upper and lower bounds for the tolerance range
                upper_bound = entry['13ema'] * (1 + tolerance)
                lower_bound = entry['13ema'] * (1 - tolerance)
            
                # Determine if the touch is from above (support) or below (resistance) using 8EMA > 13EMA
                if entry['8ema'] > entry['13ema']:
                    alert_type = 'support'
                elif entry['8ema'] < entry['13ema']:
                    alert_type = 'resistance'
                else:
                    alert_type = 'neutral'
                    
                # Check if the closing price is within the tolerance range of the 169 EMA
                if (lower_bound <= entry['close'] <= upper_bound) \
                    or (lower_bound <= entry['low'] <= upper_bound) \
                    or (lower_bound <= entry['high'] <= upper_bound) \
                    or (lower_bound <= entry['open'] <= upper_bound):
                    # If no alerts exist, create the 'alerts' dictionary
                    if 'alerts' not in entry:
                        entry['alerts'] = {}

                    # Add the 13EMA touched alert
                    entry['alerts']['13ema_touched'] = {
                        "date": entry['date'],
                        "close_price": entry['close'],
                        "13ema": entry['13ema'],
                        "type": alert_type,
                        "details": f"Close price touched EMA13 within {tolerance * 100}% tolerance"
                    }
        return data

    # Function to filter only the required keys (symbol, interval, alerts)
    def filter_data(self, data, symbol):
        filtered_data = {}
        
        # data is a list, so we need to iterate through it
        filtered_data[symbol] = [
            {   "date" : record['date'],
                'interval': record['interval'],  # Keep interval
                'alerts': record['alerts']  # Keep alerts
            }
            for record in data  # Iterate over the list directly
        ]
        
        return filtered_data
    
    def apply(self):
        self.df_dict = self.add_169ema_touch_alert(self.df_dict)
        self.df_dict = self.velocity_alert_dict(self.df_dict)
        self.df_dict = self.velocity_accel_decel_alert(self.df_dict)
        
        self.df_dict = self.filter_data(self.df_dict, self.symbol)
        
        return self.df_dict
    
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
                ema_13 = interval_data['13ema'].values
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