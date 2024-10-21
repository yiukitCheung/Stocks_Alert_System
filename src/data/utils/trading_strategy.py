import pandas as pd    
class TradingStrategy:
    def __init__(self, data_collection, alert_collection, initial_capital=10000):
        self.trades = []
        self.current_trade = {}
        self.total_values = initial_capital
        self.data_collection = data_collection
        self.alert_collection = alert_collection
        self.df = pd.DataFrame(list(self.data_collection.find({'date': {'$gte': pd.to_datetime('2024-01-01')},
                                                            'interval': 1}, 
                                                            {'_id': 0}))).sort_values(by=['symbol', 'date'])
        
        self.alert_df = pd.DataFrame(list(self.alert_collection.find({'date': {'$gte': pd.to_datetime('2024-01-01')},
                                                                    'interval': 1}, 
                                                                    {'_id': 0}))).sort_values(by=['symbol', 'date'])
        self.stock_candidates = pick_stock(self.alert_collection).run()
        
    def execute_trades(self):
        for idx, date in enumerate(self.df['date'].unique()):
            # Sell Trade
            if len(self.current_trade) != 0:
                tracked_processed_stock = self.df[(self.df['symbol'] == stock_of_the_day) & (self.df['date'] == date)]
                tracked_alert_stock = self.alert_df[(self.alert_df['symbol'] == stock_of_the_day) & (self.alert_df['date'] == date)]
                # Fixing comparison for velocity_alert
                if (tracked_alert_stock['velocity_alert'].iloc[0] == 'velocity_loss') or (idx == len(self.df['date'].unique()) - 1):
                    # Sell the stock and calculate the profit
                    exit_price = tracked_processed_stock['open'].iloc[0]
                    entry_price = self.current_trade['entry_price']
                    profit_rate = (exit_price / entry_price) - 1
                    
                    # Update total value based on the profit rate
                    self.total_values += self.total_values * profit_rate
                    
                    # Save the trade
                    self.trades.append({
                        "symbol": self.current_trade["symbol"],
                        "Entry_price": entry_price,
                        "Entry_date": self.current_trade["entry_date"],
                        "Exit_price": exit_price,
                        "Exit_date": tracked_processed_stock['date'].iloc[0],
                        "profit": profit_rate,
                        "total_asset": self.total_values
                    })
                    
                    # Close the current trade
                    self.current_trade = {}
                    stock_of_the_day = None
                    tracked_alert_stock = None
                    tracked_processed_stock = None
                    
                else:
                    continue
                
            # Buy Trade
            elif len(self.current_trade) == 0:  # No Trade and Bullish
                stock_of_the_day = self.stock_candidates[self.stock_candidates['date'] == date.strftime('%Y-%m-%d')]['symbol'].iloc[0]
                date_of_the_day = pd.to_datetime(self.stock_candidates[self.stock_candidates['date'] == date.strftime('%Y-%m-%d')]['date'].iloc[0])
                
                # Get the stock data
                self.current_trade["entry_price"] = self.df[(self.df['symbol'] == stock_of_the_day) & (self.df['date'] == date_of_the_day)]['close'].iloc[0]
                self.current_trade["entry_date"] = self.df[(self.df['symbol'] == stock_of_the_day) & (self.df['date'] == date_of_the_day)]['date'].iloc[0]
                self.current_trade["symbol"] = stock_of_the_day
            else:
                continue
            
    def get_trades(self):
        return pd.DataFrame(self.trades)
    
    def get_total_return(self):
        if self.trades:
            return self.total_values  # Return the latest total value
        return self.total_values

class pick_stock:
    def __init__(self, alert_collection):
        self.alert_collection = alert_collection
        
        # Fetch the data, sorted by symbol and date
        self.data = self.get_stock_dataframe()
        self.distinct_intervals = self.data['interval'].unique()
        
        # Dictionary to store stock candidates
        self.stock_candidates = {}

        # Define weights for intervals (e.g., larger intervals get more weight)
        self.interval_weights = {interval: weight for weight, interval in enumerate(self.distinct_intervals, start=1)}

    def get_stock_dataframe(self):
        # Fetch the data from MongoDB and convert to DataFrame
        data = self.alert_collection.find({'date': {'$gte': pd.to_datetime('2024-01-01')}}, {'_id': 0})
        df = pd.DataFrame(list(data))
        return df

    def evaluate_stocks(self, data):
        # Group by symbol and apply interval-based weighting for velocity alerts
        velocity_summary = data.groupby('symbol').apply(
            lambda x: pd.Series({
                'weighted_recover_169ema': sum((x['velocity_alert'] == '169ema_recovery') * x['interval'].map(self.interval_weights)),
                'weighted_recover_169ema_failed': sum((x['velocity_alert'] == '169ema_recover_failed') * x['interval'].map(self.interval_weights)),
                'weighted_velocity_loss': sum((x['velocity_alert'] == 'velocity_loss') * x['interval'].map(self.interval_weights)),
                'weighted_velocity_maintained': sum((x['velocity_alert'] == 'velocity_maintained') * x['interval'].map(self.interval_weights))
            }),include_groups=False
        ).reset_index()
        # Sort stocks by weighted values
        velocity_summary = velocity_summary.sort_values(
            by=['weighted_recover_169ema', 'weighted_velocity_maintained', 'weighted_recover_169ema_failed', 'weighted_velocity_loss'], 
            ascending=[False, False, True, True]
        )
        return velocity_summary
    
    def run(self):
        # Loop through each unique date
        for date in self.data['date'].unique():
            today_data = self.data[self.data['date'] == date]
            today_data_eval = self.evaluate_stocks(today_data)
            
            # Symbol of the day
            symbol_of_the_day = today_data_eval['symbol'].iloc[0]
            self.stock_candidates[date] = symbol_of_the_day
            
        # Return the stock candidates
        return pd.DataFrame(self.stock_candidates.items(), columns=['date', 'symbol'])