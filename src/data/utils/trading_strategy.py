import pandas as pd
import numpy as np

class DailyTradingStrategy:
    def __init__(self, data_collection, alert_collection, stock_candidates, start_date=None, end_date=None,
                 sandbox_mode=False, initial_capital=10000, aggressive_split=1.):

        self.trades = []
        self.current_trade = {"conservative": {}, "aggressive": {}}
        self.start_date = start_date if sandbox_mode else '2024-01-01'
        self.end_date = end_date if sandbox_mode else '2024-10-23'
        # Define split for aggressive and conservative capital
        self.aggressive_capital = initial_capital * aggressive_split
        self.conservative_capital = initial_capital * (1.0 - aggressive_split)
        self.capital_split = aggressive_split  # Correct spelling
        self.data_collection = data_collection
        self.alert_collection = alert_collection
        self.df = pd.DataFrame(list(self.data_collection.find({'date': {'$gte': pd.to_datetime(self.start_date),
                                                                        "$lte": pd.to_datetime(self.end_date)},
                                                               'interval': 1}, {'_id': 0}))).sort_values(
            by=['symbol', 'date'])
        self.alert_df = self.get_alert_dataframe()
        self.stock_candidates = stock_candidates
        self.aggressive_hold_day = 0

    def get_alert_dataframe(self):
        # Fetch the data from MongoDB and convert to DataFrame
        data = list(self.alert_collection.find({'date': {'$gte': pd.to_datetime(self.start_date),
                                                         "$lte": pd.to_datetime(self.end_date)}},
                                               {'_id': 0}))

        # Extract the alerts from the alert_dict
        for entry in data:
            if 'alerts' in entry and 'momentum_alert' in entry['alerts']:
                entry['momentum_alert'] = entry['alerts']['momentum_alert']['alert_type']
            if 'alerts' in entry and "velocity_alert" in entry['alerts']:
                entry['velocity_alert'] = entry['alerts']['velocity_alert']['alert_type']
            if 'alerts' in entry and '169ema_touched' in entry['alerts']:
                entry['touch_type'] = entry['alerts']['169ema_touched']['type']
            elif 'alerts' in entry and '13ema_touched' in entry['alerts']:
                entry['touch_type'] = entry['alerts']['13ema_touched']['type']
            else:
                entry['touch_type'] = np.nan

        # Convert the alert_dict to a DataFrame
        data = pd.DataFrame(data)
        data = data.drop(columns=['alerts'])

        return data

    # Execute both types of trades
    def execute_critical_trades(self):
        for idx, date in enumerate(self.df['date'].unique()):
            # Handle conservative trades
            self.manage_trade("conservative", date, idx)
            # Handle aggressive trades
            self.manage_trade("aggressive", date, idx)

    def manage_trade(self, trade_type, date, idx):

        # ==================== Selling Logic =========================== #
        # If there is an ongoing trade for the day                       #
        # Check if the stock is still a good hold based on the alerts    #
        # ============================================================== #

        if len(self.current_trade[trade_type]) != 0:

            # Step 1: Get the alert data and processed data for the ongoing trade
            stock = self.current_trade[trade_type]["symbol"]
            tracked_processed_stock = self.df[(self.df['symbol'] == stock) & (self.df['date'] == date)]
            tracked_alert_stock = self.alert_df[(self.alert_df['symbol'] == stock) &
                                                (self.alert_df['date'] == date) &
                                                (self.alert_df['interval'] == 1)]
            if tracked_alert_stock.empty:
                return

            # Step 2.1: Conservative selling rule: Sell if the stock fails to recover 13 EMA or 169 EMA
            if trade_type == "conservative":
                if (tracked_alert_stock['velocity_alert'].iloc[0] == '169ema_recover_failed') \
                        or (idx == len(self.df['date'].unique()) - 1):
                    self.track_profit_loss(trade_type, tracked_processed_stock, sell=True)

                    # ================== Conservative Capital Protection ================ #
                    # If the total capital falls below the initial capital, stop trading  #
                    # =================================================================== #

                    # if len(self.current_trade[trade_type]) != 0:
                    #     current_conser_captial = self.track_profit_loss(trade_type, tracked_processed_stock, tracked_alert_stock)
                    #     if current_conser_captial <= self.conservative_capital * (1 - self.captial_split):
                    #         self.track_profit_loss(trade_type, tracked_processed_stock, tracked_alert_stock, sell=True)

            # Step 2.2: Aggressive selling rule: Sell if the stock fails to maintain velocity or inverse hammer
            if trade_type == "aggressive":

                if tracked_alert_stock['velocity_alert'].iloc[0] == 'velocity_weak':
                    self.track_profit_loss(trade_type, tracked_processed_stock, sell=True)

                # # Step 2.3: Aggressive chase trade: If the stock maintains velocity, chase the trade
                # elif (tracked_alert_stock['velocity_alert'].iloc[0] == 'velocity_maintained')\
                #     and (self.aggressive_hold_day == 5):
                #     self.chase_trade(trade_type, date)

                #     # Reset the aggressive hold day
                #     self.aggressive_hold_day = 0

                # # Step 2.4: Track the number of days the stock has been held
                # self.aggressive_hold_day += 1

                # ================== Aggressive Capital Protection ================== #
                # If the total capital falls below the initial capital, stop trading  #
                # =================================================================== #
                #
                # if len(self.current_trade[trade_type]) != 0:
                #     current_aggr_captial = self.track_profit_loss(trade_type, tracked_processed_stock)
                #     if current_aggr_captial <= self.aggressive_capital * self.captial_split:
                #         self.track_profit_loss(trade_type, tracked_processed_stock, sell=True)
                #         print("Aggressive Capital Protection")

        # ==================== Buying Logic =========================== #
        # If there is no ongoing trade for the day                      #
        # Check if there is a stock candidate for the day               #
        # ============================================================= #

        elif len(self.current_trade[trade_type]) == 0:
            # Step 1: Get the stock candidate for the day
            stock_of_the_day = self.stock_candidates[self.stock_candidates['date'] == date.strftime('%Y-%m-%d')]
            if len(stock_of_the_day) == 0:
                return
            # Step 2: Find the stock based on the desired alert
            desired_alert = ['velocity_maintained'] if trade_type == "conservative" else 'velocity_maintained'
            stock = self.find_alert(trade_type, stock_of_the_day['symbol'].iloc[0], pd.to_datetime(date), self.alert_df,
                                    desired_alert)

            # Step 3: Buy the stock if found
            if not stock:
                return
            # Step 3.1: Agggresive trade: Test buy with 10% of aggressive capital (Conservative trade will buy full)
            # if trade_type == "aggressive":
            #     self.current_trade[trade_type]["testing_buy"] = True  # Flag for testing buy
            #     self.current_trade[trade_type]["testing_capital"] = self.aggressive_capital * 0.1  # 10% of aggressive capital
            #     self.aggressive_capital *= 0.9  # Hold back 90% for potential all-in later

            # Step 4: Update the current trade
            self.current_trade[trade_type]["entry_price"] = \
                self.df[(self.df['symbol'] == stock) & (self.df['date'] == date)]['close'].iloc[0]
            self.current_trade[trade_type]["entry_date"] = \
                self.df[(self.df['symbol'] == stock) & (self.df['date'] == date)]['date'].iloc[0]
            self.current_trade[trade_type]["symbol"] = stock

    # def chase_trade(self, trade_type, date):
    #     stock = self.current_trade[trade_type]["symbol"]
    #     tracked_processed_stock = self.df[(self.df['symbol'] == stock) & (self.df['date'] == date)]

    #     # Apply the remaining capital to the aggressive trade
    #     self.aggressive_capital += self.current_trade[trade_type]["testing_capital"]
    #     self.current_trade[trade_type]["testing_capital"] = 0

    #     # Compute the average entry price
    #     self.current_trade[trade_type]["entry_price"] = (self.current_trade[trade_type]["entry_price"] + tracked_processed_stock['close'].iloc[0]) / 2
    #     self.current_trade[trade_type]["entry_date"] = tracked_processed_stock['date'].iloc[0]
    #     self.current_trade[trade_type]["symbol"] = stock
    #     self.current_trade[trade_type]["testing_buy"] = False  # Reset testing buy flag

    # Separate method to handle selling a stock
    def track_profit_loss(self, trade_type, tracked_processed_stock, sell=False):
        # Step 1: Calculate profit/loss rate
        exit_price = tracked_processed_stock['close'].iloc[0]
        entry_price = self.current_trade[trade_type]['entry_price']
        profit_rate = (exit_price / entry_price) - 1
        if sell:
            # Step 2: Update the capital based on the profit rate and trade type
            if trade_type == "conservative":
                self.conservative_capital += self.conservative_capital * profit_rate

            elif trade_type == "aggressive":
                # # Check if it's a testing buy
                # if self.current_trade[trade_type]['testing_buy'] == True:
                #     # Apply profit rate only to testing buy capital if testing buy
                #     testing_capital = self.current_trade[trade_type]["testing_capital"]
                #     self.aggressive_capital += (testing_capital * (profit_rate + 1))
                # else:
                # Apply profit to full aggressive capital if it's fully invested
                self.aggressive_capital += self.aggressive_capital * profit_rate

            # Save the trade
            self.trades.append({
                "type": trade_type,
                "symbol": self.current_trade[trade_type]["symbol"],
                "Entry_price": entry_price,
                "Entry_date": self.current_trade[trade_type]["entry_date"],
                "Exit_price": exit_price,
                "Exit_date": tracked_processed_stock['date'].iloc[0],
                "profit/loss": f"{profit_rate * 100 :.2f}%",
                "total_conser_asset": self.conservative_capital,
                "total_aggr_asset": self.aggressive_capital,
                "total_asset": self.conservative_capital + self.aggressive_capital
            })

            # Reset current trade
            self.current_trade[trade_type] = {}
        else:
            if trade_type == "conservative":
                return self.conservative_capital + self.conservative_capital * profit_rate
            elif trade_type == "aggressive":
                return self.aggressive_capital + self.aggressive_capital * profit_rate

    def find_alert(self, trade_type, symbol_list: list, date, alert_df: pd.DataFrame, desired_alert: list):

        # if trade_type == "conservative":
        #     for symbol in symbol_list:
        #         symbol_df = alert_df[(alert_df['symbol'] == symbol) & (alert_df['interval'] == 1) & (alert_df['date'] == date)]
        #         print(symbol_df)
        #         velocity_alert = symbol_df['velocity_alert'].iloc[0]
        #         macd_alert = symbol_df['macd_alert'].iloc[0]
        #         if not symbol_df.empty and macd_alert and velocity_alert:
        #
        #             if macd_alert in desired_alert and velocity_alert in desired_alert:
        #
        #                 return symbol
        #
        #         else:
        #             continue

        if trade_type == "aggressive":
            for symbol in symbol_list:
                symbol_df = alert_df[
                    (alert_df['symbol'] == symbol) & (alert_df['interval'] == 1) & (alert_df['date'] == date)]
                velocity_alert = symbol_df['velocity_alert'].iloc[0]
                if not symbol_df.empty and \
                        velocity_alert:
                    if velocity_alert != 'velocity_loss':
                        return symbol
                else:
                    continue

    def run_trading_strategy(self):
        self.execute_critical_trades()

    def get_trades(self):
        return pd.DataFrame(self.trades)

    def get_total_return(self):
        total_capital = self.conservative_capital + self.aggressive_capital
        return total_capital

class Pick_Stock:
    def __init__(self, alert_collection, start_date=None, sandbox_mode=False):
        # Set Sandbox mode and start date accordingly
        self.sandbox_mode = sandbox_mode
        if self.sandbox_mode:
            self.start_date = start_date
        else:
            self.start_date = '2024-01-01'

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
        data = list(self.alert_collection.find({'date': {'$gte': pd.to_datetime(self.start_date)}}, {'_id': 0}))
        # Extract the alerts from the alert_dict
        for entry in data:
            if 'alerts' in entry and 'momentum_alert' in entry['alerts']:
                entry['momentum_alert'] = entry['alerts']['momentum_alert']['alert_type']
            if 'alerts' in entry and '169ema_touched' in entry['alerts']:
                entry['touch_type'] = entry['alerts']['169ema_touched']['type']
            elif 'alerts' in entry and '13ema_touched' in entry['alerts']:
                entry['touch_type'] = entry['alerts']['13ema_touched']['type']
            else:
                entry['touch_type'] = np.nan

        # Convert the alert_dict to a DataFrame
        data = pd.DataFrame(data)
        data = data.drop(columns=['alerts'])

        return data

    def evaluate_stocks(self, data):
        # Group by symbol and apply interval-based weighting for velocity alerts
        evaluate_summary = data.copy()

        # Map interval weights
        evaluate_summary['interval_weight'] = evaluate_summary['interval'].map(self.interval_weights)
        # Calculate weighted values for each alert type
        alert_types = ['touch_type', 'momentum_alert']
        for alert in alert_types:
            evaluate_summary[f'weighted_{alert}'] = evaluate_summary[alert] * evaluate_summary['interval_weight']

        # Group by symbol and sum the weighted values
        weighted_columns = [f'weighted_{alert}' for alert in alert_types]
        velocity_summary = evaluate_summary.groupby('symbol')[weighted_columns].sum().reset_index()

        # Sort stocks by weighted values
        sort_order = ['weighted_momentum_alert', 'weighted_touch_type']
        velocity_summary = velocity_summary.sort_values(by=sort_order, ascending=[False, False])
        velocity_summary = velocity_summary[(velocity_summary['weighted_momentum_alert'] > 0) |
                                            (velocity_summary['weighted_touch_type'] > 0)]

        return velocity_summary

    def run(self):
        for date in self.data['date'].unique():
            today_data = self.data[self.data['date'] == date]
            today_data_eval = self.evaluate_stocks(today_data)
            # Symbol of the day
            if not today_data_eval.empty:
                symbol_of_the_day = today_data_eval['symbol'].tolist()[:1]
            else:
                symbol_of_the_day = []
            self.stock_candidates[date] = symbol_of_the_day
            # Return the stock candidates
        return pd.DataFrame(self.stock_candidates.items(), columns=['date', 'symbol'])
