import pandas as pd

class TradingStrategy:
    def __init__(self, df, initial_capital=10000):
        self.df = df
        self.initial_capital = initial_capital
        self.trades = []
        self.current_trade = {}
        self.total_values = initial_capital

    def execute_trades(self):
        for date in range(len(self.df) - 1):
            try:
                # Sell Trade
                if len(self.current_trade) != 0:
                    if (self.df['dual_channel_Alert'].iloc[date] == 0) or date == len(self.df) - 1:
                        # Has Trade and Bearish
                        self.trades.append(
                            {
                                "Entry_price": self.current_trade["entry_price"],
                                "Entry_date": self.current_trade["entry_date"],
                                "Exit_price": self.df['open'].iloc[date],
                                "Exit_date": self.df['date'].iloc[date],
                                "profit": (self.df['open'].iloc[date] / self.current_trade['entry_price']) - 1,
                                "total_asset": (self.total_values * (self.df.iloc[date].open / self.current_trade['entry_price']) - 1)
                            }
                        )
                        # Update total asset
                        self.total_values = (self.total_values * (self.df.iloc[date + 1].open / self.current_trade['entry_price']))

                        # Close Trade
                        self.current_trade = {}

                # Buy Trade
                elif (self.df['dual_channel_Alert'].iloc[date] == 1) and (len(self.current_trade) == 0):  # No Trade and Bullish
                    self.current_trade["entry_price"] = self.df['close'].iloc[date]
                    self.current_trade["entry_date"] = self.df['date'].iloc[date]

            except IndexError:
                print("No Trade Data")

    def get_trades(self):
        return pd.DataFrame(self.trades)

    def get_total_return(self):
        if self.trades:
            return self.trades[-1]['total_asset']
        return 0

# # Usage

# strategy = TradingStrategy(testing_df)
# strategy.execute_trades()
# trades_df = strategy.get_trades()
# total_return = strategy.get_total_return()

# print(f"Technical Strategic Total Return: {total_return}")