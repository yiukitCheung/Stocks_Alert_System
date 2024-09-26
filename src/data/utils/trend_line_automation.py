import numpy as np
import pandas as pd

class TrendLineAutomation:
    def __init__(self, data_path: str, lookback: int = 30):
        self.data = pd.read_csv(data_path)
        self.data['date'] = self.data['date'].astype('datetime64[s]')
        self.data = self.data.set_index('date')
        self.data = np.log(self.data)  # Take natural log of data to resolve price scaling issues
        self.lookback = lookback
        self.support_slope = [np.nan] * len(self.data)
        self.resist_slope = [np.nan] * len(self.data)

    def check_trend_line(self, support: bool, pivot: int, slope: float, y: np.array):
        intercept = -slope * pivot + y[pivot]
        line_vals = slope * np.arange(len(y)) + intercept
        diffs = line_vals - y

        if support and diffs.max() > 1e-5:
            return -1.0
        elif not support and diffs.min() < -1e-5:
            return -1.0

        err = (diffs ** 2.0).sum()
        return err

    def optimize_slope(self, support: bool, pivot: int, init_slope: float, y: np.array):
        slope_unit = (y.max() - y.min()) / len(y)
        opt_step = 1.0
        min_step = 0.0001
        curr_step = opt_step
        best_slope = init_slope
        best_err = self.check_trend_line(support, pivot, init_slope, y)
        assert best_err >= 0.0

        get_derivative = True
        derivative = None
        while curr_step > min_step:
            if get_derivative:
                slope_change = best_slope + slope_unit * min_step
                test_err = self.check_trend_line(support, pivot, slope_change, y)
                derivative = test_err - best_err

                if test_err < 0.0:
                    slope_change = best_slope - slope_unit * min_step
                    test_err = self.check_trend_line(support, pivot, slope_change, y)
                    derivative = best_err - test_err

                if test_err < 0.0:
                    raise Exception("Derivative failed. Check your data.")

                get_derivative = False

            if derivative > 0.0:
                test_slope = best_slope - slope_unit * curr_step
            else:
                test_slope = best_slope + slope_unit * curr_step

            test_err = self.check_trend_line(support, pivot, test_slope, y)
            if test_err < 0 or test_err >= best_err:
                curr_step *= 0.5
            else:
                best_err = test_err
                best_slope = test_slope
                get_derivative = True

        return best_slope, -best_slope * pivot + y[pivot]

    def fit_trendlines_single(self, data: np.array):
        x = np.arange(len(data))
        coefs = np.polyfit(x, data, 1)
        line_points = coefs[0] * x + coefs[1]
        upper_pivot = (data - line_points).argmax()
        lower_pivot = (data - line_points).argmin()
        support_coefs = self.optimize_slope(True, lower_pivot, coefs[0], data)
        resist_coefs = self.optimize_slope(False, upper_pivot, coefs[0], data)
        return support_coefs, resist_coefs

    def fit_trendlines_high_low(self, high: np.array, low: np.array, close: np.array):
        x = np.arange(len(close))
        coefs = np.polyfit(x, close, 1)
        line_points = coefs[0] * x + coefs[1]
        upper_pivot = (high - line_points).argmax()
        lower_pivot = (low - line_points).argmin()
        support_coefs = self.optimize_slope(True, lower_pivot, coefs[0], low)
        resist_coefs = self.optimize_slope(False, upper_pivot, coefs[0], high)
        return support_coefs, resist_coefs

    def calculate_trendlines(self):
        for i in range(self.lookback - 1, len(self.data)):
            candles = self.data.iloc[i - self.lookback + 1: i + 1]
            support_coefs, resist_coefs = self.fit_trendlines_high_low(candles['high'], candles['low'], candles['close'])
            self.support_slope[i] = support_coefs[0]
            self.resist_slope[i] = resist_coefs[0]

# Usage example
if __name__ == "__main__":
    trendline_automation = TrendLineAutomation('BTCUSDT86400.csv')
    trendline_automation.calculate_trendlines()
    print(trendline_automation.support_slope)
    print(trendline_automation.resist_slope)