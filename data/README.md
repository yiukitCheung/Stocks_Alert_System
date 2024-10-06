# Data Repository for Stock Alert System

This repository contains three main types of data used in the Stock Alert System: **Raw**, **Processed**, and **Trainable** data. Each type of data serves a different purpose in the pipeline, from initial data gathering to preparation for machine learning and deep learning models.

## Data Types

### 1. Raw Data
- **Source**: The raw data is collected from [Yahoo Finance](https://www.yahoofinance.com) using the Python package [yfinance](https://pypi.org/project/yfinance/).
- **Content**: Raw stock data such as open price, close price, high, low, and volume for each stock symbol.
- **Usage**: This data is fetched at various intervals (e.g., 5-minute, daily, or weekly) and serves as the foundation for technical analysis and further feature engineering.

#### Raw Data Example (Columns):
- **Date**: Timestamp of the stock data.
- **Open**: Opening price.
- **High**: Highest price during the interval.
- **Low**: Lowest price during the interval.
- **Close**: Closing price.
- **Volume**: The volume of traded shares.

### 2. Processed Data
- **Content**: This dataset includes the raw data with additional technical features and signals based on the trading strategy. These features include indicators such as moving averages, MACD, and buy/sell signals derived from the strategy logic.
- **Usage**: The processed data is used for strategy evaluation, visualization, and as a pre-step to creating trainable data for machine learning models.

### Processed Data Example (Columns):
- **Date**: Timestamp of the stock data in UNIX format.
- **Symbol**: The stock symbol.
- **Open**: The opening price of the stock at the beginning of the interval.
- **High**: The highest price reached during the interval.
- **Low**: The lowest price reached during the interval.
- **Close**: The closing price of the stock at the end of the interval.
- **Volume**: The total volume of shares traded during the interval.
  
#### Technical Indicators:
- **8EMA**: The 8-period Exponential Moving Average.
- **13EMA**: The 13-period Exponential Moving Average.
- **144EMA**: The 144-period Exponential Moving Average.
- **169EMA**: The 169-period Exponential Moving Average.
  - **169EMA_Lower**: The lower bound of the 169-period EMA channel.
  - **169EMA_Upper**: The upper bound of the 169-period EMA channel.
- **MACD**: The Moving Average Convergence Divergence value.
  - **MACD_SIGNAL**: The signal line of the MACD indicator.
  - **MACD_HIST**: The histogram difference between MACD and the signal line.
  - **MACD_Alert**: Alert signal based on MACD conditions (1 for bullish, -1 for bearish).
  - **MACD_GOLDEN_CROSS**: Boolean indicating whether a golden cross has occurred (True/False).
  
#### Custom Alerts & Signals:
- **Dual_Channel_Alert**: An alert based on dual-channel strategy (e.g., -1 for sell).
- **382_Alert**: A Fibonacci 38.2% retracement alert (e.g., 0 or -1).
- **Engulf_Alert**: An alert based on engulfing candlestick patterns (e.g., -1 for bearish engulfing).
- **Incremental_High**: Boolean indicating whether the current high is incrementally higher than the previous interval (True/False).

#### Candlestick Pattern:
- **CandleStickType**: The type of candlestick pattern (e.g., "green" for bullish or "red" for bearish).
- **BodyDiff**: The body size of the candlestick (e.g., 1.6265).

#### Previous Close Prices:
- **Close_t-1**: The closing price from one interval prior.
- **Close_t-2**: The closing price from two intervals prior.
- **Close_t-3**: The closing price from three intervals prior.

#### Other Technical Features:
- **ATR (Average True Range)**: Measures the stock's volatility.

### 3. Trainable Data
- **Content**: This is the final dataset prepared for machine learning and deep learning models. It is split into training and testing sets, and features are preprocessed for model input, including one-hot encoding for categorical variables and ensuring correct data types.
- **Usage**: Trainable data is used to build and evaluate machine learning models. This data has been preprocessed for compatibility with deep learning models, ensuring proper data types and encoding.

## File Formats

- **Raw Data**: `dictionary` (in original format from Yahoo Finance).
- **Processed Data**: `documents` (with added technical indicators and signals).
- **Trainable Data**: `documents` (split into `train` and `test` sets, ready for model input).

## Usage Guide

1. **Raw Data**: Used as the foundation for all further processing. You can pull the raw data using the `yfinance` library.
2. **Processed Data**: Use these files to generate trading signals or as input for additional analyses and visualizations.
3. **Trainable Data**: Ready for training machine learning models. Load these files directly into your training pipeline for deep learning model training.

## Future Work

1. **Advanced API Call**: Use a premium API for direct technical indicator fetching to save computation power, avoiding the need to fetch historical stock data for indicator computation.
2. **Fundamentals Pre-screening**: Use a specialized API for fundamental filtering to fetch stocks with great potential or strong financial health, ensuring that the stocks selected are sound in both technical and fundamental aspects.