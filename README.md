# Stock Alert System - Hybrid Streaming and Batch Data Pipeline

This project implements a **hybrid data pipeline** for streaming and batch processing of stock data. The system is designed to:

- **Stream** small-interval candlestick data to generate real-time technical patterns and support levels, which are displayed on a live dashboard.
- **Batch** fetch and process larger interval, like daily and weekly, datasets to add technical features, enabling machine learning (ML) and deep learning (DL) models to predict future stock returns. The ML pipeline for predictions is currently under development and not implemented in this pipeline yet.

## Key Features

### 1. Real-Time Alerts
- **Streaming Data**: The system fetches and analyzes live stock candlestick data at small intervals (e.g., 15-minute or 5-minute).
- **Alert Generation**: Real-time alerts are generated based on support/resistance levels and candlestick pattern recognition, which can be visualized on a dashboard.

### 2. Batch Data Processing
- **Technical Feature Engineering**: Larger datasets are processed to add technical features like exponential moving average, MACD, etc. In addition, a trading strategy alert is generated on top of the technical features as bullish/bearish signals. This enriched data will eventually feed into ML and DL models to predict stock returns.
- **Data Storage**: Raw batch/streaming data, processed batch/streaming data, and training data are stored in the cloud-managed data lake via MongoDB Atlas. Training data are stored also in dictonary.

### 3. Strategy Sandbox Testing
- **Trading Strategy**: A designed trading strategy implemented to create bullish/bearish signals for large interval batch data.
- **Sandbox Testing**: A principal of $10,000 is initialized to buy and sell based on the strategy, generating the profit, win, and loss trades in a dashboard to demonstrate the strategy performance.

### 4. Interactive Dashboard
- **Interactive Dashboard**: The system includes a dashboard that allows users to:
- **Strategy Performance View**: View the performance of the trading strategy for each stock, showing metrics such as profit, win/loss ratio, and trade details.
- **Live Alerts**: The dashboard provides live alerts for the current trading day, displaying buy/sell signals generated in real-time based on the streaming stock data.
  
The dashboard is designed for both real-time monitoring and retrospective analysis, allowing users to visually inspect the technical indicators, alerts, and outcomes of the trading strategy in an interactive way.

### 5. Model Training (Upcoming)
- **Model Pipeline**: A model pipeline is under development to train machine learning models on batch-processed stock data to predict future stock price movements.
- **Model Storage**: Pre-trained models will be stored in `model_repository/`.

## Notebooks Overview
- **`alert_sending.ipynb`**: Demonstrates the process of sending alerts based on real-time stock data.
- **`stream_processing.ipynb`**: Shows the streaming data processing for alert generation.
- **`model_performance_study.ipynb`**: Explores various performance metrics of machine learning models.
- **`trading_automated_process.ipynb`**: Simulates an automated trading process based on strategy signals.
- **`trading_manual_process.ipynb`**: Simulates a manual trading process using stock signals.

### Usage

- **Streaming Alerts:**  Click the ready to use streamlit link to check the live alert of bullish or bearish signals in short term page; check the earning performance of impelmented strategy for each selected stock in long term page. 

### Future Work

- **Model Training Integration:** The pipeline for training ML/DL models on stock data is currently under development.

- **Improved Alert Logic:** Further improvements to alert strategies using more advanced pattern recognition and AI-based techniques.
- **Complete the Dashboard:** Complete the dashboard to include a performance section that demonstrates strategy results based solely on the strategy rather than evaluating the strategy performance in each stock individually.
