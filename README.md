# Stock Alert System - Hybrid Streaming and Batch Data Pipeline

This project implements a **hybrid data pipeline** for streaming and batch processing of stock data. The system is designed to:

- **Stream** small-interval candlestick data to generate real-time technical patterns and support levels, which are displayed on a live dashboard.
- **Batch** process larger datasets to add technical features, enabling machine learning (ML) and deep learning (DL) models to predict future stock returns. The ML pipeline for predictions is currently under development and not included in this repository yet.

## Project Structure

├── jupyter_notebook/
│   ├── alert_sending.ipynb            # Notebook for testing the alert-sending functionality
│   ├── model_performance_study.ipynb  # Study of model performance metrics
│   ├── stream_processing.ipynb        # Streaming data process demo
│   ├── trading_automated_process.ipynb # Automated trading demo process
│   ├── trading_manual_process.ipynb   # Manual trading demo process
│
├── model_repository/                   # Pre-trained model storage
│   └── _NVDA.pth                       # Example pre-trained PyTorch model file
│
├── src/
│   ├── data/
│   │   ├── utils/
│   │   │   ├── alert_strategy.py        # Alert generation strategy based on stock price data
│   │   │   ├── batch_alert.py           # Batch alert mechanism for significant stock movements
│   │   │   ├── features_engineering.py  # Feature extraction for ML models
│   │   │   ├── real_time_alert.py       # Real-time alert processing script
│   │   │   ├── trading_strategy.py      # Trading strategy logic for stock signals
│   │   │   ├── train_data_loading.py    # Data loader for training datasets
│   │   ├── data_extract.py              # Data extraction from APIs
│   │   ├── data_ingest.py               # Ingest stock data into the pipeline
│   │   ├── data_preprocess.py           # Data preprocessing and cleaning
│   │   ├── data_viz.py                  # Data visualization tools (e.g., candlestick chart generation)
│   │   ├── datastream_transform.py      # Transform streaming data for alert generation
│   │   ├── make_train.py                # Generate training datasets from the raw data
│
│   ├── model/                           # Placeholder for ML model scripts (currently in development)
│
├── train_data_repository/               # Contains the parquet files for training
│   ├── AAPL_train_data.parquet          # Apple stock training data
│   ├── AMZN_train_data.parquet          # Amazon stock training data
│   ├── DJIA_train_data.parquet          # Dow Jones Index training data
│   ├── IWM_train_data.parquet           # Russell 2000 ETF training data

## Key Features

### 1. Real-Time Alerts
- **Streaming Data**: The system fetches live stock candlestick data at small intervals (e.g., 1 minute or 5 minutes).
- **Alert Generation**: Real-time alerts are generated based on support/resistance levels and candlestick pattern recognition, which can be visualized on a dashboard.
  
### 2. Batch Data Processing
- **Technical Feature Engineering**: Larger datasets are processed to add technical features like moving averages, RSI, MACD, etc. This enriched data will eventually feed into ML and DL models to predict stock returns.
- **Data Storage**: Batch data is stored in Parquet format in the `train_data_repository` for future ML model training.

### 3. Model Training (Upcoming)
- **Model Pipeline**: A model pipeline is under development to train machine learning models on batch-processed stock data to predict future stock price movements.
- **Model Storage**: Pre-trained models will be stored in `model_repository/`.

## Notebooks Overview
- **`alert_sending.ipynb`**: Demonstrates the process of sending alerts based on real-time stock data.
- **`stream_processing.ipynb`**: Shows the streaming data processing for alert generation.
- **`model_performance_study.ipynb`**: Explores various performance metrics of machine learning models.
- **`trading_automated_process.ipynb`**: Simulates an automated trading process based on strategy signals.
- **`trading_manual_process.ipynb`**: Simulates a manual trading process using stock signals.

## Installation and Setup

1. Clone the repository:

   git clone https://github.com/your-username/stocks-alert-system.git
   cd stocks-alert-system