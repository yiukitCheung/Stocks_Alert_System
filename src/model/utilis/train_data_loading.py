import pymongo

import pandas as pd
import numpy as np

from sklearn.preprocessing import OneHotEncoder

import torch
from torch.utils.data import Dataset

import plotly.graph_objects as go

import pymongo
import pandas as pd

class StockDataPreprocessor:
    def __init__(self, db_name, collection_name, symbol, mongo_uri="mongodb://localhost:27017/"):
        """
        Initializes the MongoDB connection and prepares the collection for the stock data.

        Args:
            db_name (str): Name of the MongoDB database.
            collection_name (str): Name of the collection inside the database.
            symbol (str): The stock symbol to filter the data (e.g., 'NVDA').
            mongo_uri (str): MongoDB connection URI (default is localhost).
        """
        self.mongo_uri = mongo_uri
        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client[db_name]
        self.collection = self.db[collection_name]
        self.symbol = symbol
        self.df = None
        self.df_test = None

    def fetch_data(self):
        """Fetches stock data from the MongoDB collection and converts it to a Pandas DataFrame."""
        # Fetch the data from MongoDB
        data = self.collection.find({})
        
        # Convert the data to a Pandas DataFrame
        df = pd.DataFrame(list(data))
        
        # Extract the desired stock symbol's technical data
        df_filtered = df[df['symbol'] == self.symbol]
        if not df_filtered.empty:
            # Extract the 'technical_data' field from the filtered data
            self.df = pd.DataFrame(df_filtered['technical_data'].iloc[0])
        else:
            raise ValueError(f"No data found for symbol: {self.symbol}")

    def split_data(self):
        """Splits the data into training and testing datasets."""
        if self.df is not None:
            # Take the latest 2900 rows for training
            self.df = self.df[-3000:-100]  # From -3000 to -100 rows for training

            # Take the rest (last 100 rows) for testing
            self.df_test = self.df[-100:]
        else:
            raise ValueError("Data not loaded. Call fetch_data() first.")

    def get_train_data(self):
        """Returns the training data."""
        if self.df is not None:
            return self.df
        else:
            raise ValueError("Data not loaded or split. Call fetch_data() and split_data() first.")

    def get_test_data(self):
        """Returns the test data."""
        if self.df_test is not None:
            return self.df_test
        else:
            raise ValueError("Test data not available. Call split_data() first.")

# You can now use `train_data` and `test_data` for training and testing your model.

class DataPreprocessor:
    def __init__(self, exclude_columns=None):
        """
        Initializes the DataPreprocessor with the columns to exclude from log transformation.

        Args:
            exclude_columns (list): List of numeric columns to exclude from log transformation.
        """
        self.exclude_columns = exclude_columns if exclude_columns else ['MACD', 'MACD_SIGNAL', 'MACD_HIST']
        self.ohe = OneHotEncoder(drop='first')  # OneHotEncoder for categorical columns
    
    def preprocess(self, df):
        """
        Preprocess the dataframe by performing the following steps:
        - Drop 'date' column and rows with missing values
        - Convert alert-related columns to categorical types
        - Log-transform numeric columns except for excluded columns
        - One-hot encode categorical columns
        - Add a timestamp column

        Args:
            df (pd.DataFrame): The input dataframe to preprocess.

        Returns:
            pd.DataFrame: The preprocessed dataframe.
        """
        # Step 1: Drop 'date' column and handle missing values
        df = df.drop(columns=['date'], errors='ignore')  # Avoids error if 'date' is missing
        df = df.dropna()

        # Step 2: Convert relevant columns to category
        df["CandleStickType"] = df["CandleStickType"].astype('category')
        df["Incremental_High"] = df["Incremental_High"].astype('category')
        df["MACD_GOLDEN_CROSS"] = df["MACD_GOLDEN_CROSS"].astype('category')

        alert_columns = df.columns[df.columns.str.contains('Alert')]
        for column in alert_columns:
            df[column] = df[column].astype('category')

        # Step 3: Log transform numeric columns, excluding specified columns
        numeric_df = df.select_dtypes(include=['float64', 'int64'])
        cols_to_transform = [col for col in numeric_df.columns if col not in self.exclude_columns]
        numeric_df[cols_to_transform] = np.log1p(numeric_df[cols_to_transform])

        # Concatenate excluded numeric columns with the transformed ones
        numeric_df = pd.concat([df[self.exclude_columns], numeric_df[cols_to_transform]], axis=1)

        # Reset index for the numeric DataFrame
        numeric_df = numeric_df.reset_index(drop=True)

        # Step 4: One-hot encode categorical columns
        categorical_df = df.select_dtypes(include=['category'])
        encoded_df = pd.DataFrame(self.ohe.fit_transform(categorical_df).toarray(), 
                                columns=self.ohe.get_feature_names_out(categorical_df.columns))

        # Step 5: Concatenate numeric and encoded categorical data
        df = pd.concat([numeric_df, encoded_df], axis=1)

        # Step 6: Add timestamp column
        df['timestamp'] = df.index

        return df
    
class TimeSeriesDataset(Dataset):
    def __init__(self, dataframe, window_size):

        self.window_size = window_size
        
        # Convert dataframe to tensors
        features = torch.tensor(dataframe.drop(['close'],axis=1).values, dtype=torch.float32)
        targets = torch.tensor(dataframe['close'].values, dtype=torch.float32)

        self.windows = []
        self.targets = []
        
        # Genearate windows
        max_num_windows = len(features) - self.window_size 
        
        for i in range(max_num_windows):
        
            # Extract window data and target
            window_data_tensor = features[i:i + self.window_size]
            target = targets[i + self.window_size] # Predict the next day's close price
            
            # Append to the list
            self.windows.append(window_data_tensor)
            self.targets.append(target)

    def __len__(self):
        return len(self.windows)

    def __getitem__(self, idx):
        if idx >= len(self.windows):
            raise IndexError("Index out of range")
        return self.windows[idx], self.targets[idx]