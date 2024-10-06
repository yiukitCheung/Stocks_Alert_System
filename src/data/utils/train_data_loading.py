import pymongo, os, sys
import pandas as pd
import numpy as np
from sklearn.preprocessing import OneHotEncoder
# Add project root to sys.path dynamically
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))
from config.mongodb_config import load_mongo_config
from config.data_pipeline_config import load_pipeline_config

class fetch_and_split_data:
    def __init__(self, 
                symbol:list, 
                mongodb_config = load_pipeline_config(),
                data_pipeline_config = load_pipeline_config()):
        """
        Initializes the MongoDB connection and prepares the collection for the stock data.

        Args:
            db_name (str): Name of the MongoDB database.
            collection_name (str): Name of the collection inside the database.
            symbol (str): The stock symbol to filter the data (e.g., 'NVDA').
            mongo_uri (str): MongoDB connection URI (default is localhost).
        """
        self.client = pymongo.MongoClient(mongodb_config['url'])
        self.db = self.client[load_mongo_config()['db_name']]
        self.collection = mongodb_config['process_collection_name']
        self.save_path = data_pipeline_config['make_train']['save_path']
        self.symbol = symbol
        self.df = None
        self.df_test = None

    def fetch_data(self):
        """Fetches stock data from the MongoDB collection and converts it to a Pandas DataFrame."""
        # Fetch the data from MongoDB
        fetched_data_lst = list(self.collection.find({"symbol": self.symbol,
                                                    "interval": "daily"}))

        # Extract the desired stock symbol's technical data
        if not len(fetched_data_lst) == 0:
            # Extract the 'technical_data' field from the filtered data
            self.df = pd.DataFrame(fetched_data_lst)
        else:
            raise ValueError(f"No data found for symbol: {self.symbol}")

    def split_data(self):
        """Splits the data into training and testing datasets."""
        if self.df is not None:
            # Take the latest rows for training
            self.df_train = self.df[-252*3:-100] 

            # Take the rest (last 100 rows) for testing
            self.df_test = self.df[-100:]
        else:
            raise ValueError("Data not loaded. Call fetch_data() first.")

    def get_train_data(self):
        """Returns the training data."""
        if self.df_train is not None:
            return self.df_train
        else:
            raise ValueError("Data not loaded or split. Call fetch_data() and split_data() first.")

    def get_test_data(self):
        """Returns the test data."""
        if self.df_test is not None:
            return self.df_test
        else:
            raise ValueError("Test data not available. Call split_data() first.")

class prepare_data:
    def __init__(self, exclude_columns=None):
        """
        Initializes the DataPreprocessor with the columns to exclude from log transformation.

        Args:
            exclude_columns (list): List of numeric columns to exclude from log transformation.
        """
        self.exclude_columns = exclude_columns if exclude_columns else ['MACD', 'MACD_SIGNAL', 'MACD_HIST']
        self.ohe = OneHotEncoder(drop='first')  # OneHotEncoder for categorical columns
        
    def preprocess(self, df, train=True):
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
        self.filename = '{self.symbol}_train_data.parquet' if train else '{self.symbol}_test_data.parquet'

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

        # Step 3: One-hot encode categorical columns
        categorical_df = df.select_dtypes(include=['category'])
        encoded_df = pd.DataFrame(self.ohe.fit_transform(categorical_df).toarray(), 
                                columns=self.ohe.get_feature_names_out(categorical_df.columns))

        # Step 4: Concatenate numeric and encoded categorical data
        numeric_df = df.select_dtypes(include=['float64', 'int64'])
        numeric_df = numeric_df.reset_index(drop=True)
        
        df = pd.concat([numeric_df, encoded_df], axis=1)

        # Step 5: Add timestamp column
        df['timestamp'] = df.index

        # Create target variable
        df = self.create_target(df)
        
        # Save the prepared data for model training
        self.save_data(df, self.save_path)
        
        return df
    
    def create_target(self, df):
        # Compute the return % of the next day
        df['log_daily_return'] = np.log(df.close.pct_change() + 1)
        
        return df
    
    def save_data(self, df, file_path):
        """Saves the preprocessed dataframe to a CSV file."""
        file_path = os.path.join(file_path, self.filename)
        df.to_parquet(file_path, index=False)