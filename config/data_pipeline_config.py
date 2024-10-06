# config/config_loader.py

import yaml
import os

def load_pipeline_config():
    """
    Loads the Data Pipeline configuration from the data_pipeline_config.yaml file.
    
    Returns:
        dict: data_pipeline_config configuration dictionary.
    """
    config_path = os.path.join(os.path.dirname(__file__), "data_pipeline_config.yaml")
    try:
        with open(config_path, "r") as file:
            config = yaml.safe_load(file)
        return config['data_pipeline']
    except FileNotFoundError:
        print("Configuration file not found. Please ensure config/mongo.yaml exists.")
        raise
    except yaml.YAMLError as e:
        print("Error reading the YAML configuration file.")
        raise e