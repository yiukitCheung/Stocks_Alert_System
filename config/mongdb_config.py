# config/config_loader.py

import yaml
import os

def load_mongo_config():
    """
    Loads the MongoDB configuration from the mongo.yaml file.
    
    Returns:
        dict: MongoDB configuration dictionary.
    """
    config_path = os.path.join(os.path.dirname(__file__), "mongo.yaml")
    try:
        with open(config_path, "r") as file:
            config = yaml.safe_load(file)
        return config['mongodb']
    except FileNotFoundError:
        print("Configuration file not found. Please ensure config/mongo.yaml exists.")
        raise
    except yaml.YAMLError as e:
        print("Error reading the YAML configuration file.")
        raise e