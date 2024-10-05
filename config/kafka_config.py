import os
def load_kafka_config():

    """
    Loads the Kafka configuration from the client.properties file.
    
    """
    config = {}
    config_path = os.path.join(os.path.dirname(__file__), "client.properties")

    with open(config_path) as fh:
        for line in fh:
            line = line.strip()
            if len(line) != 0 and line[0] != "#":
                parameter, value = line.strip().split('=', 1)
                config[parameter] = value.strip()
    return config