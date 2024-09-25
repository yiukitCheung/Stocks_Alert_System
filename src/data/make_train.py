from utils.train_data_loading import fetch_and_split_data, prepare_data

def main():
    symbol = "NVDA"
    data_loader = fetch_and_split_data(symbol=symbol)
    data_loader.fetch_data()
    data_loader.split_data()
    train_df = data_loader.get_train_data()
    
    data_preparation = prepare_data()

    data_preparation.preprocess(train_df)
    
if __name__ == "__main__":
    main()
    