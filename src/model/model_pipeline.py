import torch
import torch.nn as nn
from utilis import model_architecture, model_trainer, train_data_loading

def main():
    # Initialize the class with the database and collection details
    prepare_data = train_data_loading.StockDataPreprocessor(db_name="stock_data_db",
                                        collection_name="stock_data", 
                                        symbol="NVDA")

    # Fetch data from MongoDB
    prepare_data.fetch_data()
    # Split the data into training and testing datasets
    prepare_data.split_data()

    # Retrieve training and test data
    train_data = prepare_data.get_train_data()
    test_data = prepare_data.get_test_data()

    # Initialize the preprocessor with columns to exclude from log transformation
    preprocessor = train_data_loading.DataPreprocessor(exclude_columns=['MACD', 'MACD_SIGNAL', 'MACD_HIST'])
    processed_train_data = preprocessor.preprocess(train_data)

    # Define model, criterion, optimizer, and device

    # Set device to apple silicon if available
    if torch.backends.mps.is_available():
        device = torch.device("mps")
        print("MPS device found.")
    else:
        print ("MPS device not found.")

    # Initialize the model
    model = model_architecture.TransformerModel(input_dim = processed_train_data.shape[1]-1, 
                                                d_model=64, nhead=4, num_encoder_layers=2,
                                                num_decoder_layers=2, dim_feedforward=256, dropout=0.1)

    # Apply weight initialization                          
    model.apply(model_architecture.init_weights)

    # Set model to device
    model.to(device)

    # Define loss function and optimizer
    criterion = nn.MSELoss()
    optimizer = torch.optim.Adam(model.parameters(), lr=0.001)

    # Create the trainer object
    trainer = model_trainer.TimeSeriesTrainer(model=model, 
                                            epochs=10,
                                            criterion=criterion,
                                            optimizer=optimizer,
                                            verbose=False)

    # Run training with cross-validation
    folds_train_results, folds_val_results = trainer.run(df=processed_train_data, 
                                                        window_size=30, 
                                                        batch_size=32, 
                                                        device=device)

def __main__():
    main()