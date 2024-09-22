import torch
from torch.utils.data import DataLoader
from sklearn.model_selection import TimeSeriesSplit
import os
import numpy as np
from tqdm import tqdm
import plotly.graph_objects as go
from train_data_loading import TimeSeriesDataset

class TimeSeriesTrainer:
    def __init__(self, model, epochs, criterion, optimizer, verbose=True):
        """
        Args:
            model (torch.nn.Module): The PyTorch model to train.
            epochs (int): Number of epochs to train.
            criterion (torch.nn.Module): The loss function.
            optimizer (torch.optim.Optimizer): The optimizer.
            scheduler (optional): Learning rate scheduler (if any).
        """
        self.l1_lambda = 0.001
        self.l2_lambda = 0.001
        
        self.model = model
        self.epochs = epochs
        self.criterion = criterion
        self.optimizer = optimizer
        self.verbose = verbose
        self.model_path = '/Users/yiukitcheung/Documents/Projects/Stocks/models/'
        
    def train(self, train_dataloader, epoch, device):
        self.model.train()

        # Initialize accumulators
        total_loss = 0
        train_pbar = tqdm(enumerate(train_dataloader), total=len(train_dataloader),
                        desc=f"Epoch {epoch + 1}/{self.epochs}")

        # Train over batches
        for _, (x_batch, y_batch) in train_pbar:
            x_batch, y_batch = x_batch.to(device), y_batch.to(device)
            self.optimizer.zero_grad()

            # Predict
            y_pred = self.model(x_batch)

            # Compute loss
            # Calculate regularization terms
            l1_regularization = torch.tensor(0.).to(device)
            l2_regularization = torch.tensor(0.).to(device)
            for param in self.model.parameters():
                l1_regularization += torch.norm(param, 1)
                l2_regularization += torch.norm(param, 2)

            # Compute Loss
            loss = self.criterion(y_pred, y_batch) + self.l1_lambda * l1_regularization + self.l2_lambda * l2_regularization
            loss.backward()
            self.optimizer.step()

            # Accumulate loss
            total_loss += loss.item()

            # Update progress bar
            train_pbar.set_postfix({'Batch Loss': f"{loss.item():.4f}"})

        # Calculate average loss
        avg_train_loss = total_loss / len(train_dataloader)

        return avg_train_loss

    def validate(self, test_dataloader, device):
        self.model.eval()

        # Initialize accumulators
        total_loss = 0

        with torch.no_grad():
            for batch in tqdm(test_dataloader, desc="Validating"):
                x_batch, y_batch = batch
                x_batch, y_batch = x_batch.to(device), y_batch.to(device)

                # Predict
                y_pred = self.model(x_batch)

                # Compute loss
                batch_loss = self.criterion(y_pred, y_batch)
                total_loss += batch_loss.item()

        # Compute average loss
        avg_val_loss = total_loss / len(test_dataloader)



        return avg_val_loss

    def run(self, df, window_size, batch_size, device):
        """
        Run the training and validation over multiple folds.
        Args:
            df (pd.DataFrame): Dataframe containing the data.
            window_size (int): The size of the sliding window.
            batch_size (int): Batch size for dataloaders.
            device (torch.device): The device to train on.
        """
        tscv = TimeSeriesSplit(n_splits=4)
        folds_val_results = {}
        folds_train_results = {}

        # Expanding Window Cross-Validation Folds
        for fold, (train_index, val_index) in enumerate(tscv.split(df)):
            train_df = df.iloc[train_index]
            val_df = df.iloc[val_index]

            # Create datasets and dataloaders
            train_dataset = TimeSeriesDataset(train_df, window_size=window_size)
            val_dataset = TimeSeriesDataset(val_df, window_size=window_size)

            train_dataloader = DataLoader(train_dataset, batch_size=batch_size, shuffle=False)
            val_dataloader = DataLoader(val_dataset, batch_size=batch_size, shuffle=False)
            
            # Initialize results containers for each fold
            fold_val_results = []
            fold_train_results = []

            # Train and validate for each epoch
            for epoch in range(self.epochs):
                avg_train_loss = self.train(train_dataloader, epoch, device)
                avg_val_loss = self.validate(val_dataloader, device)
                
                if self.verbose:
                    print(f"Fold {fold+1}: Epoch [{epoch + 1}/{self.epochs}], Train Loss: {avg_train_loss:.4f}")
                    print(f"Fold {fold+1}: Epoch [{epoch + 1}/{self.epochs}], Val Loss: {avg_val_loss:.4f}")

                # Record validation and training loss
                fold_val_results.append(avg_val_loss)
                fold_train_results.append(avg_train_loss)

            # Store results for each fold
            folds_val_results[fold] = fold_val_results
            folds_train_results[fold] = fold_train_results

        # Store the model
        self.save_model()
        
        return folds_train_results, folds_val_results
    
    def save_model(self):
        save_path = os.path.join(self.model_path+'_NVDA.pth')
        torch.save(self.model.state_dict(),save_path)
    
    def viz_performance(self, train, eval):
        # Assuming folds_train_results and folds_val_results are populated as in your example
        # Create a new figure
        fig = go.Figure()

        # Define a limited set of contrasting colors
        colors = ['blue', 'green', 'red', 'purple']

        # Number of folds
        folds = len(train)

        # Plot each fold
        for i in range(folds):
            color = colors[i // 4]  # Cycle through colors every 4 folds
            epochs = np.arange(i * len(train[i]), (i + 1) * len(train[i]))
            
            fig.add_trace(go.Scatter(x=epochs, y=train[i], mode='lines', name='Train' if i == 0 else None,
                                    line=dict(color=color, dash='solid'), showlegend=(i == 0)))
            fig.add_trace(go.Scatter(x=epochs, y=eval[i], mode='lines', name='Eval' if i == 0 else None,
                                    line=dict(color=color, dash='dash'), showlegend=(i == 0)))

            # Add vertical line to indicate fold boundary
            if i < folds - 1:
                fig.add_shape(type="line", x0=(i + 1) * len(train[i]) - 0.5,
                                y0=min(min(train[i]), min(eval[i])),
                                x1=(i + 1) * len(train[i]) - 0.5,
                                y1=max(max(train[i]), max(eval[i])),
                                line=dict(color="black", dash="dash"))

            # Add text annotation to indicate fold number
            fig.add_annotation(x=(i + 0.5) * len(train[i]),
                                y=max(max(train[i]), max(eval[i])),
                                text=f"Fold {i + 1}", showarrow=False, yshift=10)

        # Add titles and labels
        fig.update_layout(
            title='Training and Evaluation Metrics',
            xaxis_title='Epochs',
            yaxis_title='Metric',
            legend_title='Legend',
            template='plotly_white'
        )

        # Show the plot
        fig.show()
