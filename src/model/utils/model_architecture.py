import torch
import torch.nn as nn
import math

class PositionalEncoding(nn.Module):
    def __init__(self, d_model, dropout=0.1, max_len=256):
        super(PositionalEncoding, self).__init__()
        self.dropout = nn.Dropout(p=dropout)

        pe = torch.zeros(max_len, d_model)
        position = torch.arange(0, max_len, dtype=torch.float).unsqueeze(1)
        div_term = torch.exp(torch.arange(0, d_model, 2).float() * (-math.log(10000.0) / d_model))
        pe[:, 0::2] = torch.sin(position * div_term)
        pe[:, 1::2] = torch.cos(position * div_term)
        pe = pe.unsqueeze(0).transpose(0, 1)
        self.register_buffer('pe', pe)

    def forward(self, x):
        x = x + self.pe[:x.size(0), :]
        return self.dropout(x)

class TransformerModel(nn.Module):
    def __init__(self, input_dim, d_model, nhead, num_encoder_layers,
                num_decoder_layers, dim_feedforward, dropout):
        super(TransformerModel, self).__init__()
        self.linear_encoder = nn.Linear(input_dim, d_model)
        self.pos_encoder = PositionalEncoding(d_model, dropout)
        self.transformer = nn.Transformer(d_model, nhead, num_encoder_layers, num_decoder_layers, dim_feedforward, dropout)
        self.ffn = nn.Sequential(
            nn.Linear(d_model, dim_feedforward),
            nn.BatchNorm1d(dim_feedforward),
            nn.ReLU(),
            nn.Linear(dim_feedforward, d_model),
            nn.BatchNorm1d(d_model),
            nn.ReLU()
        )
        self.linear_decoder = nn.Linear(d_model, 1)
        self.d_model = d_model

    def forward(self, x):
        # Validate h3_indices

        # Encode the input features
        x = self.linear_encoder(x) * math.sqrt(self.d_model)
        # Add positional encoding to input features
        x = self.pos_encoder(x)
        # Transformer expects inputs in the shape (S, N, E) -> (sequence_length, batch_size, d_model)
        x = x.transpose(0, 1)
        # Pass through the transformer
        transformer_output = self.transformer(x, x)

        # Apply feedforward network with BatchNorm1d
        batch_size, seq_len, feature_dim = transformer_output.size()
        transformer_output = transformer_output.contiguous().view(-1, feature_dim)
        # Apply feedforward network
        transformer_output = self.ffn(transformer_output)
        # Use the last time step for each sequence in the batch
        transformer_output = transformer_output.view(seq_len, batch_size, self.d_model).transpose(0, 1).contiguous()
        output = self.linear_decoder(transformer_output[-1])
        return output.squeeze(1)

def init_weights(m):
    if type(m) in [nn.Linear, nn.Conv2d]:
        nn.init.xavier_uniform_(m.weight)
        if m.bias is not None:
            nn.init.zeros_(m.bias)
