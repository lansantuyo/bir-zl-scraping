import pandas as pd
import json
import re
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder
from collections import Counter
from typing import List, Dict, Tuple

import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import Dataset, DataLoader
from torch.nn.utils.rnn import pad_sequence, pack_padded_sequence, pad_packed_sequence
# pip install pytorch-crf
from torchcrf import CRF

# --- Configuration & Constants ---
# (Should be similar to your Keras setup)
KEYWORDS_FOR_STARTS_WITH = ["Province", "City", "Municipality", "Barangay", "Zone", "Street"]
ZV_PATTERN_REGEX = re.compile(
    r"\d+(?:ST|ND|RD|TH)\s+(?:REVISION|Rev)(?:.*Z\.?V\.?.*SQ.*M\.?)?|"
    r"(?:\d+(?:ST|ND|RD|TH)\s+REVISION|Rev\s+ZV\s+/?.*SQ\.?\s*M\.?)|"
    r"(?:Z|2)\.?V\.?.*SQ.*M\.?|FINAL",
    re.IGNORECASE
)
VOCAB_SIZE = 20000  # Max vocabulary size for row texts
EMBEDDING_DIM = 128  # Embedding dimension for row texts
TEXT_SEQ_OUTPUT_LEN = 50  # Max words per row's concatenated text (for truncating/padding individual row texts)
LSTM_HIDDEN_DIM = 128  # Hidden dim for the main BiLSTM processing rows
BATCH_SIZE = 4  # Small batch size for demonstration
EPOCHS = 3  # Small number of epochs for demonstration
LEARNING_RATE = 1e-3
DEVICE = torch.device("cuda" if torch.cuda.is_available() else "cpu")
PAD_TOKEN = "<pad>"
UNK_TOKEN = "<unk>"


# --- Feature Engineering (Identical to your Keras version) ---
def is_numeric_str(s: str) -> bool:
    if not s: return False
    try:
        float(s)
        return True
    except ValueError:
        return False


def extract_single_row_features(raw_cells_list: list[str]) -> dict:
    scalar_features_dict = {}
    non_whitespace_cells_texts = [str(cell_text) for cell_text in raw_cells_list if
                                  cell_text is not None and str(cell_text).strip() != ""]
    concatenated_text = " ".join(non_whitespace_cells_texts)
    scalar_features_dict['num_cells_in_row'] = float(len(raw_cells_list))
    first_non_null_idx = -1
    for i, cell_text in enumerate(raw_cells_list):
        if cell_text is not None and str(cell_text).strip() != "":
            first_non_null_idx = i
            break
    scalar_features_dict['first_non_null_column'] = float(first_non_null_idx)
    last_non_null_idx = -1
    for i in range(len(raw_cells_list) - 1, -1, -1):
        cell_text = raw_cells_list[i]
        if cell_text is not None and str(cell_text).strip() != "":
            last_non_null_idx = i
            break
    scalar_features_dict['last_non_null_column'] = float(last_non_null_idx)
    scalar_features_dict['num_non_empty_cells'] = float(len(non_whitespace_cells_texts))
    if scalar_features_dict['num_cells_in_row'] > 0:
        scalar_features_dict['ratio_non_empty_cells'] = scalar_features_dict['num_non_empty_cells'] / \
                                                        scalar_features_dict['num_cells_in_row']
    else:
        scalar_features_dict['ratio_non_empty_cells'] = 0.0
    numeric_cell_count = sum(1 for text in non_whitespace_cells_texts if is_numeric_str(text))
    scalar_features_dict['num_numeric_cells'] = float(numeric_cell_count)
    if scalar_features_dict['num_non_empty_cells'] > 0:
        scalar_features_dict['ratio_numeric_cells'] = numeric_cell_count / scalar_features_dict['num_non_empty_cells']
    else:
        scalar_features_dict['ratio_numeric_cells'] = 0.0
    scalar_features_dict['is_row_empty_or_whitespace_only'] = 1.0 if scalar_features_dict[
                                                                         'num_non_empty_cells'] == 0 else 0.0
    first_cell_raw = str(raw_cells_list[0]) if raw_cells_list and raw_cells_list[0] is not None else ""
    for keyword in KEYWORDS_FOR_STARTS_WITH:
        scalar_features_dict[f'starts_with_keyword_{keyword.lower().replace(" ", "_")}'] = \
            1.0 if first_cell_raw.lower().startswith(keyword.lower()) else 0.0
    raw_concatenated_for_zv = " ".join(str(cell) for cell in raw_cells_list if cell is not None)
    scalar_features_dict['contains_keyword_zv'] = 1.0 if bool(ZV_PATTERN_REGEX.search(raw_concatenated_for_zv)) else 0.0
    feature_order = [
                        'num_cells_in_row', 'first_non_null_column', 'last_non_null_column',
                        'num_non_empty_cells', 'ratio_non_empty_cells', 'num_numeric_cells',
                        'ratio_numeric_cells', 'is_row_empty_or_whitespace_only', 'contains_keyword_zv'
                    ] + [f'starts_with_keyword_{kw.lower().replace(" ", "_")}' for kw in KEYWORDS_FOR_STARTS_WITH]
    numeric_features_list = [scalar_features_dict.get(fname, 0.0) for fname in feature_order]
    return {
        'concatenated_text_clean': concatenated_text,
        'numeric_features': np.array(numeric_features_list, dtype=np.float32)
    }


# --- PyTorch Text Vectorization Helper ---
class TextVectorizer:
    def __init__(self, max_tokens=VOCAB_SIZE, output_sequence_length=TEXT_SEQ_OUTPUT_LEN):
        self.max_tokens = max_tokens
        self.output_sequence_length = output_sequence_length
        self.vocab = {}
        self.token_to_idx = {}
        self.idx_to_token = {}
        self.pad_token_id = 0
        self.unk_token_id = 1

    def fit_on_texts(self, texts: List[str]):
        word_counts = Counter()
        for text in texts:
            word_counts.update(text.lower().split())

        # Keep most common words, reserve for pad and unk
        common_words = [word for word, count in word_counts.most_common(self.max_tokens - 2)]
        self.token_to_idx = {PAD_TOKEN: self.pad_token_id, UNK_TOKEN: self.unk_token_id}
        for i, token in enumerate(common_words):
            self.token_to_idx[token] = i + 2  # Start after pad and unk
        self.idx_to_token = {idx: token for token, idx in self.token_to_idx.items()}
        self.vocab_size = len(self.token_to_idx)
        print(f"Vocabulary size: {self.vocab_size}")

    def texts_to_sequences(self, texts: List[str]) -> List[List[int]]:
        sequences = []
        for text in texts:
            tokens = text.lower().split()
            seq = [self.token_to_idx.get(token, self.unk_token_id) for token in tokens]
            # Pad or truncate individual row text sequence
            if len(seq) < self.output_sequence_length:
                seq.extend([self.pad_token_id] * (self.output_sequence_length - len(seq)))
            else:
                seq = seq[:self.output_sequence_length]
            sequences.append(seq)
        return sequences


# --- Data Loading and Preprocessing ---
print("Loading annotations...")
try:
    df_annotations = pd.read_csv("annotations.csv")
except FileNotFoundError:
    print("Error: annotations.csv not found.")
    exit()

df_annotations['raw_cells_list'] = df_annotations['raw_cells_json'].apply(
    lambda x: json.loads(x) if pd.notna(x) else [])

print("Extracting features for each row...")
all_row_data = []
for index, row in df_annotations.iterrows():
    # Corrected to use 'raw_cells_list' as per previous discussion
    features = extract_single_row_features(row['raw_cells_list'])
    all_row_data.append({
        'text': features['concatenated_text_clean'],
        'numerics': features['numeric_features'],
        'label': row['label'],
        'filename': row['filename'],
        'sheetname': row['sheetname']
    })
df_processed_rows = pd.DataFrame(all_row_data)

# --- Group rows into sequences (sheets) ---
print("Grouping rows into sequences (sheets)...")
sheet_sequences_text_raw = []  # List of lists of strings (row texts within a sheet)
sheet_sequences_numerics = []  # List of np.arrays (numeric features for rows within a sheet)
sheet_sequences_labels_str = []  # List of lists of strings (labels for rows within a sheet)

for (fname, sname), group in df_processed_rows.groupby(['filename', 'sheetname']):
    sheet_sequences_text_raw.append(group['text'].tolist())
    sheet_sequences_numerics.append(np.stack(group['numerics'].values))
    sheet_sequences_labels_str.append(group['label'].tolist())

if not sheet_sequences_text_raw:
    print("No sequences found after grouping. Exiting.")
    exit()

# --- Label Encoding ---
print("Encoding labels...")
all_unique_labels = sorted(list(set(label for seq in sheet_sequences_labels_str for label in seq)))
label_encoder = LabelEncoder()
label_encoder.fit(all_unique_labels)
n_classes = len(label_encoder.classes_)
# PyTorch CRF typically doesn't need a separate padding label ID in n_classes itself,
# as padding is handled by a mask.
# However, for padding sequences of labels to the same length for batching,
# we might use a value outside 0..n_classes-1, e.g., -1 or n_classes.
# The CRF mask will then identify these. Let's use n_classes as padding value for labels for now.
LABEL_PAD_ID = n_classes  # or -1, but ensure consistency
print(f"Found {n_classes} unique classes: {label_encoder.classes_}. Using {LABEL_PAD_ID} as label padding ID.")

sheet_sequences_labels_encoded = [
    label_encoder.transform(seq) for seq in sheet_sequences_labels_str
]

# --- Text Vectorization ---
print("Vectorizing text data...")
all_texts_for_vocab = [text for seq in sheet_sequences_text_raw for text in seq]
text_vectorizer = TextVectorizer(max_tokens=VOCAB_SIZE, output_sequence_length=TEXT_SEQ_OUTPUT_LEN)
text_vectorizer.fit_on_texts(all_texts_for_vocab)

# Convert sheet texts to sequences of token IDs
sheet_sequences_text_ids = []
for sheet_texts in sheet_sequences_text_raw:
    sheet_sequences_text_ids.append(
        np.array(text_vectorizer.texts_to_sequences(sheet_texts), dtype=np.int64)
    )

# --- Determine n_numeric_features ---
_feature_order_for_len_calc = [
                                  'num_cells_in_row', 'first_non_null_column', 'last_non_null_column',
                                  'num_non_empty_cells', 'ratio_non_empty_cells', 'num_numeric_cells',
                                  'ratio_numeric_cells', 'is_row_empty_or_whitespace_only', 'contains_keyword_zv'
                              ] + [f'starts_with_keyword_{kw.lower().replace(" ", "_")}' for kw in
                                   KEYWORDS_FOR_STARTS_WITH]
n_numeric_features = len(_feature_order_for_len_calc)
print(f"Number of scalar numeric features per row: {n_numeric_features}")

# --- Data Splitting (Sheet-level) ---
print("Splitting data into train, validation, test sets...")
indices = np.arange(len(sheet_sequences_text_ids))  # Number of sheets
train_indices, test_indices = train_test_split(indices, test_size=0.2, random_state=42, shuffle=True)
train_indices, val_indices = train_test_split(train_indices, test_size=0.15, random_state=42, shuffle=True)


def get_split_data_pytorch(selected_indices):
    split_texts = [sheet_sequences_text_ids[i] for i in selected_indices]
    split_numerics = [torch.tensor(sheet_sequences_numerics[i], dtype=torch.float32) for i in selected_indices]
    split_labels = [torch.tensor(sheet_sequences_labels_encoded[i], dtype=torch.long) for i in selected_indices]
    # For CRF, we also need original lengths to create masks
    split_lengths = [len(seq) for seq in split_labels]  # Original number of rows in each sheet
    return split_texts, split_numerics, split_labels, split_lengths


train_texts_ids, train_numerics, train_labels, train_lengths = get_split_data_pytorch(train_indices)
val_texts_ids, val_numerics, val_labels, val_lengths = get_split_data_pytorch(val_indices)
test_texts_ids, test_numerics, test_labels, test_lengths = get_split_data_pytorch(test_indices)

print(
    f"Train sequences: {len(train_texts_ids)}, Val sequences: {len(val_texts_ids)}, Test sequences: {len(test_texts_ids)}")


# --- PyTorch Dataset and DataLoader ---
class SheetDataset(Dataset):
    def __init__(self, texts_ids_list, numerics_list, labels_list, lengths_list):
        self.texts_ids_list = texts_ids_list
        self.numerics_list = numerics_list
        self.labels_list = labels_list
        self.lengths_list = lengths_list  # original sequence lengths

    def __len__(self):
        return len(self.texts_ids_list)

    def __getitem__(self, idx):
        return {
            "texts": torch.tensor(self.texts_ids_list[idx], dtype=torch.long),  # (seq_len_sheet, text_seq_output_len)
            "numerics": self.numerics_list[idx],  # (seq_len_sheet, n_numeric_features)
            "labels": self.labels_list[idx],  # (seq_len_sheet,)
            "length": self.lengths_list[idx]  # scalar, original number of rows in sheet
        }


def collate_fn(batch):
    # Pad sequences within the batch to the max length in that batch
    texts_batch = [item['texts'] for item in batch]
    numerics_batch = [item['numerics'] for item in batch]
    labels_batch = [item['labels'] for item in batch]
    lengths_batch = torch.tensor([item['length'] for item in batch], dtype=torch.long)

    # Pad each type of sequence
    # texts: (batch_size, max_sheet_len_in_batch, text_seq_output_len)
    padded_texts = pad_sequence(texts_batch, batch_first=True, padding_value=text_vectorizer.pad_token_id)
    # numerics: (batch_size, max_sheet_len_in_batch, n_numeric_features)
    padded_numerics = pad_sequence(numerics_batch, batch_first=True, padding_value=0.0)
    # labels: (batch_size, max_sheet_len_in_batch)
    padded_labels = pad_sequence(labels_batch, batch_first=True, padding_value=LABEL_PAD_ID)

    return {
        "texts": padded_texts,
        "numerics": padded_numerics,
        "labels": padded_labels,
        "lengths": lengths_batch
    }


train_dataset = SheetDataset(train_texts_ids, train_numerics, train_labels, train_lengths)
val_dataset = SheetDataset(val_texts_ids, val_numerics, val_labels, val_lengths)
test_dataset = SheetDataset(test_texts_ids, test_numerics, test_labels, test_lengths)

train_loader = DataLoader(train_dataset, batch_size=BATCH_SIZE, collate_fn=collate_fn, shuffle=True)
val_loader = DataLoader(val_dataset, batch_size=BATCH_SIZE, collate_fn=collate_fn)
test_loader = DataLoader(test_dataset, batch_size=BATCH_SIZE, collate_fn=collate_fn)


# --- PyTorch Model Definition ---
class TextFeatureExtractor(nn.Module):
    def __init__(self, vocab_size, embedding_dim, text_seq_output_len):
        super().__init__()
        self.embedding = nn.Embedding(vocab_size, embedding_dim, padding_idx=text_vectorizer.pad_token_id)
        # GlobalAveragePooling1D equivalent for (batch * sheet_len, text_seq_output_len, embedding_dim)
        # We will apply mean over the text_seq_output_len dimension
        # self.pool = nn.AdaptiveAvgPool1d(1) # Could also work if input is (N, C, L)

    def forward(self, text_input_per_row):
        # text_input_per_row shape: (N, text_seq_output_len) where N = batch_size * sheet_len
        embedded_text = self.embedding(text_input_per_row)  # (N, text_seq_output_len, embedding_dim)
        # Mask out padding tokens before averaging
        mask = (text_input_per_row != text_vectorizer.pad_token_id).unsqueeze(-1).float()  # (N, text_seq_output_len, 1)
        masked_embedded_text = embedded_text * mask
        summed_embeddings = masked_embedded_text.sum(dim=1)  # (N, embedding_dim)
        non_padding_counts = mask.sum(dim=1)  # (N, 1)
        non_padding_counts = non_padding_counts.clamp(min=1e-9)  # Avoid division by zero
        text_features = summed_embeddings / non_padding_counts  # (N, embedding_dim)
        return text_features


class NumericFeatureExtractor(nn.Module):
    def __init__(self, n_numeric_features):
        super().__init__()
        # Using LayerNorm as a simple normalization.
        # BatchNorm1d would need to be adapted on the training set's flattened numeric features.
        self.norm = nn.LayerNorm(n_numeric_features)

    def forward(self, numeric_input_per_row):
        # numeric_input_per_row shape: (N, n_numeric_features)
        return self.norm(numeric_input_per_row)


class RowClassifierSequenceModel(nn.Module):
    def __init__(self, vocab_size, embedding_dim, text_seq_output_len, # This param is for TextFeatureExtractor
                 n_numeric_features, lstm_hidden_dim, n_classes):
        super().__init__()
        # text_seq_output_len is used here for the sub-module
        self.text_branch = TextFeatureExtractor(vocab_size, embedding_dim, text_seq_output_len)
        self.numeric_branch = NumericFeatureExtractor(n_numeric_features)

        self.combined_feature_dim = embedding_dim + n_numeric_features
        self.bilstm = nn.LSTM(self.combined_feature_dim, lstm_hidden_dim,
                              bidirectional=True, batch_first=True)
        self.fc_to_crf = nn.Linear(lstm_hidden_dim * 2, n_classes)  # LSTM output to emission scores
        self.crf = CRF(n_classes, batch_first=True) # Assuming CRF is imported/defined

    def forward(self, sheet_texts_ids, sheet_numerics, sheet_lengths, targets=None):
        # sheet_texts_ids: (batch_size, max_sheet_len, actual_text_seq_len_in_this_batch_input)
        # sheet_numerics: (batch_size, max_sheet_len, n_numeric_features)
        # sheet_lengths: (batch_size,) original lengths of sheets in the batch

        # Correctly get all dimensions from the input tensor's shape
        batch_size, max_sheet_len, current_text_seq_len = sheet_texts_ids.shape

        # Reshape for row-wise processing
        # (batch_size * max_sheet_len, current_text_seq_len)
        texts_flat = sheet_texts_ids.view(-1, current_text_seq_len) # Use the dimension from the input

        # The n_numeric_features is implicitly handled by sheet_numerics.shape[2]
        # (batch_size * max_sheet_len, n_numeric_features)
        numerics_flat = sheet_numerics.view(-1, sheet_numerics.shape[2])


        processed_text_flat = self.text_branch(texts_flat)  # (batch*sheet_len, embedding_dim)
        processed_numerics_flat = self.numeric_branch(numerics_flat)  # (batch*sheet_len, n_numeric_features)

        # Reshape back to sequence
        # (batch_size, max_sheet_len, embedding_dim)
        processed_text_sequence = processed_text_flat.view(batch_size, max_sheet_len, -1)
        # (batch_size, max_sheet_len, n_numeric_features)
        processed_numeric_sequence = processed_numerics_flat.view(batch_size, max_sheet_len, -1)

        merged_row_features = torch.cat([processed_text_sequence, processed_numeric_sequence], dim=2)
        # (batch_size, max_sheet_len, embedding_dim + n_numeric_features)

        # Pack padded sequence for LSTM
        # sheet_lengths should be on CPU for pack_padded_sequence if using older PyTorch versions
        # For newer versions, it might handle GPU tensors directly. Let's keep .cpu() for wider compatibility.
        packed_input = pack_padded_sequence(merged_row_features, sheet_lengths.cpu(),
                                            batch_first=True, enforce_sorted=False)
        packed_output, _ = self.bilstm(packed_input)
        bilstm_output, _ = pad_packed_sequence(packed_output, batch_first=True, total_length=max_sheet_len)
        # (batch_size, max_sheet_len, lstm_hidden_dim * 2)

        emissions = self.fc_to_crf(bilstm_output)  # (batch_size, max_sheet_len, n_classes)

        # Create mask for CRF: True for non-padded elements
        # Mask shape should be (batch_size, max_sheet_len)
        mask = torch.arange(max_sheet_len, device=emissions.device).expand(len(sheet_lengths), max_sheet_len) < sheet_lengths.unsqueeze(1).to(emissions.device)
        # Ensure mask is on the same device as emissions

        if targets is not None:
            # CRF expects tags to be LongTensor, mask to be ByteTensor (or BoolTensor in newer PyTorch)
            # Ensure targets are also on the correct device
            loss = -self.crf(emissions, targets.to(emissions.device), mask=mask, reduction='mean')  # Negative log-likelihood
            return loss
        else:
            # For inference
            decoded_sequence = self.crf.decode(emissions, mask=mask)  # List of lists of tag indices
            return decoded_sequence

# --- Model Initialization ---
print("Defining PyTorch model...")
model = RowClassifierSequenceModel(
    vocab_size=text_vectorizer.vocab_size,
    embedding_dim=EMBEDDING_DIM,
    text_seq_output_len=TEXT_SEQ_OUTPUT_LEN,
    n_numeric_features=n_numeric_features,
    lstm_hidden_dim=LSTM_HIDDEN_DIM,
    n_classes=n_classes
).to(DEVICE)

optimizer = optim.Adam(model.parameters(), lr=LEARNING_RATE)

# --- Training Loop ---
print("Training model...")
for epoch in range(EPOCHS):
    model.train()
    total_train_loss = 0
    for i, batch in enumerate(train_loader):
        texts = batch['texts'].to(DEVICE)
        numerics = batch['numerics'].to(DEVICE)
        labels = batch['labels'].to(DEVICE)
        lengths = batch['lengths'].to(DEVICE)  # lengths are already on device if collate_fn puts them there

        optimizer.zero_grad()
        loss = model(texts, numerics, lengths, targets=labels)

        if loss is not None:  # Should always be not None during training
            loss.backward()
            optimizer.step()
            total_train_loss += loss.item()

        if (i + 1) % 10 == 0:  # Print every 10 batches
            print(f"Epoch [{epoch + 1}/{EPOCHS}], Batch [{i + 1}/{len(train_loader)}], Loss: {loss.item():.4f}")

    avg_train_loss = total_train_loss / len(train_loader)
    print(f"Epoch [{epoch + 1}/{EPOCHS}] - Training Loss: {avg_train_loss:.4f}")

    # Validation
    model.eval()
    total_val_loss = 0
    with torch.no_grad():
        for batch in val_loader:
            texts = batch['texts'].to(DEVICE)
            numerics = batch['numerics'].to(DEVICE)
            labels = batch['labels'].to(DEVICE)
            lengths = batch['lengths'].to(DEVICE)

            loss = model(texts, numerics, lengths, targets=labels)
            if loss is not None:
                total_val_loss += loss.item()

    avg_val_loss = total_val_loss / len(val_loader)
    print(f"Epoch [{epoch + 1}/{EPOCHS}] - Validation Loss: {avg_val_loss:.4f}")

print("Training complete.")

# --- Inference/Prediction (Example on one batch from test set) ---
print("\nMaking predictions on a sample from test set...")
model.eval()
if len(test_dataset) > 0:
    # Get a sample batch
    sample_batch = next(iter(test_loader))
    texts_sample = sample_batch['texts'].to(DEVICE)
    numerics_sample = sample_batch['numerics'].to(DEVICE)
    labels_sample_true = sample_batch['labels']  # Keep on CPU for comparison
    lengths_sample = sample_batch['lengths'].to(DEVICE)

    with torch.no_grad():
        predicted_sequences_encoded = model(texts_sample, numerics_sample, lengths_sample)  # List of lists

    # Display predictions for the first sheet in the batch
    idx_to_show = 0
    if idx_to_show < len(predicted_sequences_encoded):
        pred_tags_for_sheet = predicted_sequences_encoded[idx_to_show]  # This is a list of tag indices
        true_tags_for_sheet = labels_sample_true[idx_to_show][
                              :lengths_sample[idx_to_show]].tolist()  # Get true labels up to original length

        # Find original text for this sample (more involved as DataLoader shuffles)
        # For simplicity, we'll just show predicted vs true tags
        # To get original text, you'd need to map back from test_loader to test_dataset indices or not shuffle test_loader.

        print(f"\nSample Prediction for sheet {idx_to_show} (length {lengths_sample[idx_to_show].item()}):")
        predicted_labels_str = label_encoder.inverse_transform(pred_tags_for_sheet)
        true_labels_str = label_encoder.inverse_transform(true_tags_for_sheet)

        for i in range(lengths_sample[idx_to_show].item()):
            # original_text_snippet = "..." # Would need to fetch original text for this row
            print(f"Row {i}: True='{true_labels_str[i]}', Predicted='{predicted_labels_str[i]}'")
    else:
        print("Sample index out of bounds for the batch.")

else:
    print("Test set is empty, cannot make predictions.")

# --- To save the model ---
# torch.save(model.state_dict(), "row_classifier_sequence_model.pth")
# print("PyTorch Model state_dict saved.")
# To save text vectorizer (vocab) and label encoder:
# import pickle
# with open('text_vectorizer.pkl', 'wb') as f:
# pickle.dump(text_vectorizer, f)
# with open('label_encoder.pkl', 'wb') as f:
# pickle.dump(label_encoder, f)
# print("Text vectorizer and label encoder saved.")

# To load:
# model_loaded = RowClassifierSequenceModel(...) # Instantiate model first
# model_loaded.load_state_dict(torch.load("row_classifier_sequence_model.pth"))
# model_loaded.to(DEVICE)
# model_loaded.eval()
# with open('text_vectorizer.pkl', 'rb') as f:
#     loaded_vectorizer = pickle.load(f)
# with open('label_encoder.pkl', 'rb') as f:
#     loaded_label_encoder = pickle.load(f)
