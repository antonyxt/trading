import os
import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import DataLoader
from tqdm import tqdm
import shutil
import numpy as np
from pathlib import Path

from dove_model import DovePeakPredictor
from dove_dataset import StreamingDoveDataset, FEATURE_LIST

# -----------------------------
# Collate function
# -----------------------------
def numpy_collate_fn(batch):
    dove_ids, x_seqs, y_targets = zip(*batch)
    dove_ids_tensor = torch.tensor(dove_ids, dtype=torch.long)
    x_seqs_tensor = torch.tensor(np.array(x_seqs, dtype=np.float32))
    y_targets_tensor = torch.tensor(np.array(y_targets, dtype=np.int64), dtype=torch.long)
    return dove_ids_tensor, x_seqs_tensor, y_targets_tensor

# -----------------------------
# Multi-class Focal Loss
# -----------------------------
class FocalLossMultiClass(nn.Module):
    def __init__(self, alpha=None, gamma=2.0, reduction='mean'):
        """
        Args:
            alpha (Tensor): A list/tensor of weights for each class. 
            gamma (float): Focusing parameter.
        """
        super().__init__()
        # Ensure alpha is a tensor on the correct device
        self.alpha = alpha 
        self.gamma = gamma
        self.reduction = reduction

    def forward(self, logits, targets):
        # 1. Standard Cross Entropy (per sample)
        ce_loss = nn.functional.cross_entropy(logits, targets, reduction='none')
        
        # 2. Calculate pt (probability of the correct class)
        pt = torch.exp(-ce_loss)
        
        # 3. Basic Focal Loss
        focal_loss = (1 - pt) ** self.gamma * ce_loss
        
        # 4. Apply Alpha Weighting (This is the fix!)
        if self.alpha is not None:
            # We index into alpha using the target labels
            # Resulting in a weight for every sample in the batch
            at = self.alpha.gather(0, targets.data)
            focal_loss = at * focal_loss
            
        return focal_loss.mean() if self.reduction == 'mean' else focal_loss.sum()

# -----------------------------
# DataLoader helper
# -----------------------------
def get_dataloader(dataset, loader_batch_size=256, shuffle=True, num_workers=2, rankDistribution = []):
    counts = np.array(rankDistribution)
    # Calculate weight per class (1 / count)
    class_weights = 1.0 / counts
    # Map every single record to its class weight
    # If your labels are 1-7, we subtract 1 to get index 0-6
    sample_weights = class_weights[dataset.all_labels]
    # Convert to Double for precision and create the Sampler
    sampler = torch.utils.data.WeightedRandomSampler(
        weights=torch.from_numpy(sample_weights).double(),
        num_samples=len(sample_weights),
        replacement=True
    )
    return DataLoader(
        dataset,
        batch_size=loader_batch_size,
        sampler=sampler, # Sampler handles the 'shuffling' logic now
        num_workers=num_workers,
        collate_fn=numpy_collate_fn
    )

# -----------------------------
# Main training function
# -----------------------------
def train_model(db_path, norm_stat_path, drive_dir, date_threshold, symbol_map, checkpoint_path=None,
                num_doves=2000, seq_len=150, label_col="rank", batch_size=128,
                epochs=20, lr=1e-4, temp_dir='./temp', device=None, num_workers=2, rankDistribution = []):
    
    device = device or torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    print(f"Using device: {device}")

    # Load dataset
    dataset = StreamingDoveDataset(db_path, norm_stat_path, date_threshold = date_threshold, symbol_map= symbol_map, seq_len=seq_len, 
                                   label_col=label_col)
    dataloader = get_dataloader(dataset, batch_size, shuffle=True, num_workers=num_workers, rankDistribution = rankDistribution)
    dataset.save_symbols_details(Path(drive_dir))

    # Initialize model
    model = DovePeakPredictor(
        num_doves=num_doves,
        seq_len=seq_len,
        num_features=len(dataset.features)
    ).to(device)
    model = torch.compile(model)

    if checkpoint_path and os.path.exists(checkpoint_path):
        print(f"Loading checkpoint: {checkpoint_path}")
        model.load_state_dict(torch.load(checkpoint_path, map_location=device))

    # Optimizer, scheduler, loss
    optimizer = optim.AdamW(model.parameters(), lr=lr)
    # --- Inside train_model function ---

    # 1. Define your counts (from your data)
    counts = torch.tensor(rankDistribution, dtype=torch.float)
    # 2. Calculate inverse weights
    weights = 1.0 / counts
    # 3. Normalize so the smallest weight (Rank 4) is 1.0 (improves stability)
    weights = weights / weights.min() 

    # 4. Initialize the new Weighted Focal Loss
    # Move weights to the same device as the model
    criterion = FocalLossMultiClass(alpha=weights.to(device), gamma=2.0)
    #criterion = FocalLossMultiClass(alpha=1.0, gamma=2.0)
    scheduler = optim.lr_scheduler.ReduceLROnPlateau(optimizer, mode='min', factor=0.5, patience=2)

    # Ensure drive directory exists
    os.makedirs(drive_dir, exist_ok=True)

    # Training loop
    for epoch in range(epochs):
        model.train()
        total_loss = 0.0
        num_batches = 0
        batch_bar = tqdm(dataloader, desc=f"Epoch {epoch+1}/{epochs}")

        for batch_idx, (dove_ids, x, y) in enumerate(batch_bar):
            # -----------------------------
            # Move full loader batch to GPU once
            # -----------------------------
            dove_ids = dove_ids.to(device, non_blocking=True)
            x = x.to(device, non_blocking=True)
            y = y.to(device, non_blocking=True)
            optimizer.zero_grad(set_to_none=True)

            # Forward pass (FULL batch)
            pred = model(x, dove_ids)

            if y.dim() > 1:
                y = y[:, -1] if y.size(1) > 1 else y.squeeze(1)
            y = y.view(-1).long()

            # Safety checks
            assert y.dim() == 1, f"Expected 1D target, got {y.shape}"
            assert y.min() >= 0 and y.max() < pred.size(1), \
                f"Target labels out of range 0–{pred.size(1)-1}, got [{y.min()},{y.max()}]"

            # Compute loss + backward + step
            loss = criterion(pred, y)
            loss.backward()
            torch.nn.utils.clip_grad_norm_(model.parameters(), 1.0)
            optimizer.step()

            total_loss += loss.item()
            num_batches += 1
            batch_bar.set_postfix(loss=f"{loss.item():.4f}")

            

        avg_loss = total_loss / num_batches
        print(f"Epoch {epoch+1}/{epochs} avg loss: {avg_loss:.4f}")
        
        scheduler.step(avg_loss)

        # Save checkpoint (move to drive_dir)
        temp_path = os.path.join(temp_dir, f'dove_model_epoch{epoch+1}.pt')
        os.makedirs(temp_dir, exist_ok=True)
        torch.save(model.state_dict(), temp_path)

        drive_path = os.path.join(drive_dir, f'dove_model_epoch{epoch+1}.pt')
        shutil.move(temp_path, drive_path)

    # Final model save
    final_temp = os.path.join(temp_dir, 'dove_model_final.pt')
    torch.save(model.state_dict(), final_temp)
    final_drive = os.path.join(drive_dir, 'dove_model_final.pt')
    shutil.move(final_temp, final_drive)

    return model
