import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import DataLoader, Dataset
from torchvision import transforms
import numpy as np
from PIL import Image
from pathlib import Path
import csv

# ==================== SIMPLE CNN ====================
class SimpleCNN(nn.Module):
    """
    Petit CNN simple pour apprendre les features des montres
    Input: 224x224 RGB images
    Output: 128-dimensional embeddings
    """
    def __init__(self, embedding_dim=128):
        super(SimpleCNN, self).__init__()

        # Block 1: Conv -> ReLU -> MaxPool
        self.conv1 = nn.Conv2d(3, 32, kernel_size=3, padding=1)
        self.relu1 = nn.ReLU(inplace=True)
        self.pool1 = nn.MaxPool2d(2, 2)  # 224 -> 112

        # Block 2: Conv -> ReLU -> MaxPool
        self.conv2 = nn.Conv2d(32, 64, kernel_size=3, padding=1)
        self.relu2 = nn.ReLU(inplace=True)
        self.pool2 = nn.MaxPool2d(2, 2)  # 112 -> 56

        # Block 3: Conv -> ReLU -> MaxPool
        self.conv3 = nn.Conv2d(64, 128, kernel_size=3, padding=1)
        self.relu3 = nn.ReLU(inplace=True)
        self.pool3 = nn.MaxPool2d(2, 2)  # 56 -> 28

        # Block 4: Conv -> ReLU -> AdaptiveAvgPool
        self.conv4 = nn.Conv2d(128, 256, kernel_size=3, padding=1)
        self.relu4 = nn.ReLU(inplace=True)
        self.adaptive_pool = nn.AdaptiveAvgPool2d((1, 1))  # -> 1x1

        # Fully connected layer for embedding
        self.fc = nn.Linear(256, embedding_dim)

    def forward(self, x):
        # Conv blocks
        x = self.pool1(self.relu1(self.conv1(x)))
        x = self.pool2(self.relu2(self.conv2(x)))
        x = self.pool3(self.relu3(self.conv3(x)))
        x = self.relu4(self.conv4(x))

        # Global average pooling
        x = self.adaptive_pool(x)
        x = x.view(x.size(0), -1)  # Flatten

        # Embedding
        x = self.fc(x)
        return x


# ==================== DATASET ====================
class WatchDataset(Dataset):
    """Dataset pour charger les images préprocessées de montres"""

    def __init__(self, metadata_csv, transform=None):
        self.data = []
        with open(metadata_csv, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row['preprocessed'] == '1':  # Seulement les images trouvées
                    self.data.append({
                        'watch_id': row['watch_id'],
                        'image_path': row['image_path']
                    })

        self.transform = transform or transforms.Compose([
            transforms.Resize((224, 224)),
            transforms.ToTensor(),
            transforms.Normalize(
                mean=[0.485, 0.456, 0.406],
                std=[0.229, 0.224, 0.225]
            )
        ])

    def __len__(self):
        return len(self.data)

    def __getitem__(self, idx):
        item = self.data[idx]
        image = Image.open(item['image_path']).convert('RGB')
        image = self.transform(image)
        return image, item['watch_id']


# ==================== TRAINING ====================
def train_simple_cnn(metadata_csv, checkpoint_path="model_checkpoint.pt", epochs=10, batch_size=32):
    """Entraîner le CNN simple sur les images de montres"""

    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    print(f"Device: {device}")

    # Dataset et DataLoader
    dataset = WatchDataset(metadata_csv)
    dataloader = DataLoader(dataset, batch_size=batch_size, shuffle=True)

    print(f"✓ Dataset loaded: {len(dataset)} images")

    # Modèle
    model = SimpleCNN(embedding_dim=128).to(device)
    optimizer = optim.Adam(model.parameters(), lr=0.001)
    criterion = nn.MSELoss()  # Reconstruction loss (simple)

    print(f"✓ Model initialized: SimpleCNN with 128D embeddings")

    # Training loop
    print(f"\n{'='*60}")
    print(f"Training for {epochs} epochs")
    print(f"{'='*60}")

    for epoch in range(epochs):
        total_loss = 0.0
        for batch_idx, (images, watch_ids) in enumerate(dataloader):
            images = images.to(device)

            # Forward pass
            embeddings = model(images)

            # Simple reconstruction loss (try to maintain info)
            loss = embeddings.norm(p=2, dim=1).mean()

            # Backward pass
            optimizer.zero_grad()
            loss.backward()
            optimizer.step()

            total_loss += loss.item()

        avg_loss = total_loss / len(dataloader)
        print(f"Epoch {epoch+1}/{epochs} | Loss: {avg_loss:.4f}")

    # Sauvegarder le modèle
    torch.save({
        'model_state_dict': model.state_dict(),
        'embedding_dim': 128
    }, checkpoint_path)

    print(f"\n✓ Model saved to: {checkpoint_path}")
    return model


# ==================== INFERENCE ====================
def extract_embeddings(metadata_csv, model_path, output_csv):
    """Extraire les embeddings pour TOUTES les montres"""

    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

    # Charger le modèle
    checkpoint = torch.load(model_path, map_location=device)
    model = SimpleCNN(embedding_dim=checkpoint['embedding_dim']).to(device)
    model.load_state_dict(checkpoint['model_state_dict'])
    model.eval()

    # Dataset
    dataset = WatchDataset(metadata_csv)
    dataloader = DataLoader(dataset, batch_size=32, shuffle=False)

    print(f"\n{'='*60}")
    print(f"Extracting embeddings from {len(dataset)} images")
    print(f"{'='*60}")

    results = []
    with torch.no_grad():
        for batch_idx, (images, watch_ids) in enumerate(dataloader):
            images = images.to(device)
            embeddings = model(images).cpu().numpy()

            for watch_id, embedding in zip(watch_ids, embeddings):
                emb_str = ",".join([f"{x:.6f}" for x in embedding])
                results.append(f"{watch_id},{emb_str}")

            if (batch_idx + 1) % 10 == 0:
                print(f"  ✓ Processed {batch_idx * 32}/{len(dataset)}")

    # Sauvegarder
    with open(output_csv, 'w') as f:
        f.write("watch_id," + ",".join([f"feat_{i}" for i in range(128)]) + "\n")
        for line in results:
            f.write(line + "\n")

    print(f"\n✓ Embeddings exported to: {output_csv}")