"""
TauGrid Managed Storage Layer - Proof-of-Concept Training Script (Section 4.4)

Demonstrates the full storage lifecycle:
  1. Read data from the HotLoad NVMe path (/data/datasets/...)
  2. Run a synthetic training loop with dummy tensors
  3. Save checkpoint .pt to /checkpoints/
  4. Write 0-byte .done marker to trigger the checkpoint sidecar

The sidecar detects the .done file via inotify, locks the .pt, uploads to
Azure Blob, verifies integrity, then evicts local copies - without blocking
this training loop.
"""

import os
import time
import hashlib
import pathlib
import logging
import torch
import torch.nn as nn
from torch.utils.data import DataLoader, Dataset

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("tau-train")

DATASET_PATH   = pathlib.Path(os.getenv("DATASET_PATH",   "/data/datasets"))
CHECKPOINT_DIR = pathlib.Path(os.getenv("CHECKPOINT_DIR", "/checkpoints"))
SHARED_DIR     = pathlib.Path(os.getenv("SHARED_DIR",     "/shared"))
NUM_EPOCHS     = int(os.getenv("NUM_EPOCHS", "10"))
BATCH_SIZE     = int(os.getenv("BATCH_SIZE", "32"))

IMG_SIZE    = 224
NUM_CLASSES = 1000

CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)
SHARED_DIR.mkdir(parents=True, exist_ok=True)


class HotLoadDataset(Dataset):
    """Reads from the NVMe-cached dataset path populated by the init container."""

    def __init__(self, root: pathlib.Path, size: int = 1024):
        self.root  = root
        self.size  = size
        self.files = sorted(root.rglob("*.jpg")) if root.exists() else []
        if self.files:
            log.info("HotLoadDataset: %d real files under %s", len(self.files), root)
        else:
            log.info("HotLoadDataset: no files at %s - using synthetic tensors", root)

    def __len__(self):
        return len(self.files) if self.files else self.size

    def __getitem__(self, idx):
        x = torch.randn(3, IMG_SIZE, IMG_SIZE)
        y = torch.randint(0, NUM_CLASSES, (1,)).item()
        return x, y


class SimpleVisionModel(nn.Module):
    def __init__(self):
        super().__init__()
        self.backbone = nn.Sequential(
            nn.Conv2d(3, 64, kernel_size=7, stride=2, padding=3),
            nn.BatchNorm2d(64),
            nn.ReLU(inplace=True),
            nn.AdaptiveAvgPool2d((7, 7)),
            nn.Flatten(),
        )
        self.head = nn.Linear(64 * 49, NUM_CLASSES)

    def forward(self, x):
        return self.head(self.backbone(x))


def save_checkpoint(model: nn.Module, epoch: int, loss: float) -> pathlib.Path:
    """Save checkpoint then write .done marker to trigger the sidecar upload.

    Protocol (Section 3 - Checkpoint Synchronisation Loop):
        model_epoch_03.pt        <- weights (~GBs in real training)
        model_epoch_03.pt.done   <- 0-byte inotify trigger for sidecar
    """
    ckpt_path = CHECKPOINT_DIR / f"model_epoch_{epoch:03d}.pt"
    done_path = pathlib.Path(str(ckpt_path) + ".done")

    t0 = time.perf_counter()
    torch.save({"epoch": epoch, "model_state_dict": model.state_dict(), "loss": loss}, ckpt_path)
    elapsed_ms = (time.perf_counter() - t0) * 1000

    size_mb = ckpt_path.stat().st_size / 1024 / 1024
    digest  = _sha256(ckpt_path)
    log.info("checkpoint saved epoch=%d size=%.1fMiB sha256=%.16s... elapsed=%.0fms",
             epoch, size_mb, digest, elapsed_ms)

    # Write .done AFTER checkpoint is fully flushed to disk.
    done_path.write_bytes(b"")
    log.info(".done marker written path=%s", done_path)
    return ckpt_path


def _sha256(path: pathlib.Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def update_shared_state(epoch: int, rank: int, loss: float) -> None:
    """Write per-worker heartbeat to the NFS shared state volume."""
    state_file = SHARED_DIR / f"worker_{rank}_state.json"
    state_file.write_text(
        f'{{"epoch":{epoch},"rank":{rank},"loss":{loss:.6f},"ts":{time.time():.3f}}}\n'
    )


def train() -> None:
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    log.info("device=%s epochs=%d batch_size=%d", device, NUM_EPOCHS, BATCH_SIZE)

    dataset = HotLoadDataset(DATASET_PATH / "imagenet")
    loader  = DataLoader(dataset, batch_size=BATCH_SIZE, num_workers=4, pin_memory=True)

    model     = SimpleVisionModel().to(device)
    criterion = nn.CrossEntropyLoss()
    optimizer = torch.optim.AdamW(model.parameters(), lr=3e-4)
    rank      = int(os.getenv("RANK", "0"))

    for epoch in range(1, NUM_EPOCHS + 1):
        model.train()
        epoch_loss  = 0.0
        epoch_start = time.perf_counter()

        for step, (images, labels) in enumerate(loader):
            images = images.to(device, non_blocking=True)
            labels = (
                labels.to(device, non_blocking=True)
                if isinstance(labels, torch.Tensor)
                else torch.tensor(labels, device=device)
            )
            optimizer.zero_grad(set_to_none=True)
            loss = criterion(model(images), labels)
            loss.backward()
            optimizer.step()
            epoch_loss += loss.item()

            if step % 50 == 0:
                log.info("  epoch=%d step=%d loss=%.4f", epoch, step, loss.item())

        avg_loss = epoch_loss / max(len(loader), 1)
        log.info("epoch=%d avg_loss=%.4f elapsed=%.1fs",
                 epoch, avg_loss, time.perf_counter() - epoch_start)

        update_shared_state(epoch, rank, avg_loss)
        save_checkpoint(model, epoch, avg_loss)

    log.info("training finished")


if __name__ == "__main__":
    train()
