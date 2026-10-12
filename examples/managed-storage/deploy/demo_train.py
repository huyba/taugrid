"""
TauGrid Managed Storage Demo — stdlib only, no GPU required.
Simulates the checkpoint .done marker protocol for e2e validation.
"""
import os, time, hashlib, pathlib, struct, random, logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s")
log = logging.getLogger("demo")

DATASET_PATH   = pathlib.Path(os.getenv("DATASET_PATH",   "/data/datasets"))
CHECKPOINT_DIR = pathlib.Path(os.getenv("CHECKPOINT_DIR", "/data/checkpoints"))
SHARED_DIR     = pathlib.Path(os.getenv("SHARED_DIR",     "/data/shared"))
NUM_EPOCHS     = int(os.getenv("NUM_EPOCHS", "5"))

CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)
SHARED_DIR.mkdir(parents=True, exist_ok=True)

def fake_train_epoch(epoch):
    """Simulate a training step by burning ~1s CPU."""
    total = 0.0
    for _ in range(50_000):
        total += random.random()
    loss = 1.0 / (epoch + total * 1e-10)
    return loss

def save_checkpoint(epoch, loss):
    ckpt = CHECKPOINT_DIR / f"model_epoch_{epoch:03d}.pt"
    done = pathlib.Path(str(ckpt) + ".done")

    # Write a fake 512KB checkpoint binary
    data = struct.pack(">I", epoch) + bytes(random.getrandbits(8) for _ in range(512 * 1024))
    ckpt.write_bytes(data)

    digest = hashlib.sha256(data).hexdigest()
    log.info("CHECKPOINT  epoch=%d  size=%d bytes  sha256=%.16s...", epoch, len(data), digest)

    # Signal sidecar: write 0-byte .done marker AFTER flush
    done.write_bytes(b"")
    log.info(".done marker written → %s", done)

def update_shared(epoch, loss):
    (SHARED_DIR / f"worker_0_epoch_{epoch:03d}.json").write_text(
        f'{{"epoch":{epoch},"loss":{loss:.6f},"ts":{time.time():.3f}}}\n'
    )

# Show dataset files (will be empty in demo, but path is wired)
files = list(DATASET_PATH.rglob("*")) if DATASET_PATH.exists() else []
log.info("Dataset files in %s: %d", DATASET_PATH, len(files))

for epoch in range(1, NUM_EPOCHS + 1):
    t0 = time.perf_counter()
    loss = fake_train_epoch(epoch)
    elapsed = time.perf_counter() - t0
    log.info("TRAIN  epoch=%d/%d  loss=%.4f  elapsed=%.2fs", epoch, NUM_EPOCHS, loss, elapsed)
    update_shared(epoch, loss)
    save_checkpoint(epoch, loss)
    time.sleep(1)

log.info("DONE — all %d epochs complete", NUM_EPOCHS)
