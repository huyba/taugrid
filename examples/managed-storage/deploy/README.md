# Managed Storage Demo — AKS Deployment & Test Results

Self-contained demo validating the 3-tier storage architecture on AKS
without GPU or custom Docker images.

---

## What This Demo Actually Tests (Real vs Mock)

| Component | Status | Detail |
|---|---|---|
| **Azure Premium SSD PVC** (checkpoints) | ✅ **Real** | `managed-csi-premium`, RWO, auto-provisioned in AKS node RG |
| **Azure Files Premium PVC** (shared state) | ✅ **Real** | `azurefile-csi-premium`, RWX, 100Gi, auto-provisioned |
| **Init container → dataset volume** | ✅ **Real flow** | Init container runs, populates `emptyDir` before training starts |
| **Dataset content** | ⚠️ **Mocked** | `busybox dd /dev/urandom` — not a real Azure Blob download |
| **Checkpoint write to PVC** | ✅ **Real** | 524 KB `.pt` file written to Azure Premium SSD each epoch |
| **`.done` marker protocol** | ✅ **Real** | Training writes 0-byte marker; sidecar detects and evicts |
| **Sidecar upload to Azure Blob** | ⚠️ **Mocked** | `busybox rm` simulates eviction; no real SDK upload |
| **New Azure Storage Account** | ❌ **Not created** | Uses AKS-managed accounts only |
| **GPU** | ❌ **Not used** | Synthetic CPU tensors (cluster has no GPU node) |
| **Multi-worker shared state** | ⚠️ **Partial** | PVC bound RWX but only 1 worker in demo |

---

## Actual Test Run Results

Cluster: `helmdeep-aks` (AKS 1.35, `westus2`, 1× `Standard_D` system node)
Date: 2026-10-12

### Storage provisioned

```
NAME                STATUS   CAPACITY   ACCESS MODES   STORAGECLASS            AGE
demo-checkpoints    Bound    5Gi        RWO            managed-csi-premium     verified
demo-shared-state   Bound    100Gi      RWX            azurefile-csi-premium   verified
```

### Init container log

```
Creating synthetic dataset files...
Init complete — 10 files created
```

Training container confirmed visibility: `Dataset files in /data/datasets: 11`

### Training log (5 epochs, CPU only)

```
2026-10-12 05:45:44  Dataset files in /data/datasets: 11
2026-10-12 05:45:44  TRAIN  epoch=1/5  loss=1.0000  elapsed=0.00s
2026-10-12 05:45:44  CHECKPOINT  epoch=1  size=524292 bytes  sha256=0d1f1d0ddeba55bf...
2026-10-12 05:45:44  .done marker written  path=/data/checkpoints/model_epoch_001.pt.done
2026-10-12 05:45:45  TRAIN  epoch=2/5  loss=0.5000  elapsed=0.00s
2026-10-12 05:45:45  CHECKPOINT  epoch=2  size=524292 bytes  sha256=678f715ed64c9835...
2026-10-12 05:45:45  .done marker written  path=/data/checkpoints/model_epoch_002.pt.done
2026-10-12 05:45:46  TRAIN  epoch=3/5  loss=0.3333  elapsed=0.01s
...
2026-10-12 05:45:49  DONE — all 5 epochs complete
```

Total: 5 epochs × 1s/epoch, 5 checkpoints × 524 KB on Azure Premium SSD.

### Checkpoint sidecar log

```
Checkpoint sidecar started — watching /data/checkpoints for .done markers
[SIDECAR] Detected .done: /data/checkpoints/model_epoch_003.pt.done
[SIDECAR] Checkpoint: /data/checkpoints/model_epoch_003.pt  size=524292 bytes
[SIDECAR] Simulating Azure Blob upload (mock)...
[SIDECAR] Upload complete — removing local copy
[SIDECAR] Total uploaded: 1
... (5 checkpoints detected and evicted)
```

All 5 `.done` markers were detected. Local checkpoint files were evicted after
each "upload". The `.done` → detect → evict round-trip completed in < 2 s.

> **Known issue:** sidecar detected > 5 files across repeated test runs because
> the PVC persists between pod deletions and retains stale `.done` files.
> Production `tau-sidecar` avoids this with SHA-256 deduplication and the
> O_EXCL lock file — a file already processed cannot be re-locked.

---

## What Is Missing for Production

To replace each mocked component with the real implementation:

### 1. Real dataset download (init container)

Replace the `busybox` init container with `ghcr.io/azure/taugrid/tau-init`:

```yaml
initContainers:
  - name: init-dataset-imagenet
    image: ghcr.io/azure/taugrid/tau-init:latest   # real tau-init binary
    env:
      - name: STORAGE_ACCOUNT
        value: "myaccount"
      - name: CONTAINER
        value: "datasets"
      - name: BLOB_PREFIX
        value: "imagenet/"
      - name: DEST_DIR
        value: /data/datasets/imagenet
      - name: AZURE_STORAGE_KEY
        valueFrom:
          secretKeyRef:
            name: azure-blob-creds
            key: storageAccountKey
```

tau-init downloads in parallel (up to 16 workers), verifies SHA-256 per file,
and skips already-present files on pod restart.

### 2. Real checkpoint upload (sidecar)

Replace the `busybox` sidecar with `ghcr.io/azure/taugrid/tau-sidecar`:

```yaml
- name: checkpoint-sidecar
  image: ghcr.io/azure/taugrid/tau-sidecar:latest  # real tau-sidecar binary
  env:
    - name: WATCH_DIR
      value: /data/checkpoints
    - name: STORAGE_ACCOUNT
      value: "myaccount"
    - name: CONTAINER
      value: "checkpoints"
    - name: MAX_RETRY_ATTEMPTS
      value: "10"
    - name: AZURE_STORAGE_KEY
      valueFrom:
        secretKeyRef:
          name: azure-blob-creds
          key: storageAccountKey
```

tau-sidecar uses fsnotify (inotify), O_EXCL lock, size+hash verification, and
exponential backoff retry (2s→5m) before local eviction.

### 3. Azure Blob Storage account

```bash
az storage account create \
  --name mytaugriddata \
  --resource-group helmdeep-rg \
  --location westus2 \
  --sku Standard_LRS \
  --kind StorageV2

az storage container create --name datasets      --account-name mytaugriddata
az storage container create --name checkpoints   --account-name mytaugriddata

kubectl create secret generic azure-blob-creds \
  --from-literal=storageAccountKey=$(az storage account keys list \
    --account-name mytaugriddata --query '[0].value' -o tsv) \
  -n tau-storage-test
```

### 4. GPU node pool

The real training workflow requires a GPU node pool. Add one via the existing
Terraform in `terraform/aks/` or:

```bash
az aks nodepool add \
  --resource-group helmdeep-rg \
  --cluster-name helmdeep-aks \
  --name gpunvme \
  --node-vm-size Standard_NC24ads_A100_v4 \
  --node-count 1 \
  --labels kubernetes.azure.com/agentpool=gpunvme
```

---

## Quick Deploy

```bash
# Install CRD
kubectl apply -f ../../controllers/tau-core/config/crd/bases/tau.azure.com_datafetches.yaml

# Run demo
kubectl apply -f test-demo.yaml

# Stream logs
kubectl logs -f tau-storage-demo -n tau-storage-test -c init-dataset-imagenet-demo
kubectl logs -f tau-storage-demo -n tau-storage-test -c training
kubectl logs -f tau-storage-demo -n tau-storage-test -c checkpoint-watch

# Cleanup
kubectl delete -f test-demo.yaml
kubectl delete pvc demo-checkpoints demo-shared-state -n tau-storage-test
```
