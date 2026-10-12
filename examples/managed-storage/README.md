# Managed Storage Layer — Examples

> Implements [Feature #277](https://github.com/Azure/taugrid/issues/277) · Full design doc: [`docs/design/managed-storage-layer.md`](../../docs/design/managed-storage-layer.md)

This directory contains everything needed to configure and test the TauGrid
Managed Storage Layer: CRD configs, Kubernetes manifests, a PyTorch training
script, and a self-contained AKS demo.

---

## What is the Managed Storage Layer?

TauGrid's storage layer decouples AI training workloads from underlying Azure
infrastructure by automatically classifying data into three tiers and wiring
the right storage backend to each tier — without researcher intervention.

```
Researcher writes tau.yaml
        │
        ▼
┌───────────────────────────────────────────────────────────────┐
│  DataFetch CR                                             │
│  ┌─────────────────┐  ┌───────────────────┐  ┌────────────┐  │
│  │ datasets:       │  │ checkpoints:      │  │sharedState:│  │
│  │  HotLoad / ▼    │  │  localPath: /ckpt │  │  type: NFS │  │
│  │  StreamMount    │  │  syncStrategy:    │  │  RWX 100Gi │  │
│  └────────┬────────┘  │    Asynchronous   │  └─────┬──────┘  │
│           │           └────────┬──────────┘        │         │
└───────────┼────────────────────┼───────────────────┼─────────┘
            │                    │                   │
            ▼                    ▼                   ▼
    ┌──────────────┐    ┌──────────────────┐  ┌─────────────────┐
    │ emptyDir     │    │ Azure Premium    │  │ Azure Files     │
    │ (NVMe cache) │    │ SSD PVC (RWO)    │  │ Premium NFS PVC │
    │ or BlobFuse2 │    │ + async sidecar  │  │ (RWX, NFS 4.1) │
    └──────────────┘    └──────────────────┘  └─────────────────┘
```

---

## Storage Tier Summary

| Tier | Strategy | Latency | Access | Azure Backend |
|---|---|---|---|---|
| **Datasets** | HotLoad (init container) | Disk I/O, zero HTTP | ReadOnly | Azure Blob → NVMe emptyDir |
| **Datasets** | StreamMount (fallback) | HTTP per read | ReadOnly | BlobFuse2 CSI |
| **Checkpoints** | Asynchronous (sidecar) | Non-blocking | RWO | Premium SSD + Blob |
| **Shared State** | NFS v4.1 | ~100µs | RWX | Azure Files Premium |

---

## Pod Injection Flow

When a pod has the annotation `tau.azure.com/data-fetch: <name>`, the
mutating webhook automatically injects all storage containers:

```
Pod spec (before admission)          Pod spec (after injection)
────────────────────────────         ──────────────────────────────────────
spec:                                spec:
  containers:                          initContainers:
    - name: training                     - name: init-dataset-imagenet    ← injected
      image: pytorch/pytorch               image: tau-init
                                           (streams blob → NVMe emptyDir)

                                       containers:
                                         - name: training                 (original)
                                           volumeMounts:
                                             - /data/datasets/imagenet    ← added
                                             - /checkpoints               ← added
                                             - /shared                    ← added

                                         - name: checkpoint-sidecar       ← injected
                                           image: tau-sidecar
                                           (inotify .done → Blob upload)

                                       volumes:
                                         - dataset-imagenet-nvme (emptyDir)
                                         - checkpoint-staging (PVC)
                                         - shared-state-nfs (PVC)
```

---

## Checkpoint Sync Protocol

The sidecar and training container communicate via marker files — no IPC, no
shared memory, completely decoupled:

```
Training container              Checkpoint sidecar
──────────────────              ──────────────────

torch.save("model_v3.pt")

open("model_v3.pt.done","w")  ──inotify CREATE──►  detect model_v3.pt.done
.close()                                            │
                                                    ▼
Training epoch N+1 starts      acquire O_EXCL lock (model_v3.pt.uploading)
(GPU never waits)                                   │
                                                    ▼
                               UploadFile() → Azure Blob
                               [retry: exp backoff 2s→5m, 10 attempts]
                                                    │
                                                    ▼
                               GetProperties() → verify size + hash
                                                    │
                               ┌────────────────────┘
                               │ size match? hash match?
                               ├─ NO  → retry, keep local file
                               └─ YES → rm .done + .pt + lock
                                        pruneOldCheckpoints(keep=2)
```

---

## NVMe Capacity Fallback

If the total `sizeGB` of all `HotLoad` datasets exceeds `nvmeCapacityGiB`,
the controller automatically degrades to `StreamMount` (BlobFuse2):

```
spec:
  nvmeCapacityGiB: 3000     ← node NVMe capacity

  datasets:
    - sizeGB: 2000           ─┐ total = 3500 GiB
    - sizeGB: 1500           ─┘ > 3000 GiB

status:
  datasets:
    - effectiveLoad: StreamMount  ← auto-downgraded
      message: "HotLoad downgraded: 3500 GiB exceeds 3000 GiB node NVMe"
```

No researcher action needed. Logged in `kubectl get di -o wide`.

---

## Files in This Directory

```
examples/managed-storage/
├── README.md                    ← this file
├── tau.yaml                     ← researcher-facing config (tau run --config)
├── pvc-azure-files-nfs.yaml     ← StorageClass + PVC manifests
├── train.py                     ← full PyTorch training script with .done protocol
├── pod-validation.yaml          ← fully-expanded pod spec (post-webhook injection)
└── deploy/
    ├── demo_train.py            ← stdlib-only demo (no GPU, no torch)
    └── test-demo.yaml           ← all-in-one AKS test manifest
```

---

## Quick Deploy (AKS)

```bash
# 1. Install CRD
kubectl apply -f ../../controllers/tau-core/config/crd/bases/tau.azure.com_datafetches.yaml

# 2. Deploy demo (no GPU, no custom Docker image required)
kubectl apply -f deploy/test-demo.yaml

# 3. Watch training + sidecar logs
kubectl logs -f tau-storage-demo -n tau-storage-test -c training
kubectl logs -f tau-storage-demo -n tau-storage-test -c checkpoint-watch
```

For full deployment instructions see [`docs/design/managed-storage-layer.md`](../../docs/design/managed-storage-layer.md).
