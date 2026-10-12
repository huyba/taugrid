# Managed Storage Layer for TauGrid

> **Status:** `v1alpha1` · Implements [Feature #277](https://github.com/Azure/taugrid/issues/277)

This document describes the architecture, components, and operational guide for
the TauGrid Managed Storage Layer — a Kubernetes-native, event-driven system
that eliminates I/O bottlenecks in large-scale AI training workloads running
on AKS with Kueue and KubeRay.

---

## Table of Contents

1. [Problem Statement](#1-problem-statement)
2. [Architecture Overview](#2-architecture-overview)
3. [Component Deep Dive](#3-component-deep-dive)
4. [Storage Tier Classification](#4-storage-tier-classification)
5. [Runtime Workflow](#5-runtime-workflow)
6. [Failure Mode Handling](#6-failure-mode-handling)
7. [Quick Start](#7-quick-start)
8. [File Reference](#8-file-reference)

---

## 1. Problem Statement & Solution Paradigm

### Solution Paradigm

The Managed Storage Layer decouples infrastructure complexity from the researcher's
workflow. Instead of manually provisioning storage, writing sidecar containers, and
configuring Azure Blob uploads, the researcher submits a single `tau.yaml` file:

```
Researcher writes tau.yaml
        │
        ▼ (kubectl apply via tau run)
DataIngestion CR
        │
        ▼
Tau-Storage-Controller automatically:
  ├── Provisions the right PVCs (Premium SSD for checkpoints, Azure Files NFS for shared state)
  ├── Detects NVMe capacity and downgrades HotLoad → StreamMount if needed
  └── Signals StoragePodInjector to inject init containers + sidecar at admission time

Training Pod gets:
  ├── /data/datasets/*  — pre-loaded from Azure Blob to local NVMe (zero HTTP during training)
  ├── /data/checkpoints — async-synced to Azure Blob via inotify sidecar (GPU never waits)
  └── /data/shared      — RWX NFS mount shared across all distributed workers
```

The three storage tiers are classified automatically based on access pattern:

| Tier | Access Pattern | Strategy |
|---|---|---|
| Datasets | Read-intensive, immutable | `HotLoad` (NVMe cache) or `StreamMount` (BlobFuse2 fallback) |
| Checkpoints | Write-once, large files | Async inotify sidecar — training writes locally, sidecar uploads in background |
| Shared State | Concurrent read/write, many workers | Azure Files Premium NFS (RWX, NFS v4.1) |

---

The current TauGrid storage implementation relies heavily on static volume mappings
(`/data/`) backed directly by BlobFuse2 (Azure Blob CSI Driver). This introduces
three critical engineering pain points:

| Pain Point | Root Cause | Impact |
|---|---|---|
| **I/O Bottleneck & GPU Underutilization** | BlobFuse2 maps POSIX `read()` → synchronous HTTP REST | GPUs idle waiting for data |
| **Checkpoint Logic Contradiction** | `tau run` writes to `/data/projects/.../checkpoints/`, `tau serve` prepends `/data/checkpoints/` ([Issue #208](https://github.com/Azure/taugrid/issues/208)) | Deployment failures; BlobFuse2 too slow for multi-GB checkpoints; RWO disk breaks multi-node |
| **Shared Working State Gridlock** | Azure Blob lacks microsecond synchronisation; managed disk is RWO | Ray/PyTorch state mismatch across workers |

**1. I/O Bottleneck & GPU Underutilization**

Training modern Deep Learning models (Vision/LLMs) requires high-throughput, low-latency
data feeding. BlobFuse2 translates OS POSIX filesystem calls (`read`, `open`) into
synchronous HTTP REST API calls. Millions of small files cause high connection overhead,
starving the GPUs (GPU Idle state).

**2. The Checkpoint Logic Contradiction**

There is an active path mismatch bug between `tau run` and `tau serve`
([Issue #208](https://github.com/Azure/taugrid/issues/208)). Training writes checkpoints
to `/data/projects/<workspace>/runs/<run-name>/checkpoints/`, but `tau serve deploy`
automatically prepends `/data/checkpoints/` to deployment paths. Furthermore, storing
massive checkpoint files (tens of GBs) over BlobFuse2 is too slow, while using Azure
Managed Disk (ReadWriteOnce) breaks multi-node recoverability and scaling.

**3. Shared Working State Gridlock**

Distributed training workers (PyTorch Elastic/Ray) need a unified, low-latency,
concurrent-write storage layer (ReadWriteMany). Azure Blob Storage lacks microsecond
synchronization, causing state mismatch, whereas standard block storage cannot be shared
among hundreds of scattered compute nodes.

---

## 2. Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────┐
│                     Researcher: tau run --config tau.yaml               │
└───────────────────────────────────┬─────────────────────────────────────┘
                                    │ kubectl apply (DataIngestion CR)
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                         Kube-API-Server / etcd                          │
└───────────────────────────────────┬─────────────────────────────────────┘
                                    │ Watch: DataIngestion CRD events
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│              Tau-Storage-Controller (Go / controller-runtime)           │
│                                                                         │
│  Reconcile()                                                            │
│    ├── reconcileDatasets()   →  emptyDir volumes (HotLoad)              │
│    │                            BlobFuse2 CSI inline (StreamMount)      │
│    ├── reconcileCheckpointPVC() → Azure Premium SSD PVC (RWO, 200Gi)   │
│    └── reconcileSharedStatePVC() → Azure Files NFS PVC (RWX, 100Gi)    │
└───────────┬───────────────────────────────────┬─────────────────────────┘
            │ owns PVCs                         │ PVC provisioned
            ▼                                   ▼
┌───────────────────────┐           ┌───────────────────────────────────┐
│  StoragePodInjector   │           │    Azure Storage Infrastructure   │
│  (Mutating Webhook)   │           │                                   │
│                       │           │  ┌─────────────────────────────┐  │
│  On Pod admission:    │           │  │ Azure Blob (Datasets)       │  │
│  ├─ init containers   │           │  │ Azure Blob (Checkpoints)    │  │
│  ├─ sidecar container │           │  │ Azure Files Premium NFS     │  │
│  └─ volumes + mounts  │           │  │   (Shared State)            │  │
└───────────┬───────────┘           └───────────────────────────────────┘
            │ mutated pod spec
            ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                          Training Pod                                   │
│                                                                         │
│  ┌──────────────────┐  ┌────────────────────┐  ┌───────────────────┐   │
│  │  init-dataset-*  │  │     training       │  │ checkpoint-sidecar│   │
│  │  (tau-init)      │  │  (train.py)        │  │ (tau-sidecar)     │   │
│  │                  │  │                    │  │                   │   │
│  │ Azure Blob       │  │ /data/datasets  ◄──┤  │ inotify(.done)    │   │
│  │    ─────────►    │  │ (emptyDir NVMe) ───┘  │    ─────────────► │   │
│  │ emptyDir NVMe    │  │                    │  │ Azure Blob upload │   │
│  │ SHA-256 verify   │  │ /checkpoints ◄──►──┼──► lock + verify     │   │
│  │ exit 0           │  │ (Premium SSD PVC)  │  │ evict local copy  │   │
│  └──────────────────┘  │                    │  └───────────────────┘   │
│                         │ /shared (NFS RWX)  │                         │
│                         │ (Azure Files)      │                         │
│                         └────────────────────┘                         │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 3. Component Deep Dive

### 3.1 DataIngestion CRD

The `DataIngestion` custom resource is the single interface between the
researcher and the storage infrastructure.

```yaml
apiVersion: tau.azure.com/v1alpha1
kind: DataIngestion
metadata:
  name: my-run
spec:
  workspace: my-workspace
  datasets:
    - name: imagenet
      loadStrategy: HotLoad    # or StreamMount
      sizeGB: 150
      mountPath: /data/datasets/imagenet
      source:
        storageAccount: myaccount
        container: datasets
        prefix: imagenet/
  checkpoints:
    localPath: /data/checkpoints
    syncStrategy: Asynchronous
    retentionCount: 2
    remote:
      storageAccount: myaccount
      container: checkpoints
  sharedState:
    type: HighThroughputNFS
    mountPath: /shared
    capacity: "100Gi"
```

### 3.2 Tau-Storage-Controller

Located at [controllers/tau-core/internal/controller/storage_controller.go](../controllers/tau-core/internal/controller/storage_controller.go).

The controller reconciliation loop:

```
Reconcile(req)
  │
  ├── Get DataIngestion CR
  ├── Add finalizer (first run)
  │
  ├── reconcileDatasets()
  │     ├── Sum HotLoad SizeGB
  │     ├── IF total > NVMeCapacityGiB → downgrade to StreamMount  ← Section 5
  │     └── Record EffectiveLoad in status
  │
  ├── reconcileSharedStatePVC()
  │     └── Create RWX PVC (azurefile-premium-nfs, 100Gi) if absent
  │
  ├── reconcileCheckpointPVC()
  │     └── Create RWO PVC (managed-csi-premium, 200Gi) if absent
  │
  └── Update DataIngestion.Status (phase=Ready, PVC names)
```

**Register in main.go:**
```go
if err := (&controller.DataIngestionReconciler{
    Client:          mgr.GetClient(),
    SystemNamespace: cfg.SystemNamespace,
}).SetupWithManager(mgr); err != nil {
    setupLog.Error(err, "unable to create DataIngestion controller")
    os.Exit(1)
}
```

### 3.3 StoragePodInjector (Mutating Webhook)

Located at [controllers/tau-core/internal/controller/pod_injector.go](../controllers/tau-core/internal/controller/pod_injector.go).

When a pod with annotation `tau.azure.com/data-ingestion: <name>` is admitted,
the webhook:

1. Fetches the referenced DataIngestion CR
2. For each **HotLoad** dataset: injects an `init-dataset-*` container + emptyDir volume
3. For each **StreamMount** dataset: injects an inline BlobFuse2 CSI volume
4. If checkpoints configured: injects `checkpoint-sidecar` container + PVC mount
5. If shared state configured: mounts the NFS PVC into all app containers

**Register webhook in main.go:**
```go
mgr.GetWebhookServer().Register("/mutate-pod-storage",
    &webhook.Admission{Handler: &controller.StoragePodInjector{
        Client: mgr.GetClient(),
    }})
```

### 3.4 tau-init (Init Container Binary)

Located at [controllers/tau-init/cmd/preloader/main.go](../controllers/tau-init/cmd/preloader/main.go).

```
tau-init boot
  │
  ├── Read env: STORAGE_ACCOUNT, CONTAINER, BLOB_PREFIX, DEST_DIR
  ├── Authenticate (workload identity or storage key)
  ├── List blobs under prefix (paginated)
  │
  └── downloadAll() [parallel, NumCPU×2 workers, max 16]
        └── For each blob:
              ├── Skip if file exists and non-empty (incremental)
              ├── DownloadStream → tmpfile + SHA-256 hash
              ├── Rename tmp → final (atomic)
              └── Log throughput (MiB/s)
  
  exit 0  →  training container starts with data on local disk
```

### 3.5 tau-sidecar (Checkpoint Sync Engine)

Located at [controllers/tau-sidecar/cmd/sidecar/main.go](../controllers/tau-sidecar/cmd/sidecar/main.go).

```
tau-sidecar boot
  │
  ├── Watch WATCH_DIR via fsnotify (inotify on Linux)
  │
  └── Event loop (up to 4 concurrent upload goroutines):
        On CREATE/WRITE of *.done:
          │
          ├── Derive checkpoint path: "model.pt.done" → "model.pt"
          ├── Verify checkpoint exists (skip stale markers)
          │
          └── handleCheckpoint() [goroutine]:
                ├── Acquire O_EXCL lock file (model.pt.uploading)
                ├── SHA-256 local file
                │
                └── Retry loop (max 10 × exponential backoff 2s→5m):
                      ├── UploadFile → Azure Blob (64MiB blocks, 8 parallel)
                      ├── GetProperties → verify remote size == local size
                      │
                      ├── [FAIL] → wait backoff, retry
                      └── [PASS] → remove .done + .pt + lock
                                   pruneOldCheckpoints() [keep last N]
```

---

## 4. Storage Tier Classification

| Tier | Workload | K8s Access | Azure Backend | Mechanism |
|---|---|---|---|---|
| **Datasets** | Raw training inputs | ReadOnly / Multi-node | Azure Blob | HotLoad (init container NVMe) or StreamMount (BlobFuse2 CSI) |
| **Checkpoints** | Periodic model weights | RWO (local) → RWM (cloud) | Azure Premium SSD + Blob | inotify .done markers + async sidecar |
| **Shared State** | Ray/PyTorch coordination | RWX | Azure Files Premium NFS 4.1 | NFS v4.1 nconnect=4, rsize/wsize=1MiB |

### Shared Working State — Use Cases and Design Fit

The `sharedState` tier provisions a single Azure Files Premium RWX PVC mounted
at `/data/shared` across every worker pod in the job. Because the webhook injects
the same volume definition from the same `DataIngestion` CR into every pod, all
workers automatically share the same mount path with no per-worker configuration.

The following patterns cover the typical coordination needs of distributed training:

#### 1. Rendezvous / Barrier Sync

Each worker writes its own ready marker — one file per worker, one writer per file,
so there is no contention:

```
/data/shared/ready/
├── worker-0.ready
├── worker-1.ready
└── worker-2.ready
```

Rank 0 polls until all N files exist, then writes `go.signal`. Workers unblock and
start the epoch together. No distributed locking is required because each file has
exactly one writer.

#### 2. Heartbeat / Fault Detection

Each worker updates its own file on a timer (e.g. every 30 seconds):

```
/data/shared/heartbeat/
├── worker-0    # contains last-updated Unix timestamp
├── worker-1
└── worker-2
```

Rank 0 reads all heartbeat files. A stale timestamp (> N seconds old) signals that
the worker has died. Rank 0 can then trigger a restart, re-shard work, or alert the
researcher. The `tau-sidecar` binary already runs in every pod and can be extended
to write a heartbeat on a ticker goroutine alongside its checkpoint-watching loop.

#### 3. Global Step / Epoch Counter

Rank 0 is the single writer. All other workers read:

```
/data/shared/global_step.txt    # "epoch=42 step=15340 loss=0.31"
```

Azure Files Premium with `nconnect=4` and `rsize=1MiB` makes these small reads
fast. Workers read this once per epoch rather than computing step counts
independently, which prevents drift in elastic training scenarios.

#### 4. Best Checkpoint Pointer — Resolves Issue #208

This is the most operationally significant pattern. Instead of hardcoding checkpoint
paths in both `tau run` and `tau serve`, rank 0 writes a pointer file:

```
/data/shared/best_model.txt
# epoch=42  loss=0.31  blob=runs/my-training-run/model_042.pt
```

`tau serve` reads this file to discover which checkpoint to pull from Azure Blob.
Both `run` and `serve` mount the same NFS volume, so the path is always authoritative.
The path mismatch between `tau run` (`/data/projects/<ws>/runs/<run>/checkpoints/`)
and `tau serve` (`/data/checkpoints/`) disappears — not because a hardcoded string
was fixed, but because the path is now **data, not config**.

#### 5. Elastic Membership (torchrun / PyTorch Elastic)

Each worker writes its rank, world size, and pod IP at startup:

```
/data/shared/membership/
├── worker-0.json    # {"rank":0,"world_size":4,"addr":"10.0.1.5:29500"}
├── worker-1.json
└── worker-2.json
```

Workers discover the coordinator address by reading `worker-0.json`. This replaces
the need for a separate Redis or etcd per training run — useful on AKS where
provisioning extra infrastructure per job adds cost and latency.

#### 6. Config Broadcast

Rank 0 writes a generated config once at startup:

```
/data/shared/config.json    # hyperparameters, dataset shards, run metadata
```

Because the webhook injects the NFS mount before any container starts (init
containers run first, then the training container), `/data/shared` is already
mounted and readable by the time training code runs. No race between config write
and config read.

#### Summary

| Use Case | Writer | Readers | Contention Risk |
|---|---|---|---|
| Rendezvous | Each worker (own file) | Rank 0 | None — one writer per file |
| Heartbeat | Each worker (own file) | Rank 0 | None — one writer per file |
| Global step | Rank 0 only | All workers | None — single writer |
| Best checkpoint pointer | Rank 0 only | `tau serve`, all workers | None — single writer |
| Elastic membership | Each worker (own file) | All workers | None — one writer per file |
| Config broadcast | Rank 0 only | All workers | None — written once before readers start |

The consistent pattern is: **one file per writer, rank 0 as the only global
coordinator**. NFS advisory locking (`fcntl`) over Azure Files is unreliable and
unnecessary when this convention is followed.

### Azure Files NFS StorageClass

```yaml
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: azurefile-premium-nfs
provisioner: file.csi.azure.com
parameters:
  skuName: Premium_LRS
  protocol: nfs
  networkEndpointType: privateEndpoint
mountOptions:
  - nconnect=4        # 4 parallel TCP connections
  - rsize=1048576     # 1 MiB read block
  - wsize=1048576     # 1 MiB write block
  - hard
  - noresvport
  - nodiratime
```

---

## 5. Runtime Workflow

### Phase 1: Pod Initialization

```
Kubelet schedules Pod
        │
        ▼
┌───────────────────────────────────────────────┐
│  init-dataset-imagenet (tau-init)             │
│                                               │
│  1. List blobs under imagenet-1k-tokenized/   │
│  2. Parallel download (16 workers, NVMe)      │
│  3. SHA-256 per file → verify                 │
│  4. exit 0                                    │
└───────────────────────────────────────────────┘
        │ (sequential)
        ▼
┌───────────────────────────────────────────────┐
│  init-dataset-laion (tau-init)                │
│  [same as above for laion dataset]            │
└───────────────────────────────────────────────┘
        │ (all inits complete)
        ▼
┌──────────────────────┐  ┌────────────────────┐
│  training container  │  │ checkpoint-sidecar │
│  starts              │  │ starts (concurrent)│
└──────────────────────┘  └────────────────────┘
```

### Phase 2: Training + Checkpoint Sync Loop

```
training container                  checkpoint-sidecar
─────────────────                   ──────────────────
epoch N done
  ↓
torch.save("model_N.pt")
  ↓
open("model_N.pt.done","w").close()  ── inotify CREATE ──►  detect "model_N.pt.done"
                                                              ↓
                                                         lock "model_N.pt.uploading"
                                                              ↓
                                                         uploadBlob("model_N.pt")
                                                         [retry loop with backoff]
                                                              ↓
                                                         verify size + hash
                                                              ↓
                                                         rm .done + .pt + lock
                                                              ↓
                                                         pruneOldCheckpoints(keep=2)
epoch N+1 starts
(GPU never waits)
```

---

## 6. Failure Mode Handling

### 6.1 NVMe Capacity Exhaustion (Section 5)

**Scenario:** A researcher requests `loadStrategy: HotLoad` for 4TB of datasets
but the target node pool has only 3TB NVMe per node.

**Detection:** `reconcileDatasets()` sums all HotLoad `sizeGB` fields and
compares against `spec.nvmeCapacityGiB` (default: 3000 GiB).

**Automatic Fallback:**

```
spec.nvmeCapacityGiB = 3000

Dataset A: sizeGB=2000, HotLoad  ─┐
Dataset B: sizeGB=1500, HotLoad  ─┤ total = 3500 GiB > 3000 GiB
                                   ↓
Controller detects overflow
  ↓
status.datasets[A].effectiveLoad = StreamMount  ← downgraded
status.datasets[B].effectiveLoad = StreamMount  ← downgraded
status.datasets[A].message = "HotLoad downgraded to StreamMount: ..."
  ↓
Pod injector reads status.effectiveLoad
  ↓
Injects inline BlobFuse2 CSI volume instead of emptyDir init container
```

No researcher intervention required. The degradation is logged and surfaced in
`kubectl get di -o wide`.

### 6.2 Network Partition During Checkpoint Upload (Section 5)

**Scenario:** The sidecar is mid-upload of a 50GB checkpoint when the AKS node
loses connectivity to Azure Blob for several minutes.

**Recovery protocol:**

```
Step 1: O_EXCL Lock
  model_epoch_07.pt.uploading created atomically.
  A pod restart or concurrent goroutine will see the lock and skip.

Step 2: Upload with retry
  attempt 1: fail → wait 2s
  attempt 2: fail → wait 4s
  attempt 3: fail → wait 8s
  ...
  attempt N: success → continue

Step 3: Pre-eviction integrity check
  remote_size = GetProperties(blob).ContentLength
  IF remote_size != local_size → retry (do NOT evict)
  IF hash mismatch             → retry (do NOT evict)

Step 4: Eviction (only after confirmed upload)
  os.Remove(".done")
  os.Remove("model_epoch_07.pt")
  os.Remove(".uploading")

Result: checkpoint is NEVER lost. Worst case it remains on local disk
        until the retry limit (10 × ~5m = ~50 min window) is exhausted.
```

If `MAX_RETRY_ATTEMPTS` is exhausted the sidecar logs a permanent error but
**does not remove** the local checkpoint file. The file remains on the Premium
SSD PVC and will be retried on the next pod start.

---

## 7. Quick Start

### Prerequisites

```bash
# Apply StorageClass
kubectl apply -f examples/managed-storage/pvc-azure-files-nfs.yaml

# Create blob credential secret
kubectl create secret generic azure-blob-creds \
    --from-literal=storageAccountKey=<YOUR_KEY> \
    -n research-team-a

# Install CRD
kubectl apply -f controllers/tau-core/config/crd/bases/tau.azure.com_dataingestions.yaml
```

### Deploy DataIngestion

```bash
kubectl apply -f controllers/tau-core/config/samples/tau.azure.com_v1alpha1_dataingestion.yaml
kubectl get di -n research-team-a -w
```

### Run Validation Pod

```bash
# Create training script ConfigMap
kubectl create configmap tau-training-script \
    --from-file=train.py=examples/managed-storage/train.py \
    -n research-team-a

# Deploy pod
kubectl apply -f examples/managed-storage/pod-validation.yaml

# Follow logs
kubectl logs -f tau-managed-storage-e2e -c training -n research-team-a
kubectl logs -f tau-managed-storage-e2e -c checkpoint-sidecar -n research-team-a
```

### tau CLI (Researcher UX)

```bash
tau run --config examples/managed-storage/tau.yaml
```

---

## 8. File Reference

```
controllers/
├── tau-core/
│   ├── api/v1alpha1/
│   │   ├── storage_types.go          # DataIngestion CRD Go types
│   │   ├── storage_register.go       # Scheme registration
│   │   └── storage_zz_deepcopy.go   # DeepCopy implementations
│   ├── internal/controller/
│   │   ├── storage_controller.go     # Reconciler: PVC provisioning + NVMe fallback
│   │   └── pod_injector.go          # Mutating webhook: init + sidecar injection
│   └── config/
│       ├── crd/bases/
│       │   └── tau.azure.com_dataingestions.yaml  # CRD manifest
│       └── samples/
│           └── tau.azure.com_v1alpha1_dataingestion.yaml  # Sample CR
├── tau-sidecar/
│   ├── go.mod
│   └── cmd/sidecar/main.go          # inotify watcher + async upload + retry
└── tau-init/
    ├── go.mod
    └── cmd/preloader/main.go        # Concurrent blob downloader + SHA-256 verify

examples/managed-storage/
├── tau.yaml                          # Researcher-facing config (tau run --config)
├── pvc-azure-files-nfs.yaml         # StorageClass + PVC manifests
├── train.py                          # PyTorch e2e validation training script
└── pod-validation.yaml              # Fully-expanded pod spec (post-injection)

docs/design/
└── managed-storage-layer.md         # This document
```
