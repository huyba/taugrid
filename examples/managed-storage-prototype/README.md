# Managed storage prototype

Runnable prototype for [docs/design/managed-storage.md](../../docs/design/managed-storage.md),
section 11. It proves the **runtime storage contract**: hot tier, durable tier,
commit markers and verified resume. It does so with a real PyTorch training job,
against two durable backends:

- `posix`: a mounted directory standing in for `/data` (BlobFuse2, Lustre, NFS).
- `blob`: the Azure Blob API, served locally by [Azurite](https://github.com/Azure/Azurite).

It does not exercise the controller, CRDs, BlobFuse2 or AKS. Those are phase 2
in the design doc.

| File | What it is |
|---|---|
| `tau_storage.py` | Candidate `tau.storage` SDK module: stores, dataset publish/stage, `CheckpointManager` |
| `prepare_data.py` | CPU data-prep run that publishes an immutable byte-level dataset |
| `train.py` | Small decoder-only transformer; single process or `torchrun` DDP |
| `run_scenarios.py` | Fault-injection scenarios S1–S7 with assertions; writes `results.json` |
| `results.json` | Output of the last full run |

```bash
pip install torch numpy azure-storage-blob
npm install -g azurite && azurite-blob --inMemoryPersistence &
python run_scenarios.py --backend posix --backend blob
```

Faults are injected with `TAU_PROTO_CRASH=<point>:<step>`. The points are
`after_upload_before_marker` and `after_marker`. The process exits through
`os._exit(137)`, so no cleanup code runs.
