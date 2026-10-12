# Capacity-Aware Kueue Admission — examples and test results

Everything here was run on a real AKS cluster on 2026-10-12. It compares
**before** (quota-only admission) with **after** (admission gated by a
ProvisioningRequest AdmissionCheck) across four scenarios.

| Document | Contents |
|---|---|
| This page | The before/after test matrix: what each case does and what happened |
| [`cpu-demo/README.md`](cpu-demo/README.md) | Step-by-step log of the first manual run: setup commands, manifests, timeline, object dumps, cleanup |
| [`tests/e2e/provisioning/`](../../../tests/e2e/provisioning/) | The test code and queue fixtures |

## Summary

All eight tests pass. What they show:

1. **Success works.** With the gate, Kueue held the Job suspended until the AKS
   Cluster Autoscaler scaled the pool from zero and reported `Provisioned=True`.
2. **The gate does not release quota when capacity never arrives.** The AKS
   autoscaler does not fail an unsatisfiable request; it reports
   `CapacityIsNotFound` and keeps retrying. Kueue keeps the quota reserved and a
   second job stays starved, exactly as in quota-only mode. In a 21-minute watch
   the request never failed. The design doc previously claimed quota is released
   immediately; that is not what AKS does.
3. **What the gate does change in that case** is what is left lying around: the
   Job stays suspended, no pod is created, and the reason is visible on the
   ProvisioningRequest instead of on an `Unschedulable` pod.
4. **Cancellation and cleanup are clean in both modes.** Deleting the Job removes
   the Workload, pods and ProvisioningRequest and frees the quota.

## Environment

| | |
|---|---|
| Cluster | `helmdeep-aks` (`helmdeep-rg`), Kubernetes 1.35.7 |
| Autoscaled pool | `prdemo`, `Standard_B2pls_v2` (arm64, 1.9 CPU allocatable), min 0 / max 1, label and taint `demo=provisioning` |
| Kueue | upstream v0.20.1 (TauGrid pins the MCR build v0.19.2, not tested) |
| Extra setup | CRD `provisioningrequests.autoscaling.x-k8s.io` installed by hand; AKS does not ship it |
| Provisioning class | `best-effort-atomic-scale-up.autoscaling.x-k8s.io` |

Both queues use the same pool and a 1000 CPU quota. The only difference is the
AdmissionCheck on the "after" ClusterQueue
([`before-resources.yaml`](../../../tests/e2e/provisioning/fixtures/before-resources.yaml),
[`after-resources.yaml`](../../../tests/e2e/provisioning/fixtures/after-resources.yaml)).

## How to run

```bash
cd tests/e2e
AI_RUNTIME_E2E=1 E2E_PROVISIONING=1 \
E2E_PROVISIONING_NODE_SELECTOR='demo=provisioning' \
go test -v -timeout 40m ./provisioning/
```

Output of the full run (20:17:54–20:20:16 UTC, pool already at one node):

```text
--- PASS: TestBeforeSuccess (6.41s)
--- PASS: TestAfterSuccess (6.63s)
--- PASS: TestBeforeTimeout (38.68s)
--- PASS: TestAfterTimeout (42.82s)
--- PASS: TestBeforeCancellation (4.43s)
--- PASS: TestAfterCancellation (10.52s)
--- PASS: TestBeforeCleanup (10.57s)
--- PASS: TestAfterCleanup (18.73s)
PASS
ok  	github.com/Azure/taugrid/tests/e2e/provisioning	140.527s
```

## Results by scenario

### 1. Success — capacity can be provisioned

Job: one pod, 500m CPU.

| | Before (quota-only) | After (gated) |
|---|---|---|
| Test | `TestBeforeSuccess` — pass | `TestAfterSuccess` — pass |
| Admission | Immediately, on quota alone | Only after `Provisioned=True` |
| ProvisioningRequest | None | Created by Kueue, owned by the Workload |
| While the node is missing | Job resumed, pod exists and is `Pending` | Job suspended, no pod |
| Pod | Runs once the node is up | Runs, annotated as consuming that request |

Runs starting from a **zero-node pool**, timed from the moment Kueue reserved
quota (one sample each, taken in different runs):

| | Before | After |
|---|---|---|
| Workload admitted | 0s | 84s |
| Pod created | 1s, `Pending`, no node | 84s, node already there |
| Node created | 33s | 43s |
| Pod running | within 76s (test duration) | 87s |

Snapshot 20 seconds into the cold "before" run — admitted and resumed with
nowhere to run:

```text
job-success-a4812 QuotaReserved=True@20:34:44Z Admitted=True@20:34:44Z PodsReady=False
success-8q9tj phase=Pending created=20:34:45Z node=
job suspend=false
No ProvisioningRequest in namespace
node aks-prdemo-40886686-vmss000002 created=20:35:17Z
```

The equivalent timeline for the gated run is in
[`cpu-demo/README.md`](cpu-demo/README.md#timeline).

### 2. Timeout — capacity never arrives

Job: one pod asking for 600 CPU, which fits no node. A second 600 CPU job is
submitted afterwards; together they exceed the 1000 CPU quota.

| | Before (quota-only) | After (gated) |
|---|---|---|
| Test | `TestBeforeTimeout` — pass | `TestAfterTimeout` — pass |
| First workload | `Admitted=True` | `QuotaReserved=True`, `Admitted=False` (`UnsatisfiedAdmissionChecks`) |
| First job | Resumed (`suspend: false`) | Still suspended |
| Pod | Created, `Pending`, `Unschedulable` | None |
| Where the reason shows | Pod scheduling condition | ProvisioningRequest: `Provisioned=False`, `CapacityIsNotFound`, "Capacity is not found, CA will try to find it later." AdmissionCheck state `Pending` |
| Quota | Held | **Held** |
| Second job | `QuotaReserved=False`, "insufficient unused quota" | Same |
| Changes on its own (30s test window) | No | No |
| Recovery | Delete the stuck job; the second job then gets quota | Same |

Longer watch of the gated case, outside the test: one job (3 CPU on a pool whose
nodes have 1.9) was left for 21 minutes, 20:14:42–20:36 UTC. Its state never
changed:

```text
ProvisioningRequest: Accepted=True  Provisioned=False/CapacityIsNotFound
Workload: active=true QuotaReserved=True Admitted=False/UnsatisfiedAdmissionChecks  check=Pending
ClusterQueue: reservingWorkloads=1, cpu reserved=3
```

So on AKS there is **no timeout** for this case within 21 minutes: no
`Failed=True`, no retry, no rejection, no quota release. `ValidUntilSeconds`
does not apply either; it bounds how long provisioned capacity is held, not how
long provisioning may take. Whether the autoscaler eventually gives up after a
longer period was not observed.

### 3. Cancellation — job deleted while waiting for capacity

Job: the 600 CPU job, deleted once it is waiting.

| | Before (quota-only) | After (gated) |
|---|---|---|
| Test | `TestBeforeCancellation` — pass | `TestAfterCancellation` — pass |
| State when cancelled | Admitted, pod `Pending`/`Unschedulable` | Quota reserved, request `Accepted`, no pod |
| After deleting the Job | Workload gone, pod gone | Workload gone, ProvisioningRequest gone, never any pod |
| Queue | Idle: pending 0, reserving 0, admitted 0 | Same |

Not covered: cancelling while a scale-up is actually in flight. These tests
cancel a request the autoscaler could not satisfy, which makes the test
deterministic but does not show whether the autoscaler aborts a VM that is
already being created.

### 4. Cleanup — job runs to completion

Job: one pod, 500m CPU, `sleep 5`.

| | Before (quota-only) | After (gated) |
|---|---|---|
| Test | `TestBeforeCleanup` — pass | `TestAfterCleanup` — pass |
| Workload | `Finished=True` | `Finished=True` |
| Queue after finish | Idle | Idle |
| ProvisioningRequest | Never existed | Removed by Kueue after the workload finished |

Node scale-down is not asserted by the tests. It was observed once: the pool
returned to zero nodes about 14 minutes after the last job was removed, even
while an unsatisfiable ProvisioningRequest was still pending in another
namespace.

## What this means for the design

- The gate delivers the headline behaviour of issue #278: no pods and no resumed
  Job until nodes exist.
- It does **not** by itself solve "quota is taken but not used and the next job
  cannot proceed". On AKS that still needs a manual delete, or a separate
  mechanism that deactivates a workload whose check has been `Pending` too long.
  The retry settings in `ProvisioningRequestConfig.spec.retryStrategy` only act
  after the autoscaler reports a failure, which did not happen here.
- The failure-mode table in the design doc has been corrected accordingly.

## Not covered

- GPU pools and GPU VM sizes; multi-node requests and atomicity (M < N nodes).
- A request that the autoscaler accepts and then fails, booking expiry, retries.
- Cancellation during an in-flight scale-up.
- The pinned Kueue v0.19.2 build.
- The TauGrid chart, CLI and `ProvisioningReady` observer. These tests use plain
  Kueue objects. The chart objects were only validated by server-side dry run.
- Repeatability: each timing above is a single sample.
