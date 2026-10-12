# Capacity-Aware Kueue Admission — examples and test results

Everything here was run on a real AKS cluster on 2026-10-12. It compares
**before** (quota-only admission) with **after** (admission gated by a
ProvisioningRequest AdmissionCheck) across four scenarios, and then shows the two ways a gated queue gets its quota
back when capacity does not arrive.

| Document | Contents |
|---|---|
| This page | The before/after test matrix: what each case does and what happened |
| [`cpu-demo/README.md`](cpu-demo/README.md) | Step-by-step log of the first manual run: setup commands, manifests, timeline, object dumps, cleanup |
| [`tests/e2e/provisioning/`](../../../tests/e2e/provisioning/) | The test code and queue fixtures |

## Summary

All tests pass. What they show:

1. **Success works.** With the gate, Kueue held the Job suspended until the AKS
   Cluster Autoscaler scaled the pool from zero and reported `Provisioned=True`.
2. **The gate alone does not release quota when capacity never arrives.** The
   AKS autoscaler does not fail an unsatisfiable request; it reports
   `CapacityIsNotFound` and keeps retrying. Kueue keeps the quota reserved and a
   second job stays starved, as in quota-only mode. In a 21-minute watch the
   request never failed.
3. **If the autoscaler does report `Failed`, Kueue releases the quota.** With
   the default retry settings the starved job got the quota about 60 seconds
   after the failure.
4. **A provisioning timeout closes the gap.** The new
   `--provisioning-timeout` in tau-core-controller deactivates a workload whose
   provisioning check has been pending too long. With a 45s timeout the stuck
   workload was deactivated 45 seconds after it reserved quota, its
   ProvisioningRequest was removed, and the starved job got the quota.
5. **Cancellation and cleanup are clean in both modes.** Deleting the Job removes
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

Output of the full run (21:02:02–21:06:43 UTC, pool starting at zero nodes, no
timeout controller running):

```text
--- PASS: TestBeforeSuccess (78.42s)
--- PASS: TestAfterSuccess (8.57s)
--- PASS: TestBeforeTimeout (36.68s)
--- PASS: TestAfterTimeout (44.97s)
--- PASS: TestBeforeCancellation (4.40s)
--- PASS: TestAfterCancellation (10.56s)
--- PASS: TestBeforeCleanup (10.61s)
--- PASS: TestAfterCleanup (16.75s)
--- PASS: TestAfterFailedReleasesQuota (66.62s)
--- SKIP: TestAfterTimeoutReleasesQuota (0.00s)
PASS
ok  	github.com/Azure/taugrid/tests/e2e/provisioning	279.361s
```

`TestBeforeSuccess` ran first and paid for the scale-up; `TestAfterSuccess`
then found the node already there. `TestAfterTimeoutReleasesQuota` needs the
timeout controller and was run separately (scenario 6): pass, 52.21s.

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

This is the behaviour without the provisioning timeout. Scenario 6 shows the
same case with the timeout enabled.

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

## Releasing quota when capacity does not arrive

Both tests use the gated queue and the same pair of jobs as the timeout
scenario: a 600 CPU job that can never be provisioned, and a second 600 CPU job
starved by it.

### 5. Failed — the autoscaler gives up

`TestAfterFailedReleasesQuota` — pass (66.5s).

The AKS autoscaler was never seen to fail a request, so the test writes the
`Failed=True` condition onto the ProvisioningRequest, as the autoscaler would.
State 45 seconds into the run:

```text
ProvisioningRequest: Accepted=True  Provisioned=False/CapacityIsNotFound  Failed=True/SimulatedByTest
job-too-big:  QuotaReserved=True  check=Retry "Retrying after failure: e2e test marked the request as failed"
              requeueState.requeueAt = 60s after the failure
job-starved:  QuotaReserved=False "insufficient unused quota for cpu ..., 200 more needed"
```

| Step | Result |
|---|---|
| Kueue sees `Failed=True` | AdmissionCheck goes `Pending → Retry`, requeue scheduled 60s out |
| During the backoff | Quota is still held by the failed workload |
| After the backoff (about 60s) | Starved job gets `QuotaReserved=True` |
| Failed job | Still suspended, never got a pod |

The 60 seconds is Kueue's default `retryStrategy.backoffBaseSeconds`. The chart
exposes it as `capacityAwareAdmission.admissionCheck.minBackoffSeconds`, with
`retryLimit` bounding how many times the workload is retried before the check is
rejected.

### 6. Timeout — the autoscaler never answers

`TestAfterTimeoutReleasesQuota` — pass (52.2s), with the
`ProvisioningTimeoutReconciler` running against the cluster at a 45s timeout.

| Step | Result |
|---|---|
| Stuck workload holds quota for 45s | Controller sets `spec.active=false` on the Workload |
| Kueue | Evicts the workload: `QuotaReserved=False`; deletes the ProvisioningRequest |
| Starved job | Gets `QuotaReserved=True` |
| Timed-out job | Still suspended, never got a pod |
| Why | Warning event `ProvisioningTimeout` on the Workload |

Controller log:

```text
INFO  Starting workers  {"controller": "provisioning-timeout", "controllerKind": "Workload"}
INFO  deactivated workload after provisioning timeout  {"Workload": {"name":"job-too-big-b74d9","namespace":"e2e-capacity-after"}, "admissionCheck": "e2e-capacity-after-check", "waited": "45.074249s"}
```

The timed-out workload stays deactivated; it is not retried. The user has to
resubmit, or delete the Job.

This was run two ways, both from a workstation against the cluster (no
controller image was deployed):

1. **Only the timeout reconciler**, through the gated harness in the controller
   module. Result above (52.2s).
2. **The full tau-core-controller binary** built from this branch, after
   installing the three TauGrid CRDs from `charts/tau-core-controller/crds/`.
   All four reconcilers started (TauCluster, TauWorkspace, TauQuotaRequest and
   provisioning-timeout) with no errors logged, and the whole test package was
   run against it:

```text
--- PASS: TestBeforeSuccess (82.41s)
--- PASS: TestAfterSuccess (6.42s)
--- PASS: TestBeforeTimeout (38.84s)
--- SKIP: TestAfterTimeout (0.00s)
--- PASS: TestBeforeCancellation (4.48s)
--- PASS: TestAfterCancellation (10.61s)
--- PASS: TestBeforeCleanup (10.72s)
--- PASS: TestAfterCleanup (16.71s)
--- PASS: TestAfterFailedReleasesQuota (66.33s)
--- PASS: TestAfterTimeoutReleasesQuota (50.07s)
PASS
ok  	github.com/Azure/taugrid/tests/e2e/provisioning	288.471s
```

```bash
# full controller, 45s timeout
kubectl apply --server-side -f charts/tau-core-controller/crds/
cd controllers/tau-core
go run ./cmd/controller --provisioning-timeout=45s \
  --metrics-bind-address=0 --health-probe-bind-address=0

# the reconciler alone, where the CRDs are not installed
TAU_PROVISIONING_TIMEOUT_LIVE=45s TAU_PROVISIONING_TIMEOUT_LIVE_RUN=4m \
go test -run '^TestProvisioningTimeoutLive$' -timeout 10m ./internal/controller/

# in another terminal: the e2e tests, told what timeout the controller uses
cd tests/e2e
AI_RUNTIME_E2E=1 E2E_PROVISIONING=1 E2E_PROVISIONING_TIMEOUT=45s \
E2E_PROVISIONING_NODE_SELECTOR='demo=provisioning' \
go test -v -timeout 30m ./provisioning/
```

Two things the full run showed about choosing the timeout:

- **It must be longer than a normal scale-up.** The clock starts when quota is
  reserved, and a cold scale-up here took about 85s. With a 45s timeout a job
  that needs a new node would be deactivated before the node arrives.
  `TestAfterSuccess` passed only because the node was already up.
- **It interacts with Kueue's retry backoff.** In the failed-request test the
  controller deactivated the failed workload 60s after it reserved quota (its
  log shows `waited: 1m0.08s`), which is when Kueue's 60s retry backoff ended.
  The likely cause is that the check went back to `Pending` while the original
  reservation time was still in place; this was not investigated further. A
  timeout well above `backoffBaseSeconds` avoids it, but a shorter one can turn
  the first retry into a termination.

Still not exercised: the controller image, in-cluster RBAC (the `patch` rule on
workloads) and the Helm wiring.

`TestAfterTimeout` (scenario 2) is skipped when `E2E_PROVISIONING_TIMEOUT` is
set, because with the controller running the workload no longer stays stuck.

To enable the timeout in a deployment:

```yaml
tau-core-controller:
  provisioningTimeout: 30m
```

## What this means for the design

- The gate delivers the headline behaviour of issue #278: no pods and no resumed
  Job until nodes exist.
- By itself it does not solve "quota is taken but not used and the next job
  cannot proceed", because the AKS autoscaler never fails the request. The
  provisioning timeout is what releases the quota in that case.
- If the autoscaler does fail a request, Kueue's own retry-then-reject handling
  releases the quota without TauGrid's involvement.
- The failure-mode table in the design doc reflects all three.

## Not covered

- GPU pools and GPU VM sizes; multi-node requests and atomicity (M < N nodes).
- A failure reported by the real autoscaler (it was simulated), booking expiry,
  and what happens after the retry limit is exhausted.
- The provisioning timeout in a deployed controller image, its in-cluster RBAC
  and its chart wiring. The controller was run from a workstation, and the
  chart tests could not be run here (`helm` is not installed).
- Cancellation during an in-flight scale-up.
- The pinned Kueue v0.19.2 build.
- The TauGrid chart, CLI and `ProvisioningReady` observer. These tests use plain
  Kueue objects. The chart objects were only validated by server-side dry run.
- Repeatability: each timing above is a single sample.
