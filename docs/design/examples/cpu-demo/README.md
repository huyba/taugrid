# Capacity-Aware Kueue Admission — AKS run log

**Result: the flow works on AKS.** A suspended Job was held by Kueue, the AKS
managed Cluster Autoscaler accepted the ProvisioningRequest and scaled a node
pool from 0 to 1, and only then was the Job unsuspended. Submit to container
start took 88 seconds.

Everything on this page was observed on the cluster. What the run does **not**
cover is listed under [Not tested](#not-tested).

| | |
|---|---|
| Date | 2026-10-12 (times UTC) |
| Cluster | `helmdeep-aks` (`helmdeep-rg`), Kubernetes 1.35.7 |
| System pool | `arm`, 1 × `Standard_B2pls_v2` (arm64), autoscaling off |
| Demo pool | `prdemo`, `Standard_B2pls_v2`, autoscaler min 0 / max 1, taint `demo=provisioning:NoSchedule`, label `demo=provisioning` |
| Kueue | upstream v0.20.1 (TauGrid pins the MCR build v0.19.2, which was not tested) |
| Provisioning class | `best-effort-atomic-scale-up.autoscaling.x-k8s.io` |
| Workload | `batch/v1` Job, 1 pod, 500m CPU / 256Mi |

## Timeline

| Time | Δ | Event | Source |
|---|---|---|---|
| 18:40:52 | 0s | Job applied with `suspend: true` | `kubectl apply` |
| 18:40:53 | 1s | `QuotaReserved=True`; Kueue creates ProvisioningRequest `job-demo-sleep-job-3de63-demo-provisioning-check-1` | Workload condition, event |
| 18:40:57 | 5s | `Accepted=True` — "ProvisioningRequest is accepted by ClusterAutoscaler" | ProvisioningRequest condition |
| 18:41:36 | 44s | Node `aks-prdemo-40886686-vmss000000` created (pool 0 → 1) | Node `creationTimestamp` |
| 18:42:17 | 85s | `Provisioned=True` — "Capacity is found in the cluster"; AdmissionCheck `Pending → Ready`; Workload `Admitted=True`; Job resumed; pod created | conditions, events |
| 18:42:20 | 88s | Container started on the new node | Pod event |
| 18:42:22 | 90s | Workload `PodsReady=True` | Workload condition |
| — | — | Job `Complete 1/1`; pod log: `admitted after provisioning` | `kubectl get job`, `kubectl logs` |

The node was created 41 seconds **before** the pod existed, so the scale-up was
driven by the ProvisioningRequest, not by a pending pod. The Job stayed
`suspend: true` for the whole 85 seconds.

## Setup that was required

```bash
# 1. Kueue
kubectl apply --server-side -f \
  https://github.com/kubernetes-sigs/kueue/releases/download/v0.20.1/manifests.yaml
# On this 2-vCPU node the manager was Pending (Insufficient cpu) until:
kubectl -n kueue-system set resources deploy/kueue-controller-manager \
  -c manager --requests=cpu=100m,memory=256Mi

# 2. ProvisioningRequest CRD — AKS does not install it
kubectl apply --server-side -f \
  https://raw.githubusercontent.com/kubernetes/autoscaler/master/cluster-autoscaler/apis/config/crd/autoscaling.x-k8s.io_provisioningrequests.yaml
kubectl -n kueue-system rollout restart deploy/kueue-controller-manager

# 3. Zero-node autoscaled pool
az aks nodepool add --cluster-name helmdeep-aks --resource-group helmdeep-rg \
  --name prdemo --mode User --node-vm-size Standard_B2pls_v2 \
  --enable-cluster-autoscaler --min-count 0 --max-count 1 --node-count 0 \
  --node-taints demo=provisioning:NoSchedule --labels demo=provisioning
```

Step 2 matters. Before the CRD existed, Kueue logged:

```text
"msg":"Skipping provisioning controller setup: Provisioning Requests not supported (Possible cause: missing or unsupported cluster-autoscaler)"
"error":"no matches for kind \"ProvisioningRequest\" in version \"autoscaling.x-k8s.io/v1\""
```

Kueue checks once at startup, hence the restart. After it, the
`provisioning-workload-reconciler` started and the AdmissionCheck went
`Active=True` ("The admission check is active").

## Manifests applied

```yaml
apiVersion: kueue.x-k8s.io/v1beta2
kind: ProvisioningRequestConfig
metadata:
  name: demo-provisioning-config
spec:
  provisioningClassName: best-effort-atomic-scale-up.autoscaling.x-k8s.io
  managedResources:
  - cpu
  parameters:
    ValidUntilSeconds: "600"
---
apiVersion: kueue.x-k8s.io/v1beta2
kind: AdmissionCheck
metadata:
  name: demo-provisioning-check
spec:
  controllerName: kueue.x-k8s.io/provisioning-request
  parameters:
    apiGroup: kueue.x-k8s.io
    kind: ProvisioningRequestConfig
    name: demo-provisioning-config
---
apiVersion: kueue.x-k8s.io/v1beta2
kind: ResourceFlavor
metadata:
  name: demo-cpu-flavor
spec:
  nodeLabels:
    demo: provisioning
  tolerations:
  - key: demo
    operator: Equal
    value: provisioning
    effect: NoSchedule
---
apiVersion: kueue.x-k8s.io/v1beta2
kind: ClusterQueue
metadata:
  name: demo-cluster-queue
spec:
  namespaceSelector: {}
  resourceGroups:
  - coveredResources: [cpu, memory]
    flavors:
    - name: demo-cpu-flavor
      resources:
      - name: cpu
        nominalQuota: "4"
      - name: memory
        nominalQuota: 8Gi
  admissionChecksStrategy:
    admissionChecks:
    - name: demo-provisioning-check
      onFlavors:
      - demo-cpu-flavor
---
apiVersion: kueue.x-k8s.io/v1beta2
kind: LocalQueue
metadata:
  name: demo-local-queue
  namespace: tau-demo
spec:
  clusterQueue: demo-cluster-queue
---
apiVersion: batch/v1
kind: Job
metadata:
  name: demo-sleep-job
  namespace: tau-demo
  labels:
    kueue.x-k8s.io/queue-name: demo-local-queue
spec:
  suspend: true
  template:
    spec:
      restartPolicy: Never
      containers:
      - name: demo
        image: busybox:1.36
        command: ["sh", "-c", "echo admitted after provisioning; sleep 30"]
        resources:
          requests:
            cpu: "500m"
            memory: "256Mi"
```

## Observed objects

ProvisioningRequest (created by Kueue, owned by the Workload):

```text
owner=Workload/job-demo-sleep-job-3de63
class=best-effort-atomic-scale-up.autoscaling.x-k8s.io params={"ValidUntilSeconds":"600"}
podSets=[{"count":1,"podTemplateRef":{"name":"ppt-job-demo-sleep-job-3de63-demo-provisioning-check-1-main"}}]
2026-10-12T18:40:57Z Accepted=True    reason=Accepted              msg=ProvisioningRequest is accepted by ClusterAutoscaler
2026-10-12T18:42:17Z Provisioned=True reason=CapacityIsProvisioned msg=Capacity is found in the cluster
```

Workload:

```text
2026-10-12T18:40:53Z QuotaReserved=True reason=QuotaReserved
2026-10-12T18:42:17Z Admitted=True      reason=Admitted
2026-10-12T18:42:22Z PodsReady=True     reason=Started
check demo-provisioning-check state=Ready msg=Capacity is found in the cluster
event: Admitted by ClusterQueue demo-cluster-queue, wait time since reservation was 84s
```

Pod, as mutated by Kueue on admission (the Job template had no selector,
toleration, or annotations):

```text
node=aks-prdemo-40886686-vmss000000
nodeSelector={"demo":"provisioning"}
toleration={"effect":"NoSchedule","key":"demo","operator":"Equal","value":"provisioning"}
autoscaling.x-k8s.io/consume-provisioning-request=job-demo-sleep-job-3de63-demo-provisioning-check-1
autoscaling.x-k8s.io/provisioning-class-name=best-effort-atomic-scale-up.autoscaling.x-k8s.io
```

## Issues found in this branch

Found by server-side dry run against Kueue v0.20.1. Not checked against the
pinned v0.19.2 build, which may behave differently.

1. **`v1beta1` is not served.** Only `kueue.x-k8s.io/v1beta2` is served for
   AdmissionCheck, ProvisioningRequestConfig, ClusterQueue and ResourceFlavor.
   Affected: `charts/taugrid/templates/capacity-aware-admission.yaml`
   (ProvisioningRequestConfig, ResourceFlavor),
   `cluster-overlays/queues/capacity-aware-admission-patch.yaml` (ClusterQueue),
   and `provisioningRequestConfigGVK` in `provisioning_ready.go`.
2. **The chart's AdmissionCheck is rejected:**
   `strict decoding error: unknown field "spec.retryDelayMinutes"`. Retry
   settings belong in `ProvisioningRequestConfig.spec.retryStrategy`
   (`backoffLimitCount`, `backoffBaseSeconds`, `backoffMaxSeconds`).
3. **`v1beta2` LocalQueue uses `spec.clusterQueue`**, not `spec.clusterQueueName`.
4. **The runbook omits the ProvisioningRequest CRD install**, without which
   Kueue silently disables the feature.

## Not tested

- GPU node pools and GPU VM sizes (this was one arm64 CPU node).
- Atomicity: a request for N > 1 nodes where only M < N can be provisioned.
- Failure paths: capacity exhaustion, `ValidUntilSeconds` expiry, retries.
- Cancellation while provisioning is in flight.
- The MCR Kueue v0.19.2 build that TauGrid pins.
- The TauGrid chart, CLI, and the `ProvisioningReady` observer (the observer is
  not wired in `main.go`). Only upstream Kueue objects were used.
- Scale-down of `prdemo` back to zero after the Job finished.

## Cluster state left by this run

- `kueue-system`: Kueue v0.20.1, its CRDs and webhooks (manager CPU request 100m).
- CRD `provisioningrequests.autoscaling.x-k8s.io`.
- Node pool `prdemo` (autoscaled 0–1; one node was up when the run ended).
- Namespace `tau-demo` and the cluster-scoped `demo-*` Kueue objects.

Cleanup:

```bash
kubectl delete namespace tau-demo
kubectl delete clusterqueue demo-cluster-queue
kubectl delete admissioncheck demo-provisioning-check
kubectl delete provisioningrequestconfig demo-provisioning-config
kubectl delete resourceflavor demo-cpu-flavor
kubectl delete -f https://github.com/kubernetes-sigs/kueue/releases/download/v0.20.1/manifests.yaml
kubectl delete crd provisioningrequests.autoscaling.x-k8s.io
az aks nodepool delete --cluster-name helmdeep-aks --resource-group helmdeep-rg --name prdemo
```
