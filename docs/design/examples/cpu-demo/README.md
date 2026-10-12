# Capacity-Aware Kueue Admission — AKS run log

**Status: incomplete.** The ProvisioningRequest flow has **not** been observed
end to end. This page records what was actually run and what it showed. An
earlier version of this page listed expected output and a ~20s timeline; none of
that was measured and it has been removed.

| | |
|---|---|
| Date | 2026-10-12 |
| Cluster | `helmdeep-aks` (`helmdeep-rg`), Kubernetes 1.35.7 |
| Nodes | 1 × `Standard_B2pls_v2` (arm64), pool `arm`, autoscaling off |
| Kueue | upstream v0.20.1 (TauGrid pins the MCR build v0.19.2, which was not tested) |

## What was run

1. Installed Kueue v0.20.1 from the upstream release manifest.
2. Lowered the Kueue manager CPU request from 500m to 100m, because the node had
   only ~390m unrequested CPU and the pod was `Pending` with `Insufficient cpu`.
3. Read the Kueue manager log.
4. Validated the demo manifests and the chart's manifest shapes with
   `kubectl apply --dry-run=server`.

Nothing else was applied. No queue objects, Job, ProvisioningRequest CRD, or
node pool were created.

## Findings

### 1. AKS does not ship the ProvisioningRequest CRD, so Kueue disables the feature

Kueue manager log at startup:

```
"msg":"Skipping admission check controller setup: Provisioning Requests not supported (Possible cause: missing or unsupported cluster-autoscaler)",
"error":"no matches for kind \"ProvisioningRequest\" in version \"autoscaling.x-k8s.io/v1\""
"msg":"Skipping provisioning controller setup: Provisioning Requests not supported (Possible cause: missing or unsupported cluster-autoscaler)"
```

`kubectl get crd | grep -Ei "provisioningrequest|autoscaling"` returned nothing.
Kueue checks for the CRD once at startup, so the CRD must be installed before
Kueue starts (or Kueue restarted afterwards). The runbook in the design doc does
not include this step.

### 2. No Cluster Autoscaler was running

`az aks show` reports `enableAutoScaling: false` on the only pool. Without an
autoscaler nothing sets `Provisioned=True`. Whether the **AKS managed** Cluster
Autoscaler processes ProvisioningRequests at all (it requires
`--enable-provisioning-requests` upstream, which AKS does not expose as a
setting) is **unverified** and is the main open risk for this design.

### 3. Kueue v0.20.1 serves only `v1beta2`

```
admissionchecks:            v1beta1(served=false) v1beta2(served=true,storage=true)
provisioningrequestconfigs: v1beta1(served=false) v1beta2(served=true,storage=true)
clusterqueues:              v1beta1(served=false) v1beta2(served=true,storage=true)
resourceflavors:            v1beta1(served=false) v1beta2(served=true,storage=true)
```

Affected in this branch (all use `kueue.x-k8s.io/v1beta1`):

- `charts/taugrid/templates/capacity-aware-admission.yaml` — `ProvisioningRequestConfig`, `ResourceFlavor`
- `cluster-overlays/queues/capacity-aware-admission-patch.yaml` — `ClusterQueue`
- `controllers/tau-core/internal/controller/provisioning_ready.go` — `provisioningRequestConfigGVK`

Server response for a `v1beta1` `ProvisioningRequestConfig`:

```
no matches for kind "ProvisioningRequestConfig" in version "kueue.x-k8s.io/v1beta1"
```

Not checked against the pinned v0.19.2 build, which may still serve `v1beta1`.

### 4. The chart's AdmissionCheck is rejected by the `v1beta2` schema

```
AdmissionCheck in version "v1beta2" cannot be handled as a AdmissionCheck:
strict decoding error: unknown field "spec.retryDelayMinutes"
```

`v1beta2` `AdmissionCheck.spec` has only `controllerName` and `parameters`.
Retry settings live on `ProvisioningRequestConfig.spec.retryStrategy`
(`backoffLimitCount`, `backoffBaseSeconds`, `backoffMaxSeconds`).

### 5. `v1beta2` LocalQueue uses `spec.clusterQueue`

```
LocalQueue in version "v1beta2" cannot be handled as a LocalQueue:
strict decoding error: unknown field "spec.clusterQueueName"
```

## Manifests that pass server-side validation on v0.20.1

Validated with `--dry-run=server` only; not applied.

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
```

## Remaining steps to complete the run

These were not executed (they change cluster infrastructure and need the cluster
owner's go-ahead):

```bash
# 1. Zero-node autoscaled pool, tainted so nothing else lands on it
az aks nodepool add --cluster-name helmdeep-aks --resource-group helmdeep-rg \
  --name prdemo --mode User --node-vm-size Standard_B2pls_v2 \
  --enable-cluster-autoscaler --min-count 0 --max-count 1 --node-count 0 \
  --node-taints demo=provisioning:NoSchedule --labels demo=provisioning

# 2. ProvisioningRequest CRD, then restart Kueue so it detects it
kubectl apply --server-side -f \
  https://raw.githubusercontent.com/kubernetes/autoscaler/master/cluster-autoscaler/apis/config/crd/autoscaling.x-k8s.io_provisioningrequests.yaml
kubectl -n kueue-system rollout restart deploy/kueue-controller-manager

# 3. Apply the manifests above plus a LocalQueue (spec.clusterQueue) and a
#    suspended Job labelled kueue.x-k8s.io/queue-name, tolerating the taint.

# 4. Observe
kubectl get provisioningrequests -A -w
kubectl get workloads -A -o wide
```

The result to record is whether the managed autoscaler sets `Provisioned=True`
and scales `prdemo` from 0 to 1, or leaves the request unanswered.

## Cluster state left by this run

- Namespace `kueue-system` with Kueue v0.20.1, its CRDs, and its webhook
  configurations (manager CPU request patched to 100m).

Remove with:

```bash
kubectl delete -f https://github.com/kubernetes-sigs/kueue/releases/download/v0.20.1/manifests.yaml
```
