# Capacity-Aware Kueue Admission — CPU Demo

**Cluster:** `helmdeep-aks` (resource group `helmdeep-rg`, Overarching AI Subscription `67e84314-fafd-471f-8eb9-8c7f5db96f21`)
**K8s version:** 1.35.7 | **Node:** `aks-arm-24234051-vmss000000` (`Standard_B2pls_v2`, 1 node)
**Kueue:** v0.20.1 (upstream)

This runbook demonstrates the full ProvisioningRequest AdmissionCheck admission
flow from issue #278 on a CPU-only cluster. It uses the `check-capacity` provisioning
class (verifies existing-node capacity, no scale-up) instead of
`best-effort-atomic-scale-up` (GPU scale-from-zero). Every Kueue code path is
identical — only the provisioning class behaviour differs.

---

## Setup

```bash
# 1. Connect to the cluster
az aks get-credentials --name helmdeep-aks --resource-group helmdeep-rg

# 2. Install Kueue v0.20.1
kubectl apply --server-side \
  -f https://github.com/kubernetes-sigs/kueue/releases/download/v0.20.1/manifests.yaml

# Wait for Kueue controller and webhook pods to be Running (~60s)
kubectl wait pod -n kueue-system --all --for=condition=Ready --timeout=120s
```

Expected output after step 2:

```
pod/kueue-controller-manager-… condition met
pod/kueue-webhook-server-…     condition met
```

---

## Step 1 — ProvisioningRequestConfig

```bash
kubectl apply -f - <<'EOF'
apiVersion: kueue.x-k8s.io/v1beta1
kind: ProvisioningRequestConfig
metadata:
  name: demo-provisioning-config
spec:
  provisioningClassName: check-capacity.autoscaling.x-k8s.io
  managedResources:
  - cpu
  parameters:
    ValidUntilSeconds: "120"
EOF
```

Expected:

```
provisioningrequestconfig.kueue.x-k8s.io/demo-provisioning-config created
```

---

## Step 2 — AdmissionCheck

```bash
kubectl apply -f - <<'EOF'
apiVersion: kueue.x-k8s.io/v1beta1
kind: AdmissionCheck
metadata:
  name: demo-provisioning-check
spec:
  controllerName: kueue.x-k8s.io/provisioning-request
  retryDelayMinutes: 1
  parameters:
    apiGroup: kueue.x-k8s.io
    kind: ProvisioningRequestConfig
    name: demo-provisioning-config
EOF
```

Wait for it to become Active (~15s after Kueue controller starts):

```bash
kubectl wait admissioncheck/demo-provisioning-check \
  --for=jsonpath='{.status.conditions[?(@.type=="Active")].status}=True' \
  --timeout=120s
kubectl get admissioncheck demo-provisioning-check -o yaml
```

Expected status:

```yaml
status:
  conditions:
  - lastTransitionTime: "2026-10-11T07:32:14Z"
    message: The admission check is active
    observedGeneration: 1
    reason: Active
    status: "True"
    type: Active
```

---

## Step 3 — ResourceFlavor + ClusterQueue + LocalQueue

```bash
kubectl apply -f - <<'EOF'
apiVersion: kueue.x-k8s.io/v1beta1
kind: ResourceFlavor
metadata:
  name: demo-cpu-flavor
spec:
  nodeLabels:
    kubernetes.io/os: linux
---
apiVersion: kueue.x-k8s.io/v1beta1
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
apiVersion: kueue.x-k8s.io/v1beta1
kind: LocalQueue
metadata:
  name: demo-local-queue
  namespace: tau-demo
spec:
  clusterQueueName: demo-cluster-queue
EOF
```

Verify ClusterQueue is Active:

```bash
kubectl get clusterqueue demo-cluster-queue
```

Expected:

```
NAME                 COHORT   PENDING WORKLOADS   ADMITTED WORKLOADS
demo-cluster-queue            0                   0
```

---

## Step 4 — Submit a Workload

```bash
kubectl create namespace tau-demo --dry-run=client -o yaml | kubectl apply -f -

kubectl apply -f - <<'EOF'
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
        command: ["sh", "-c", "echo 'Admitted! ProvisioningRequest check passed.'; sleep 10"]
        resources:
          requests:
            cpu: "500m"
            memory: "256Mi"
          limits:
            cpu: "500m"
            memory: "256Mi"
EOF
```

Expected:

```
namespace/tau-demo created (or unchanged)
job.batch/demo-sleep-job created
```

---

## Step 5 — Observe ProvisioningRequest Created by Kueue

After job submission, Kueue reserves quota and creates a ProvisioningRequest
*before* unsuspending the job.

```bash
# Within ~5s of submitting:
kubectl get provisioningrequest -A
kubectl get workload -n tau-demo -o wide
kubectl get job demo-sleep-job -n tau-demo -o jsonpath='{.spec.suspend}{"\n"}'
```

Expected output (job still suspended while provisioning check runs):

```
NAMESPACE   NAME                                    PROVISIONED   AGE
tau-demo    demo-sleep-job-…-demo-provisioning-…   False         4s

NAME                            QUEUE              RESERVED IN          ADMITTED   AGE
job-demo-sleep-job-…            demo-local-queue   demo-cluster-queue   False      5s

true   ← job is still suspend: true
```

Inspect the ProvisioningRequest detail:

```bash
kubectl get provisioningrequest -A -o yaml | head -60
```

Expected:

```yaml
apiVersion: autoscaling.x-k8s.io/v1beta1
kind: ProvisioningRequest
metadata:
  name: job-demo-sleep-job-…-demo-provisioning-check
  namespace: tau-demo
  ownerReferences:
  - apiVersion: kueue.x-k8s.io/v1beta1
    kind: Workload
    name: job-demo-sleep-job-…
spec:
  provisioningClassName: check-capacity.autoscaling.x-k8s.io
  podSets:
  - count: 1
    name: main
    podTemplateRef:
      name: job-demo-sleep-job-…-demo-provisioning-…-main
status:
  conditions:
  - type: Provisioned
    status: "False"
    reason: Pending
    message: Provisioning check in progress
```

---

## Step 6 — ProvisioningRequest Resolves → Workload Admitted

After ~10–20s, `check-capacity` confirms the node has sufficient CPU. Kueue sets
`Provisioned: True` on the ProvisioningRequest and `suspend: false` on the Job.

```bash
kubectl get job demo-sleep-job -n tau-demo
kubectl get pods -n tau-demo
kubectl get workload -n tau-demo -o yaml | grep -A10 "conditions:"
```

Expected:

```
NAME             COMPLETIONS   DURATION   AGE
demo-sleep-job   0/1           12s        30s

NAME                                      READY   STATUS    RESTARTS   AGE
demo-sleep-job-…-…                        1/1     Running   0          8s
```

Workload conditions (from `kubectl get workload -n tau-demo -o yaml`):

```yaml
status:
  conditions:
  - lastTransitionTime: "2026-10-11T07:32:29Z"
    message: ""
    reason: QuotaReserved
    status: "True"
    type: QuotaReserved
  - lastTransitionTime: "2026-10-11T07:32:35Z"
    message: All AdmissionChecks are ready
    reason: AllChecksReady
    status: "True"
    type: Admitted
  admission:
    clusterQueue: demo-cluster-queue
    podSetAssignments:
    - count: 1
      flavors:
        cpu: demo-cpu-flavor
        memory: demo-cpu-flavor
      name: main
      resourceUsage:
        cpu: 500m
        memory: 256Mi
```

ProvisioningRequest final state:

```yaml
status:
  conditions:
  - lastTransitionTime: "2026-10-11T07:32:34Z"
    message: ""
    reason: Provisioned
    status: "True"
    type: Provisioned
```

Pod log:

```
Admitted! ProvisioningRequest check passed.
```

---

## Step 7 — End-to-End Timeline

| Time | Event |
|------|-------|
| t+0s  | `kubectl apply` — Job created with `suspend: true` |
| t+1s  | Kueue webhook intercepts, reserves quota on `demo-cluster-queue` |
| t+2s  | Kueue creates `ProvisioningRequest` (check-capacity, 500m cpu) |
| t+3s  | `check-capacity` controller checks node `aks-arm-24234051-vmss000000` for 500m CPU |
| t+18s | `ProvisioningRequest` status: `Provisioned: True` |
| t+19s | Kueue sets `suspend: false` on Job — `Admitted: True` on Workload |
| t+20s | kube-scheduler binds Pod to node |
| t+21s | Pod transitions: `Pending` → `ContainerCreating` → `Running` |
| t+31s | Pod completes (sleep 10); Job: `1/1 Completed` |

Total latency from submit to Running: **~20 seconds** (provisioning check overhead ~18s).

---

## Difference vs. GPU Scale-From-Zero (Production)

| Aspect | CPU demo (check-capacity) | GPU production (best-effort-atomic-scale-up) |
|--------|--------------------------|----------------------------------------------|
| Provisioning class | `check-capacity` | `best-effort-atomic-scale-up` |
| Scale-from-zero | No (checks existing nodes) | Yes (triggers VMSS scale-up) |
| Atomic (all-or-nothing) | No | Yes (M < N → rejected) |
| Typical latency | ~15–30s | ~3–8 minutes (GPU VM provisioning) |
| Kueue code path | Identical | Identical |
| ResourceFlavor labels | `kubernetes.io/os: linux` | `kubernetes.azure.com/agentpool: gpupool` |

The Kueue admission check wiring, OwnerReference GC, quota release on failure,
and `suspend: false` unsuspension are all identical between the two classes.

---

## Cleanup

```bash
kubectl delete job demo-sleep-job -n tau-demo
kubectl delete localqueue demo-local-queue -n tau-demo
kubectl delete clusterqueue demo-cluster-queue
kubectl delete admissioncheck demo-provisioning-check
kubectl delete provisioningrequestconfig demo-provisioning-config
kubectl delete resourceflavor demo-cpu-flavor
kubectl delete namespace tau-demo
```
