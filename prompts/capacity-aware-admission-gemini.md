# Capacity Aware Admission Taugrid — Gemini conversation

Conversation with Google Gemini used to draft the design prompt for issue #278
(capacity-aware Kueue admission). It was later given to Claude Code as the
starting prompt; that session is in
[`capacity-aware-kueue-admission-session.md`](capacity-aware-kueue-admission-session.md).

- Source: <https://gemini.google.com/app/41a3ab667c13aa62>
- Text is as pasted by the user; only Markdown structure (headings, code fences,
  paragraph breaks) was added. Parts are in Vietnamese.
- Gemini's answers were written before anything was run. Several claims in them do
  not match what was later observed on AKS — notably that a request the autoscaler
  cannot satisfy is failed and its quota released immediately, and the `v1beta1`
  API versions in the YAML. See `docs/design/examples/README.md` for the measured
  behaviour.


---

## 1. User

bai toan day - https://github.com/Azure/taugrid/issues/278 hay viet promt day du cu the chi tiet xem nen thiet ke the nao 1. User: tạo container image, quyết định sẽ chạy thế nào, cần resoruce gì, bao nhiêu, submit request tới taugrid 2. Taugrid: nhận user request, gọi Kueue qua k8s server api 3. Kueue: nhận request và ghi vào etcd, đánh dấu suspend: true để bọn Job controller/kuberay controller ko động vào 4. Kueue sau đó đích thân tạo ra 1 províison request để xin request, cái này crd và nó được track bới cluster autoscaler (1 phần của k8s) 5. Cluster Autoscaler track được provisiionreuqest có tạo mới thì liênf đọc xem cần gì và gọi cloud autoscaler để bật máy lên, sau khi bật xong thì báo lại cho cluster scalẻ là bật xong rồi 6. clusster scaler cũng check provision request, thấy xong rồi thì liềnbáo cho kueue 7. kueue đánh dấu là ngon rồi suspend: false, thế là bọn job scheduler/kueuray scheduler nhảy vào đọc từ etcd và kéo container với thông tin về vào tạo pod, đánh dấu pod là pending 8. k8s scheduler check thấy pending liền nhảy vào bỏ pod vào máy móc đã được cấp bới cloud autoscaler ở bước 5 9. sau khi nó xong thì k8s scheduler update là pod đã ngon rồi, kubelet báo là ngon => update job info trong ectd 10. các thông về provisionrequest, kueue và job đều được ghi ở etcd, taugrid chỉ cần check thằng này là biết mọi thứ ok hay ko để báo lại cho user 11. nếu có sai: provisionrequest false thì kueue sẽ bỏ requêst nẩy, trả lại quota để job sau còn có mà dùng. 12. nếu chỉ cấp được 1 phần (xin 8 máy mà cloud auto scaler chỉ cấp được 4 máy) thì trả lại phần đã cấp và tra lại quota 13. nếu cấp được hết mà tạo pod fail hoặc ko tải pod lên 8 máy đó thì fallback, release 8 máy, release quota Chuỗi luồng dữ liệu (Data Flow & Lifecycle) bạn vừa tóm tắt lại rất chính xác, đầy đủ và sâu sắc về mặt kỹ thuật! Bạn đã nắm trọn vẹn cả kiến trúc hệ thống lẫn cơ chế hoạt động bên dưới của Kubernetes, Kueue, Cluster Autoscaler (CA) và TauGrid. Dưới đây là một vài điểm chuẩn hóa nhỏ mang tính tinh chỉnh (refinement) để bức tranh tổng thể đạt mức $100\%$ chuẩn xác theo đúng specs của K8s ecosystem: Điểm danh lại các bước & Tinh chỉnh nhẹ về mặt kỹ thuật - Bước 1, 2, 3 (Khởi tạo & Cọc Quota Logic): Chính xác $100\%$. - TauGrid CLI render YAML với suspend: true. Kueue nhận Workload qua K8s API Server, kiểm tra Quota logical trên ClusterQueue. Nếu còn quota, Kueue giữ cọc (reserve quota) nhưng vẫn giữ suspend: true để Job Controller / KubeRay Operator không tạo Pod. - - Bước 4 (Kueue tạo ProvisioningRequest): Chính xác. - Kueue chính là bên đứng ra tạo đối tượng ProvisioningRequest (CRD thuộc nhóm autoscaling.x-k8s.io). - - Bước 5, 6 (Cluster Autoscaler tương tác Cloud API): Chính xác. - Cluster Autoscaler (chạy trong Control Plane) thấy ProvisioningRequest mới liền gọi Cloud Provider API (ví dụ: Azure VMSS, AWS Auto Scaling Group) để provision đúng SKU / số lượng VM. Khi máy khởi chạy và gia nhập cluster (Node Ready), CA cập nhật Condition trên ProvisioningRequest thành Provisioned: True. Kueue đọc trạng thái này từ etcd. - - Bước 7, 8, 9 (Chuyển giao cho K8s Scheduler): Chính xác & Rõ ràng. - - - Kueue thấy Provisioned: True liền cập nhật suspend: false trên Job/RayJob. - - Lúc này KubeRay / Job Controller mới tạo các Pod thật (ở trạng thái Pending). - - kube-scheduler quét các Pod Pending và gán (bind) chúng vào đúng các Node vừa tạo nhờ cơ chế podSetUpdates / nodeSelector. - - kubelet trên từng Node kéo container image về, chạy container và báo trạng thái Running về K8s API Server (lưu vào etcd). - - Bước 10 (Role của TauGrid): Chính xác. - TauGrid đóng vai trò Quan sát (Observer/Workflow UI). Nó chỉ cần poll/watch trạng thái từ API Server (etcd) thông qua TauCluster controller để cập nhật cho user trên terminal (Capacity provisioning Pod scheduling Running). - Xử lý các kịch bản ngoại lệ (Steps 11, 12, 13) Cơ chế xử lý lỗi trong đề xuất của bạn hoàn toàn khớp với spec Atomic Scale-Up (Scale nguyên cụm): - Bước 11 (Provisioning Request False): Chính xác. - Nếu Cloud hết máy (Out of Capacity) hoặc Bounded Timeout, CA đánh dấu Failed hoặc CapacityIsNotFound. Kueue cập nhật AdmissionCheck thành Rejected, nhả lại Quota logic ngay lập tức để các Job khác không bị nghẽn queue. TauGrid báo lỗi về CLI watch. - - Bước 12 (Partial Scale Up / Cấp không đủ gang): Chính xác (Nguyên lý Atomic). - Với các AI workload (RayCluster, Gang Training), nếu xin 8 máy mà Cloud chỉ còn 4 máy, lớp best-effort-atomic-scale-up sẽ không nhận 4 máy lẻ loi đó. CA hủy booking, Kueue không nhả suspend: false, release lại quota. Điều này tránh tình trạng "chiếm dụng partial capacity" (chỉ chạy được Ray Head mà 7 Worker thiếu máy). - - Bước 13 (Cấp xong Node nhưng Pod fail / Không kéo được Image): Tinh chỉnh nhỏ về phân cấp quản lý: - - - Khi Node đã được cấp xong nhưng Pod không chạy được (ví dụ ImagePullBackOff hoặc Crash): Lúc này trách nhiệm thuộc về K8s Scheduler & Resilience Layer. - - Kueue đã làm xong nhiệm vụ AdmissionCheck. Khi Pod fail hoặc bị Evicted/Preempted, bộ phận Resilience/Retry policy của TauGrid hoặc Job Controller sẽ tiến hành thu dọn (cleanup), nếu Job bị cancel/delete thì Kueue xóa Workload, dẫn đến ProvisioningRequest bị xoá theo OwnerReference Cluster Autoscaler tự động scale-down 8 máy thừa đó về 0 để tiết kiệm chi phí. - Bản tóm tắt 13 bước này của bạn đã thể hiện chính xác cách thiết kế hạ tầng Kubernetes cho các hệ thống AI/ML lớn hiện đại! [User]    │   ▼ (1. Submit Job/RayJob) [TauGrid (CLI / API)]   │   ▼ (2. Render YAML with suspend:true) [Kueue (Admission Control)]   │   ▼ (3. Create ProvisioningRequest CRD) [Cluster Autoscaler (K8s Controller)]   │   ▼ (4. Call Provision API: EC2 / GCE API) [Cloud Provider (AWS / GCP / Azure)]   │   ▼ (5. Provision VMs -> Node Ready) [Cluster Autoscaler]   │   ▼ (6. Update ProvisioningRequest: Provisioned=True) [Kueue]   │   ▼ (7. Unsuspend Job: suspend=false) [KubeRay / Job Controller]   │   ▼ (8. Generate Pods) [K8s Scheduler]   │   ▼ (9. Bind Pods to Ready GPU Nodes) [TauGrid]   │   ▼ (10. Report Status: RUNNING) [User]


### Gemini

Chào bạn, đây là bản Prompt chi tiết, đầy đủ và mang tính kỹ thuật cao dùng để thiết kế kiến trúc, viết code hoặc định hướng triển khai hệ thống TauGrid tích hợp Kueue, Cluster Autoscaler và ProvisioningRequest cho AI/ML workloads (đặc biệt là RayCluster / distributed training).

Bạn có thể copy trực tiếp đoạn prompt dưới đây để đưa cho AI (hoặc đội ngũ engineering) triển khai:

PROMPT THIẾT KẾ HỆ THỐNG: TauGrid Job Lifecycle & Dynamic Provisioning với Kueue & Cluster Autoscaler

1. Bối cảnh và Mục tiêu (Context & Objective)

Xây dựng một hệ thống quản lý Job / Distributed RayCluster (TauGrid) trên Kubernetes, tích hợp Kubernetes Kueue (làm Admission Control và Queue Management) kết hợp với Cluster Autoscaler (CA) thông qua ProvisioningRequest CRD (autoscaling.x-k8s.io).

Mục tiêu cốt lõi là đảm bảo cơ chế Atomic Scale-Up (cấp đủ cụm GPU mới chạy, tránh tình trạng cấp thiếu một phần gây lãng phí tài nguyên), quản lý Quota logic chặt chẽ, và xử lý dọn dẹp tài nguyên (cleanup/fallback) khi xảy ra lỗi.

2. Chi tiết 10 Bước Luồng Dữ Liệu & Vòng Đời Job (Data Flow & Lifecycle)

Hãy thiết kế và hiện thực hóa các tầng xử lý tuân thủ nghiêm ngặt chuỗi lifecycle sau:

Bước 1 & 2: User Submit & Render YAML

User định nghĩa container image, yêu cầu tài nguyên (CPU, Memory, GPU SKU, số lượng replica), và submit request tới TauGrid (qua CLI / API).

TauGrid Backend tiếp nhận, render ra manifest chuẩn Kubernetes (ví dụ: RayJob hoặc Job) với thuộc tính suspend: true để ngăn chặn Controller tạo Pod ngay lập tức.

Bước 3: Kueue Admission & Quota Reservation

Manifest được gửi tới K8s API Server. Kueue bắt sự kiện qua Admission Controller, kiểm tra Quota logic trên ClusterQueue.

Nếu đủ quota, Kueue tiến hành giữ cọc quota (reserve quota) nhưng giữ nguyên trạng thái suspend: true để KubeRay / Job Controller chưa được phép đụng đến.

Bước 4: Tạo ProvisioningRequest CRD

Sau khi admit vào hàng đợi và giữ cọc quota, Kueue đích thân tạo ra một đối tượng ProvisioningRequest (thuộc CRD nhóm autoscaling.x-k8s.io).

Đối tượng này chứa thông tin cụ thể về số lượng node, loại GPU, và ràng buộc topology cần cấp phát từ cloud provider.

Bước 5 & 6: Cluster Autoscaler gọi Cloud Provider API

Cluster Autoscaler (CA) phát hiện ProvisioningRequest mới, đọc thông số yêu cầu và gọi API của Cloud Provider (ví dụ: Azure VMSS, AWS ASG, GCP MIG) để bật máy ảo (VMs).

Khi các VM được khởi chạy thành công và gia nhập cluster (Node Ready), CA cập nhật trạng thái trên ProvisioningRequest thành Provisioned: True. Kueue theo dõi (watch) trạng thái này từ etcd.

Bước 7: Unsuspend Job

Ngay khi thấy ProvisioningRequest đạt trạng thái Provisioned: True, Kueue tự động cập nhật lại trường suspend: false trên RayJob / Job.

Bước 8 & 9: Pod Generation, Scheduling & Execution

KubeRay Operator / Job Controller ngay lập tức tạo ra các Pod thật (ban đầu ở trạng thái Pending).

kube-scheduler quét các Pod Pending này và gán (bind) chúng vào đúng các GPU Node vừa được cấp phát thông qua cơ chế node selector / node affinity / podSetUpdates.

kubelet trên các node tiến hành kéo container image (ImagePull), khởi chạy container và báo cáo trạng thái Running về K8s API Server (lưu trữ toàn bộ tại etcd).

Bước 10: TauGrid Observer & Reporting

TauGrid đóng vai trò quan sát (Observer / Workflow UI), thực hiện poll/watch trạng thái từ K8s API Server thông qua TauCluster controller để cập nhật trực quan tiến trình cho user trên terminal hoặc giao diện (Capacity Provisioning → Pod Scheduling → Running).

3. Các Kịch Bản Ngoại Lệ & Xử Lý Lỗi (Exception Handling & Fallbacks)

Hệ thống phải xử lý triệt để 3 kịch bản lỗi sau theo nguyên lý Atomic & Fail-safe:

Kịch bản 1: Cloud Hết Máy / Timeout (ProvisioningRequest Thất Bại)

Tình huống: Cloud Provider báo lỗi Out of Capacity hoặc hết thời gian chờ (Bounded Timeout). CA đánh dấu ProvisioningRequest là Failed hoặc CapacityIsNotFound.

Hành động: Kueue cập nhật AdmissionCheck thành Rejected, nhả ngay lập tức Quota logic đã giữ cọc ở Bước 3 để các Job khác trong hàng đợi có thể sử dụng. TauGrid báo lỗi chi tiết về cho user.

Kịch bản 2: Cấp Thiếu Phần Trăm Tài Nguyên (Partial Scale-Up)

Tình huống: User xin 8 máy GPU cho mô hình Distributed Training, nhưng Cloud Autoscaler chỉ cấp được 4 máy.

Hành động: Áp dụng nguyên tắc Atomic Scale-Up. Hệ thống từ chối nhận 4 máy lẻ loi để tránh lãng phí và kẹt tài nguyên (tránh trường hợp chỉ chạy được Ray Head mà thiếu 7 Worker). CA hủy booking, Kueue giữ nguyên trạng thái suspend: true, đồng thời release toàn bộ Quota.

Kịch bản 3: Lỗi Kéo Image / Pod Crash Sau Khi Đã Cấp Máy

Tình huống: Node đã được cấp thành công, nhưng Pod gặp lỗi khi khởi chạy (ví dụ: ImagePullBackOff, CrashLoopBackOff).

Hành động: Lúc này hạ tầng đã hoàn thành nhiệm vụ cấp phát. Trách nhiệm thuộc về tầng Resilience & Retry Policy của TauGrid hoặc Job Controller. Khi Job/Workload bị user hủy hoặc quá số lần retry bị xóa, Kueue sẽ xóa Workload, dẫn đến ProvisioningRequest bị xóa theo cơ chế OwnerReference, kích hoạt Cluster Autoscaler tự động scale-down số máy thừa về 0 để tối ưu chi phí.

4. Yêu Cầu Đầu Ra Từ Kỹ Sư / AI Triển Khai

Dựa trên kiến trúc chuẩn trên, hãy cung cấp:

Ví dụ các file Manifest YAML minh họa (Kueue ClusterQueue, ProvisioningRequest CRD mẫu, và một RayJob tích hợp suspend: true).

Sơ đồ kiến trúc hoặc mã giả (Pseudo-code / Controller Logic) mô tả cách TauGrid sync trạng thái từ K8s API Server để báo cáo chính xác cho User.

Các điểm cần lưu ý (Best practices) về cấu hình timeout và resource cleanup để tránh tình trạng "treo tài nguyên mồ côi" (orphan resources) trên Cloud.

Bạn có muốn điều chỉnh thêm chi tiết nào về phần Cloud Provider (ví dụ tối ưu riêng cho Azure AKS hay AWS EKS) trước khi bắt tay vào viết code chi tiết không?


---

## 2. User

hay viet tieng anh, tham khao tai lieu nay https://github.com/Flashback29/taugrid/blob/1d7541252c21bd46c22361e266c7e4504bd7fd0c/docs/design/capacity-aware-kueue-admission.md


### Gemini

Here is a comprehensive technical prompt written in English, structured specifically for designing, implementing, or reviewing the system architecture based on the specifications in the Capacity-Aware Kueue Admission design document for TauGrid.

SYSTEM DESIGN PROMPT: TauGrid Capacity-Aware Kueue Admission & Dynamic Provisioning Lifecycle

1. Context & Architecture Goal

Design and implement the workload scheduling, admission control, and dynamic capacity provisioning flow for TauGrid by integrating Kubernetes Kueue with Cluster Autoscaler (CA) via the ProvisioningRequest CRD (autoscaling.x-k8s.io).

The core goal is to guarantee Atomic Scale-Up (gang-scheduling behavior at the node level where a distributed ML job like a RayCluster or RayJob only runs if 100% of its required GPU nodes are successfully provisioned), enforce strict quota reservation, and automate resource fallback/cleanup upon failures.

2. End-to-End Data Flow & Workload Lifecycle (10-Step Pipeline)

The system must implement the following multi-stage lifecycle across TauGrid, Kueue, Cluster Autoscaler, and Kubernetes Control Plane:

Step 1: Request Submission & Rendering

User Flow: The user defines the container image, resource demands (CPU, Memory, GPU SKU, replica count), and submits the execution request via TauGrid CLI/API.

TauGrid Action: TauGrid renders the corresponding workload manifest (e.g., RayJob, RayCluster, or standard K8s Job) and sets suspend: true by default to prevent downstream controllers from generating Pods prematurely.

Step 2 & 3: Kueue Admission Control & Quota Reservation

API Ingestion: The manifest is submitted to the Kubernetes API Server.

Kueue Ingestion: Kueue's Admission Controller interceptors validate the request against the defined ClusterQueue logical quotas.

Quota Holding: If sufficient logical quota is available, Kueue reserves the quota while keeping suspend: true, ensuring no compute controllers spawn unbacked Pods.

Step 4: ProvisioningRequest Creation

CRD Instantiation: Upon acquiring logical quota, Kueue generates a custom ProvisioningRequest resource (under autoscaling.x-k8s.io).

Request Payload: The object defines exact node counts, instance types, accelerator capabilities, and topology requirements necessary for the cluster to scale.

Step 5 & 6: Infrastructure Scale-Up & Readiness Signaling

CA Detection: The Cluster Autoscaler watches for new ProvisioningRequest objects and evaluates the required capacity.

Cloud Scaling: CA communicates with Cloud Provider APIs (e.g., Azure VMSS, AWS ASG, GCP MIG) to trigger VM provisioning.

Node Integration: Once the new nodes join the cluster and transition to NodeReady, CA sets Provisioned: True in the ProvisioningRequest status. Kueue tracks this status update from etcd.

Step 7: Workload Un-suspension

State Transition: Upon observing Provisioned: True, Kueue updates the original workload manifest by setting suspend: false.

Step 8 & 9: Pod Scheduling & Execution

Pod Generation: The respective workload controller (e.g., KubeRay Operator or Job Controller) creates the concrete Pod instances (initially in Pending state).

Binding: kube-scheduler identifies the pending Pods and binds them to the newly provisioned GPU nodes using nodeSelector, nodeAffinity, and podSetUpdates.

Execution: kubelet on each node pulls the required container image, boots the runtime containers, and writes the Running status back to etcd.

Step 10: TauGrid Lifecycle Reporting

Observer Controller: TauGrid's status observer polls and watches etcd via the TauCluster controller to stream real-time progress to the end user:

Capacity Provisioning⟶Pod Scheduling⟶Container Running

3. Failure Modes, Resilience & Fallback Protocols

Implement fail-safe mechanisms for the following three failure scenarios:

```text
+-----------------------------------------------------------------------------------+
|                                  FAILURE MATRIX                                   |
+--------------------------+-------------------------------+------------------------+
| Scenario                 | Trigger Condition             | Resolution Action      |
+--------------------------+-------------------------------+------------------------+
| 1. Provisioning Failure  | CapacityIsNotFound / Timeout  | Reject AdmissionCheck, |
|                          |                               | Release Kueue Quota    |
+--------------------------+-------------------------------+------------------------+
| 2. Partial Scale-Up      | Cloud provides < 100% capacity| Atomic Cancel, Release |
|                          |                               | ProvisioningRequest    |
+--------------------------+-------------------------------+------------------------+
| 3. Pod Execution Error   | ImagePullBackOff / CrashLoop  | Resilience Retry Policy|
|                          |                               | GC via OwnerReference |
+--------------------------+-------------------------------+------------------------+
```

1. Cloud Capacity Exhaustion / Provisioning Timeout

Condition: Cloud Provider returns out-of-capacity errors or exceeds the bounding timeout duration. CA marks ProvisioningRequest as Failed or CapacityIsNotFound.

Action: Kueue converts the AdmissionCheck state to Rejected and immediately releases the reserved logical quota back to the ClusterQueue for adjacent queued workloads.

2. Partial Scale-Up (Atomic Scale-Up Violation)

Condition: Workload requests N nodes (e.g., 8 GPU instances), but the cloud autoscaler can only provision M<N nodes (e.g., 4 GPU instances).

Action: Enforce strict Atomic Scale-Up. Partial capacity allocation is rejected to prevent resource starvation (e.g., Ray Head node running without its required Worker set). CA cancels the partial booking, Kueue keeps suspend: true, and all logical quota is returned.

3. Post-Provisioning Pod Failures (Runtime Errors)

Condition: Nodes are provisioned, but Pod execution fails due to runtime errors (e.g., ImagePullBackOff, CrashLoopBackOff).

Action: Handled by TauGrid's Resilience & Retry Policy layer. If the job exceeds retry limits or is cancelled by the user, Kueue deletes the parent Workload. K8s garbage collection cleans up the ProvisioningRequest via OwnerReference, triggering Cluster Autoscaler to automatically scale down unused nodes to 0.

4. Expected Deliverables & Engineering Tasks

When executing this design, ensure the following engineering artifacts are generated:

Kubernetes Manifest Schemas:

Sample Kueue ClusterQueue and LocalQueue CRD configurations.

ProvisioningRequest CRD specification with capacity constraints.

Target workload CRD (e.g., RayJob) demonstrating default suspend: true integration.

Controller Logic / Pseudo-Code:

State machine for TauGrid observer service reconciling ProvisioningRequest and Workload statuses from the K8s API Server.

Configuration & Timeout Guidelines:

Bounded timeout thresholds for ProvisioningRequest lifecycle to avoid stranded/orphan cloud resources.


---

## 3. User

ProvisioningRequest code yaml


### Gemini

Here are the core configuration YAML manifests required to set up and trigger a ProvisioningRequest via Kueue and Cluster Autoscaler for dynamic AI/ML workloads (such as a distributed RayJob requiring GPU nodes).

1. AdmissionCheck Configuration (ProvisioningRequestConfig)

This object defines how Kueue interacts with the Cluster Autoscaler using the atomic scale-up provisioning class.

```yaml
apiVersion: kueue.x-k8s.io/v1beta1
kind: AdmissionCheck
metadata:
  name: provreq-check
spec:
  controllerName: kueue.x-k8s.io/provisioning-request
  parameters:
    apiGroup: kueue.x-k8s.io
    kind: ProvisioningRequestConfig
    name: default-config
---
apiVersion: kueue.x-k8s.io/v1beta1
kind: ProvisioningRequestConfig
metadata:
  name: default-config
spec:
  # Use best-effort atomic scale-up to ensure gang scheduling of all required GPU nodes
  provisioningClassName: best-effort-atomic-scale-up.autoscaling.x-k8s.io
  managedResources:
    - nvidia.com/gpu
  retryStrategy:
    backoffLimitCount: 3
    backoffBaseSeconds: 60
    backoffMaxSeconds: 1800
  podSetUpdates:
    - appendNodeSelectorsToPods:
        - key: "kubernetes.io/hostname"
```

2. Target Workload Manifest (RayJob / Job) with suspend: true

When this workload is submitted to TauGrid, Kueue intercepts it, checks the logical quota, and automatically generates a ProvisioningRequest.

```yaml
apiVersion: ray.io/v1
kind: RayJob
metadata:
  name: taugrid-distributed-training
  namespace: default
  labels:
    kueue.x-k8s.io/queue-name: user-queue
  annotations:
    # Pass custom parameters (like timeout) to the underlying ProvisioningRequest via Kueue
    provreq.kueue.x-k8s.io/ValidUntilSeconds: "900" 
spec:
  # CRITICAL: Must be true initially so pods aren't spawned before nodes exist
  suspend: true
  rayClusterConfig:
    rayVersion: "2.35.0"
    headGroupSpec:
      rayStartParams:
        dashboard-host: "0.0.0.0"
      template:
        spec:
          containers:
            - name: ray-head
              image: rayproject/ray:2.35.0-py310
              resources:
                cpu: "4"
                memory: "16Gi"
    workerGroupSpecs:
      - groupName: gpu-workers
        replicas: 4
        minReplicas: 4
        maxReplicas: 4
        rayStartParams: {}
        template:
          spec:
            containers:
              - name: ray-worker
                image: rayproject/ray:2.35.0-py310-gpu
                resources:
                  limits:
                    cpu: "8"
                    memory: "32Gi"
                    nvidia.com/gpu: "2"
                  requests:
                    cpu: "8"
                    memory: "32Gi"
                    nvidia.com/gpu: "2"
            tolerations:
              - key: "nvidia.com/gpu"
                operator: "Exists"
                effect: "NoSchedule"
```

3. Automatically Generated ProvisioningRequest (Reference Example)

Note: You typically do not write this file manually. Once Kueue evaluates the Job above, its controller automatically instantiates this CRD for the Cluster Autoscaler to pick up.

```yaml
apiVersion: autoscaling.x-k8s.io/v1
kind: ProvisioningRequest
metadata:
  generateName: taugrid-distributed-training-
  namespace: default
  ownerReferences:
    - apiVersion: kueue.x-k8s.io/v1beta1
      kind: Workload
      name: workload-taugrid-distributed-training
      controller: true
      blockOwnerDeletion: true
spec:
  provisioningClassName: best-effort-atomic-scale-up.autoscaling.x-k8s.io
  parameters:
    ValidUntilSeconds: "900"
  podSets:
    - count: 1
      podTemplateRef:
        name: workload-taugrid-distributed-training-pod-template
        apiVersion: v1
        kind: PodTemplate
```

Sources

ProvisioningRequest | Kueue - Kubernetes


---

## 4. User

Mình phải tạo thêm mấy controller ở đây


### Gemini

Để hệ thống TauGrid tích hợp đầy đủ vòng đời (Lifecycle) từ Quota Reservation → ProvisioningRequest → Cluster Autoscaler → Unsuspend Workload hoạt động trơn tru, bạn không cần viết toàn bộ các controller từ đầu vì Kubernetes và các hệ sinh thái đã có sẵn phần lớn.

Tuy nhiên, để kết nối TauGrid với Kueue và xử lý đúng logic tự động hóa, bạn sẽ cần thiết kế hoặc cấu hình 1 Controller chính (TauGrid-specific Controller) và tận dụng các Controller có sẵn. Cụ thể chi tiết như sau:

1. Các Controller Có Sẵn (Không cần viết code)

Kueue Controller (kueue-controller-manager): Đã có sẵn nhiệm vụ lắng nghe Workload, check quota trên ClusterQueue, và tự động tạo ra ProvisioningRequest (Bước 4) khi đủ điều kiện.

Cluster Autoscaler (CA): Đã có sẵn controller chuyên track các đối tượng ProvisioningRequest (CRD của autoscaling.x-k8s.io), gọi Cloud Provider API để bật máy (Bước 5), và cập nhật trạng thái Provisioned: True (Bước 6).

KubeRay Operator / Job Controller: Đã có sẵn nhiệm vụ quản lý RayJob / Job, khi thấy suspend: false sẽ tự động tạo Pod (Bước 7 & 8).

2. Controller Bắt Buộc Cần Viết / Tích Hợp Thêm (TauGrid-specific)

Bạn sẽ cần 01 TauGrid Custom Controller (hoặc mở rộng TauCluster Controller hiện tại) để làm cầu nối quan sát và điều phối trải nghiệm cho User:

TauGrid Observer & Workflow Controller

Nhiệm vụ chính: Watch các trạng thái của K8s API Server (Workload, ProvisioningRequest, RayJob, và Pod) để phản ánh đúng trạng thái thực tế lên CLI/UI của TauGrid (Bước 10).

Các state cần handle trong Controller loop:

Pending/Admission: Báo cho user biết Job đang nằm trong hàng đợi Kueue (đang giữ cọc quota).

Provisioning: Phát hiện ProvisioningRequest vừa được tạo, thông báo hệ thống đang gọi Cloud API để bật GPU nodes.

Scheduling: Phát hiện Provisioned: True và suspend: false, báo hiệu Pod đang chuẩn bị chạy.

Running / Failed: Bắt sự kiện lỗi (như ProvisioningRequest timeout, CapacityIsNotFound, hoặc ImagePullBackOff) để thực hiện cơ chế thông báo lỗi hoặc trigger dọn dẹp.

3. Tóm lại: Bạn cần code thêm những gì?

Thành phần

Trạng thái

Ghi chú

Kueue Manager

Có sẵn

Chỉ cần cấu hình YAML (AdmissionCheck, ProvisioningRequestConfig).

Cluster Autoscaler

Có sẵn

Bật tính năng hỗ trợ ProvisioningRequest trên Cluster Autoscaler của Cluster.

TauGrid Status Controller

Cần viết thêm

Controller dạng lightweight watch các resource K8s để stream log/status về cho CLI/UI của TauGrid.


---

## 5. User

Nên viết thêm hay bỏ vào cái nào có sẵn?


### Gemini

Tuyệt đối không cần viết thêm một Kubernetes Controller (Operator) mới hoàn toàn từ đầu.

Thay vào đó, bạn nên tích hợp logic theo dõi (Watcher) trực tiếp vào TauGrid Backend / API Server hiện có, hoặc mở rộng controller sẵn có của TauGrid (nếu có).

Tại sao không nên viết một K8s Controller mới?

Bản chất của Controller là Reconcile (Đồng bộ trạng thái): K8s Controller sinh ra để điều phối trạng thái hạ tầng (tạo/sửa/xoá resource). Trong khi đó, việc báo cáo tiến trình cho User qua CLI/UI thực chất chỉ là hành động Đọc và Lắng nghe (Read & Watch), không cần can thiệp hay ghi đè state lên Kubernetes.

Tránh cồng kềnh hệ thống: Viết một Controller mới đồng nghĩa với việc bạn phải quản lý thêm Leader Election, RBAC phức tạp, Deployment riêng cho controller trong cluster, dễ gây rối và tốn tài nguyên không cần thiết.

Giải pháp tối ưu: Xây dựng Watcher/Informer bên trong TauGrid Backend

TauGrid Backend (hoặc CLI) chỉ cần sử dụng thư viện client-go của Kubernetes để thực hiện cơ chế Watch trực tiếp các đối tượng sau:

Watch Workload (thuộc group kueue.x-k8s.io): Để biết Job đang xếp hàng ở bước nào, đã được cấp quota hay chưa (Admitted: True/False).

Watch ProvisioningRequest (thuộc group autoscaling.x-k8s.io): Để biết Cluster Autoscaler đã gọi Cloud API đến đâu (Provisioned: True/False, có bị lỗi CapacityIsNotFound hay không).

Watch RayJob / Job: Để biết Pod đã được bật lên hay chưa, trạng thái Pod là Pending, ImagePullBackOff hay Running.

Luồng xử lý mẫu trong TauGrid Backend (Pseudo-code)

```go
// TauGrid Backend lắng nghe sự kiện thay đổi trạng thái từ K8s API Server
watcher, err := clientset.AutoscalingV1().ProvisioningRequests(namespace).Watch(ctx, metav1.ListOptions{
    LabelSelector: "taugrid.io/job-id=" + jobID,
})

for event := range watcher.ResultChan() {
    pr := event.Object.(*autoscalingv1.ProvisioningRequest)
    if isProvisioned(pr) {
        fmt.Println("--> Cloud đã bật xong GPU nodes. Đang chuyển sang tạo Pod...")
    } else if isFailed(pr) {
        fmt.Println("--> Lỗi: Cloud hết máy hoặc timeout. Đang giải phóng quota...")
    }
}
```

Bằng cách này, TauGrid Backend vừa giữ được sự gọn nhẹ, vừa bám sát thời gian thực (real-time) để trả kết quả mượt mà về cho User trên Terminal/UI mà không phải viết thêm bất kỳ K8s Operator nào phức tạp.


---

## 6. User

is there any trade off we made in this design


### Gemini

Yes, adopting this architecture involves several key architectural and operational trade-offs. While it provides robust resource safety and cost control, it comes with specific engineering compromises.

1. Time-to-Start (Latency) vs. Resource Safety (Atomic Scale-Up)

The Trade-off: We chose strict atomic scale-up (all-or-nothing: waiting for 100% of the required GPU nodes to be provisioned before starting the job).

The Impact:

Pro: Prevents wasted resources and half-baked clusters (e.g., a Ray Head node running without its required Workers).

Con: Higher job startup latency. If the cloud provider takes 5–10 minutes to spin up specialized GPU nodes, or if 1 out of 8 nodes fails to provision, the entire job must wait or fail, slowing down immediate execution.

2. Native Ecosystem Extensibility vs. Debugging Complexity

The Trade-off: We leaned heavily on native Kubernetes components (Kueue + Cluster Autoscaler + ProvisioningRequest CRD) instead of building a custom, closed-loop orchestrator inside TauGrid.

The Impact:

Pro: Highly standardized, production-ready, and aligns with modern cloud-native standards.

Con: Debugging becomes multi-layered. When a job gets stuck, troubleshooting requires inspecting a chain of disparate resources: TauGrid state → Kueue Workload → AdmissionCheck → ProvisioningRequest → Cluster Autoscaler logs → Cloud Provider API limits. If something fails silently, tracing the root cause takes more effort.

3. Strict Quota Locking vs. Queue Throughput

The Trade-off: Kueue reserves logical quota and holds it while the ProvisioningRequest is out waiting for the cloud provider to provision physical infrastructure.

The Impact:

Pro: Prevents cluster over-commitment and race conditions where multiple jobs fight for the same incoming capacity.

Con: If a large distributed job gets stuck in provisioning (e.g., due to cloud capacity bottlenecks or timeouts), its reserved quota remains locked, potentially stalling downstream smaller jobs unless sophisticated borrowing/preemption policies are configured in Kueue's ClusterQueue.

4. Cloud Provider Coupling

The Trade-off: Delegating the heavy lifting to Cluster Autoscaler and ProvisioningRequest means relying heavily on the cloud provider's underlying autoscaling APIs (e.g., Azure VMSS, AWS ASG).

The Impact:

Pro: No need for TauGrid to implement custom cloud-calling logic.

Con: Your system's reliability is bounded by the cloud provider's API rate limits, regional GPU availability, and potential out-of-capacity errors (CapacityIsNotFound).

Summary

Ultimately, this design trades immediate execution speed and architectural simplicity for cost predictability, resource safety, and native Kubernetes alignment. For AI/ML workloads involving expensive GPUs, this is generally the correct trade-off to make.
