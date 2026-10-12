// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	KindDataFetch = "DataFetch"

	// LoadStrategy values
	LoadStrategyHotLoad     = "HotLoad"
	LoadStrategyStreamMount = "StreamMount"

	// SyncStrategy values
	SyncStrategyAsynchronous = "Asynchronous"

	// SharedState type values
	SharedStateTypeHighThroughputNFS = "HighThroughputNFS"

	// DataFetch phase values
	DataFetchPhasePending      = "Pending"
	DataFetchPhaseProvisioning = "Provisioning"
	DataFetchPhaseReady        = "Ready"
	DataFetchPhaseDegraded     = "Degraded"

	// Condition type keys specific to DataFetch
	ConditionDatasetsReady    = "DatasetsReady"
	ConditionCheckpointsReady = "CheckpointsReady"
	ConditionSharedStateReady = "SharedStateReady"
)

// BlobSource references an Azure Blob Storage location used as a data origin or target.
type BlobSource struct {
	// StorageAccount is the Azure Storage account name.
	// +kubebuilder:validation:MinLength=3
	StorageAccount string `json:"storageAccount"`

	// Container is the blob container name.
	// +kubebuilder:validation:MinLength=1
	Container string `json:"container"`

	// Prefix is an optional object-key prefix (logical folder path).
	Prefix string `json:"prefix,omitempty"`

	// CredentialSecretRef optionally points to a Secret that holds a storage
	// account key or SAS token. When omitted the sidecar / init container use
	// workload identity (AZURECLOUDCONFIG / AZURE_CLIENT_ID).
	CredentialSecretRef *SecretKeyRef `json:"credentialSecretRef,omitempty"`
}

// SecretKeyRef identifies a key within a Kubernetes Secret.
type SecretKeyRef struct {
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`
	// +kubebuilder:validation:MinLength=1
	Key string `json:"key"`
}

// DatasetSpec describes a training dataset to be made available to training pods.
type DatasetSpec struct {
	// Name is the unique identifier for this dataset within the DataFetch.
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`

	// Source is the Azure Blob Storage location of the raw dataset.
	Source BlobSource `json:"source"`

	// LoadStrategy controls how data is delivered to the training pod.
	//   HotLoad     – streams data from Azure Blob to node-local NVMe/SSD via an
	//                 init container before the training job starts. Gives maximum
	//                 read IOPS at zero GPU-idle time.
	//   StreamMount – falls back to a BlobFuse2 CSI network mount. Used
	//                 automatically by the controller when HotLoad would exceed
	//                 available NVMe capacity (see NVMeCapacityGiB).
	// +kubebuilder:validation:Enum=HotLoad;StreamMount
	// +kubebuilder:default=HotLoad
	LoadStrategy string `json:"loadStrategy,omitempty"`

	// MountPath is the filesystem path exposed inside training containers.
	// +kubebuilder:validation:MinLength=1
	MountPath string `json:"mountPath"`

	// SizeGB is the estimated uncompressed dataset size in GiB. Used by the
	// controller to decide whether HotLoad is feasible on the target node class.
	// +kubebuilder:validation:Minimum=1
	SizeGB int64 `json:"sizeGB,omitempty"`
}

// CheckpointSpec configures how model checkpoints are staged locally and
// synchronised to durable Azure Blob Storage.
type CheckpointSpec struct {
	// LocalPath is the directory inside the training container where the training
	// process writes checkpoint files (e.g. /checkpoints).
	// +kubebuilder:validation:MinLength=1
	LocalPath string `json:"localPath"`

	// Remote is the Azure Blob Storage target for asynchronous checkpoint upload.
	Remote BlobSource `json:"remote"`

	// SyncStrategy controls the persistence mechanism.
	// Only Asynchronous is supported: the sidecar container monitors LocalPath
	// via inotify, detects .done marker files, and streams the corresponding
	// checkpoint to Remote without blocking the training loop.
	// +kubebuilder:validation:Enum=Asynchronous
	// +kubebuilder:default=Asynchronous
	SyncStrategy string `json:"syncStrategy,omitempty"`

	// RetentionCount is the maximum number of checkpoint sets to keep on local
	// disk before the sidecar evicts the oldest ones post-upload.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=2
	RetentionCount int `json:"retentionCount,omitempty"`

	// UploadRetryMaxAttempts caps the sidecar's retry loop when Azure Blob
	// upload fails due to transient network partitions. Each attempt uses
	// exponential backoff (base 2 s, cap 5 min). See Section 5 of the design.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=10
	UploadRetryMaxAttempts int `json:"uploadRetryMaxAttempts,omitempty"`
}

// SharedStateSpec configures a high-throughput ReadWriteMany volume for
// distributed training coordination metadata (Ray / PyTorch Elastic state).
type SharedStateSpec struct {
	// Type must be HighThroughputNFS. Reserved for future storage types.
	// +kubebuilder:validation:Enum=HighThroughputNFS
	// +kubebuilder:default=HighThroughputNFS
	Type string `json:"type,omitempty"`

	// MountPath is the filesystem path inside training containers.
	// +kubebuilder:validation:MinLength=1
	MountPath string `json:"mountPath"`

	// StorageClassName overrides the provisioned storage class.
	// Defaults to azurefile-premium-nfs (Azure Files Premium with NFS 4.1).
	StorageClassName string `json:"storageClassName,omitempty"`

	// Capacity is the PVC storage request (e.g. "100Gi").
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:default="100Gi"
	Capacity string `json:"capacity,omitempty"`
}

// DataFetchSpec is the desired state of a DataFetch resource.
type DataFetchSpec struct {
	// Workspace is the name of the TauWorkspace this DataFetch is scoped to.
	// +kubebuilder:validation:MinLength=1
	Workspace string `json:"workspace"`

	// Datasets lists one or more training datasets to provision.
	// +kubebuilder:validation:MinItems=1
	Datasets []DatasetSpec `json:"datasets,omitempty"`

	// Checkpoints configures checkpoint staging and async upload for this workload.
	Checkpoints *CheckpointSpec `json:"checkpoints,omitempty"`

	// SharedState configures the shared ReadWriteMany NFS volume for distributed
	// training coordination.
	SharedState *SharedStateSpec `json:"sharedState,omitempty"`

	// NVMeNodeSelector constrains which nodes are considered eligible for
	// HotLoad caching. An empty map matches any node.
	NVMeNodeSelector map[string]string `json:"nvmeNodeSelector,omitempty"`

	// NVMeCapacityGiB is the assumed per-node NVMe capacity for HotLoad
	// feasibility checks. If the total SizeGB of all HotLoad datasets exceeds
	// this value the controller automatically downgrades them to StreamMount.
	// Defaults to 3000 GiB (3 TB, typical for Standard_NC* / ND* v4 nodes).
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=3000
	NVMeCapacityGiB int64 `json:"nvmeCapacityGiB,omitempty"`
}

// DataFetchDatasetStatus captures the per-dataset provisioning outcome.
type DataFetchDatasetStatus struct {
	// Name matches DatasetSpec.Name.
	Name string `json:"name"`
	// Phase is Provisioning or Ready.
	Phase string `json:"phase,omitempty"`
	// EffectiveLoad is the load strategy actually applied (may differ from spec
	// when the controller fell back from HotLoad to StreamMount).
	EffectiveLoad string `json:"effectiveLoad,omitempty"`
	// PVCName is set when EffectiveLoad is StreamMount and a PVC was created.
	PVCName string `json:"pvcName,omitempty"`
	// Message carries a human-readable explanation for any fallback or error.
	Message string `json:"message,omitempty"`
}

// DataFetchStatus is the observed state of a DataFetch resource.
type DataFetchStatus struct {
	// +kubebuilder:validation:Enum=Pending;Provisioning;Ready;Degraded
	Phase string `json:"phase,omitempty"`

	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Datasets contains per-dataset status after the last reconcile.
	Datasets []DataFetchDatasetStatus `json:"datasets,omitempty"`

	// SharedStatePVCName is the name of the Azure Files NFS PVC provisioned for
	// shared state, when spec.sharedState is set.
	SharedStatePVCName string `json:"sharedStatePVCName,omitempty"`

	// CheckpointPVCName is the name of the Premium SSD PVC provisioned for
	// checkpoint staging, when spec.checkpoints is set.
	CheckpointPVCName string `json:"checkpointPVCName,omitempty"`

	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:path=datafetches,singular=datafetch,scope=Namespaced,shortName=di
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Workspace",type=string,JSONPath=`.spec.workspace`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
type DataFetch struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   DataFetchSpec   `json:"spec,omitempty"`
	Status DataFetchStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type DataFetchList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []DataFetch `json:"items"`
}
