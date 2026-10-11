// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	tauv1alpha1 "github.com/Azure/taugrid/controllers/tau-core/api/v1alpha1"
)

const (
	// AnnotationDataIngestion is the pod annotation that links a pod to a
	// DataIngestion resource: tau.azure.com/data-ingestion: <name>
	AnnotationDataIngestion = "tau.azure.com/data-ingestion"

	// Image references — override via controller flags for air-gapped clusters.
	DefaultInitImage    = "ghcr.io/azure/taugrid/tau-init:latest"
	DefaultSidecarImage = "ghcr.io/azure/taugrid/tau-sidecar:latest"
)

// StoragePodInjector is a mutating admission webhook that injects init and
// sidecar containers into pods annotated with tau.azure.com/data-ingestion.
//
// Register with the controller-runtime webhook server in main.go:
//
//	mgr.GetWebhookServer().Register("/mutate-pod-storage",
//	    &webhook.Admission{Handler: &StoragePodInjector{Client: mgr.GetClient()}})
type StoragePodInjector struct {
	Client     client.Client
	InitImage  string
	SidecarImg string
	decoder    admission.Decoder
}

func (i *StoragePodInjector) InjectDecoder(d admission.Decoder) error {
	i.decoder = d
	return nil
}

// Handle implements admission.Handler. It reads the DataIngestion referenced
// by the pod annotation and calls InjectStorageContainers to mutate the pod.
func (i *StoragePodInjector) Handle(ctx context.Context, req admission.Request) admission.Response {
	logger := log.FromContext(ctx)

	pod := &corev1.Pod{}
	if err := i.decoder.Decode(req, pod); err != nil {
		return admission.Errored(http.StatusBadRequest, err)
	}

	diName := pod.Annotations[AnnotationDataIngestion]
	if diName == "" {
		return admission.Allowed("no data-ingestion annotation")
	}

	var di tauv1alpha1.DataIngestion
	if err := i.Client.Get(ctx, types.NamespacedName{
		Name:      diName,
		Namespace: req.Namespace,
	}, &di); err != nil {
		logger.Error(err, "failed to fetch DataIngestion", "name", diName)
		return admission.Errored(http.StatusInternalServerError, err)
	}

	initImg := i.InitImage
	if initImg == "" {
		initImg = DefaultInitImage
	}
	sidecarImg := i.SidecarImg
	if sidecarImg == "" {
		sidecarImg = DefaultSidecarImage
	}

	if err := InjectStorageContainers(pod, &di, initImg, sidecarImg); err != nil {
		return admission.Errored(http.StatusInternalServerError, err)
	}

	marshalled, err := json.Marshal(pod)
	if err != nil {
		return admission.Errored(http.StatusInternalServerError, err)
	}
	return admission.PatchResponseFromRaw(req.Object.Raw, marshalled)
}

// InjectStorageContainers mutates pod in-place by injecting:
//   - One init container per HotLoad dataset (streams data to local NVMe)
//   - One checkpoint sidecar container (async upload via inotify + .done markers)
//   - Appropriate volumes and volume mounts for all app containers
//
// This function is also callable outside the webhook (e.g. from a controller
// that directly manages JobSets or RayJobs).
func InjectStorageContainers(pod *corev1.Pod, di *tauv1alpha1.DataIngestion, initImg, sidecarImg string) error {
	if di == nil {
		return nil
	}

	// Build effective-load map from status (reflects any HotLoad→StreamMount fallbacks).
	effectiveLoad := make(map[string]string, len(di.Status.Datasets))
	for _, ds := range di.Status.Datasets {
		effectiveLoad[ds.Name] = ds.EffectiveLoad
	}

	var (
		extraVolumes        []corev1.Volume
		extraInitContainers []corev1.Container
		extraSidecar        *corev1.Container
	)

	// ── Datasets ─────────────────────────────────────────────────────────────
	for _, ds := range di.Spec.Datasets {
		load := ds.LoadStrategy
		if override, ok := effectiveLoad[ds.Name]; ok && override != "" {
			load = override
		}

		switch load {
		case tauv1alpha1.LoadStrategyHotLoad:
			volName := fmt.Sprintf("dataset-%s-nvme", sanitizeName(ds.Name))
			extraVolumes = append(extraVolumes, hotLoadVolume(volName, ds.SizeGB))
			extraInitContainers = append(extraInitContainers, datasetInitContainer(ds, volName, initImg))

			// Mount the cached dataset read-only into every app container.
			for idx := range pod.Spec.Containers {
				pod.Spec.Containers[idx].VolumeMounts = append(
					pod.Spec.Containers[idx].VolumeMounts,
					corev1.VolumeMount{Name: volName, MountPath: ds.MountPath, ReadOnly: true},
				)
			}

		case tauv1alpha1.LoadStrategyStreamMount:
			// StreamMount is handled by an inline BlobFuse2 CSI ephemeral volume.
			volName := fmt.Sprintf("dataset-%s-blobfuse", sanitizeName(ds.Name))
			extraVolumes = append(extraVolumes, blobFuseVolume(volName, ds.Source))
			for idx := range pod.Spec.Containers {
				pod.Spec.Containers[idx].VolumeMounts = append(
					pod.Spec.Containers[idx].VolumeMounts,
					corev1.VolumeMount{Name: volName, MountPath: ds.MountPath, ReadOnly: true},
				)
			}
		}
	}

	// ── Checkpoints ──────────────────────────────────────────────────────────
	if di.Spec.Checkpoints != nil && di.Status.CheckpointPVCName != "" {
		volName := "checkpoint-staging"
		extraVolumes = append(extraVolumes, corev1.Volume{
			Name: volName,
			VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
					ClaimName: di.Status.CheckpointPVCName,
				},
			},
		})

		// Mount checkpoint volume into every app container so training writes land on PVC.
		for idx := range pod.Spec.Containers {
			pod.Spec.Containers[idx].VolumeMounts = append(
				pod.Spec.Containers[idx].VolumeMounts,
				corev1.VolumeMount{Name: volName, MountPath: di.Spec.Checkpoints.LocalPath},
			)
		}

		// Sidecar watches the same path and streams .done-marked files to Azure Blob.
		sc := checkpointSidecar(di.Spec.Checkpoints, volName, sidecarImg)
		extraSidecar = &sc
	}

	// ── Shared State ─────────────────────────────────────────────────────────
	if di.Spec.SharedState != nil && di.Status.SharedStatePVCName != "" {
		volName := "shared-state-nfs"
		extraVolumes = append(extraVolumes, corev1.Volume{
			Name: volName,
			VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
					ClaimName: di.Status.SharedStatePVCName,
				},
			},
		})
		for idx := range pod.Spec.Containers {
			pod.Spec.Containers[idx].VolumeMounts = append(
				pod.Spec.Containers[idx].VolumeMounts,
				corev1.VolumeMount{Name: volName, MountPath: di.Spec.SharedState.MountPath},
			)
		}
	}

	// ── Assemble pod spec mutations ───────────────────────────────────────────
	// Init containers run sequentially before app containers; prepend so that
	// user-defined init containers run after data is already available.
	pod.Spec.InitContainers = append(extraInitContainers, pod.Spec.InitContainers...)

	// Sidecar runs concurrently with app containers; append so it doesn't shift indices.
	if extraSidecar != nil {
		pod.Spec.Containers = append(pod.Spec.Containers, *extraSidecar)
	}

	pod.Spec.Volumes = append(pod.Spec.Volumes, extraVolumes...)
	return nil
}

// ── container builders ────────────────────────────────────────────────────────

// datasetInitContainer streams a dataset from Azure Blob to the local NVMe
// emptyDir volume. It verifies a SHA-256 checksum before exiting, so the
// main training container never starts with corrupt or partial data.
func datasetInitContainer(ds tauv1alpha1.DatasetSpec, volName, image string) corev1.Container {
	env := []corev1.EnvVar{
		{Name: "STORAGE_ACCOUNT", Value: ds.Source.StorageAccount},
		{Name: "CONTAINER", Value: ds.Source.Container},
		{Name: "BLOB_PREFIX", Value: ds.Source.Prefix},
		{Name: "DEST_DIR", Value: ds.MountPath},
		{Name: "DATASET_NAME", Value: ds.Name},
	}
	if ds.Source.CredentialSecretRef != nil {
		env = append(env, corev1.EnvVar{
			Name: "AZURE_STORAGE_KEY",
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: ds.Source.CredentialSecretRef.Name},
					Key:                  ds.Source.CredentialSecretRef.Key,
				},
			},
		})
	}

	return corev1.Container{
		Name:  fmt.Sprintf("init-dataset-%s", sanitizeName(ds.Name)),
		Image: image,
		Env:   env,
		VolumeMounts: []corev1.VolumeMount{
			{Name: volName, MountPath: ds.MountPath},
		},
		Resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("500m"),
				corev1.ResourceMemory: resource.MustParse("512Mi"),
			},
			Limits: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("4"),
				corev1.ResourceMemory: resource.MustParse("4Gi"),
			},
		},
		TerminationMessagePolicy: corev1.TerminationMessageFallbackToLogsOnError,
	}
}

// checkpointSidecar watches the checkpoint directory for .done marker files
// and uploads the corresponding checkpoint to Azure Blob asynchronously,
// without blocking the training loop (Section 3 – Asynchronous sync strategy).
func checkpointSidecar(cp *tauv1alpha1.CheckpointSpec, volName, image string) corev1.Container {
	maxRetry := cp.UploadRetryMaxAttempts
	if maxRetry == 0 {
		maxRetry = 10
	}
	retention := cp.RetentionCount
	if retention == 0 {
		retention = 2
	}

	env := []corev1.EnvVar{
		{Name: "WATCH_DIR", Value: cp.LocalPath},
		{Name: "STORAGE_ACCOUNT", Value: cp.Remote.StorageAccount},
		{Name: "CONTAINER", Value: cp.Remote.Container},
		{Name: "BLOB_PREFIX", Value: cp.Remote.Prefix},
		{Name: "MAX_RETRY_ATTEMPTS", Value: fmt.Sprintf("%d", maxRetry)},
		{Name: "RETENTION_COUNT", Value: fmt.Sprintf("%d", retention)},
	}
	if cp.Remote.CredentialSecretRef != nil {
		env = append(env, corev1.EnvVar{
			Name: "AZURE_STORAGE_KEY",
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: cp.Remote.CredentialSecretRef.Name},
					Key:                  cp.Remote.CredentialSecretRef.Key,
				},
			},
		})
	}

	return corev1.Container{
		Name:  "checkpoint-sidecar",
		Image: image,
		Env:   env,
		VolumeMounts: []corev1.VolumeMount{
			{Name: volName, MountPath: cp.LocalPath},
		},
		Resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("100m"),
				corev1.ResourceMemory: resource.MustParse("128Mi"),
			},
			Limits: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("500m"),
				corev1.ResourceMemory: resource.MustParse("512Mi"),
			},
		},
		TerminationMessagePolicy: corev1.TerminationMessageFallbackToLogsOnError,
	}
}

// ── volume builders ───────────────────────────────────────────────────────────

// hotLoadVolume returns a disk-backed emptyDir sized to hold the dataset plus
// headroom. The empty dir is mapped to node NVMe on properly labelled node pools
// (requires the node to have /var/lib/kubelet on the NVMe device).
func hotLoadVolume(name string, sizeGB int64) corev1.Volume {
	limit := resource.MustParse(fmt.Sprintf("%dGi", sizeGB+2))
	return corev1.Volume{
		Name: name,
		VolumeSource: corev1.VolumeSource{
			EmptyDir: &corev1.EmptyDirVolumeSource{
				SizeLimit: &limit,
			},
		},
	}
}

// blobFuseVolume returns an inline CSI ephemeral volume backed by BlobFuse2.
// Used as the automatic fallback when HotLoad is not feasible (Section 5).
func blobFuseVolume(name string, src tauv1alpha1.BlobSource) corev1.Volume {
	return corev1.Volume{
		Name: name,
		VolumeSource: corev1.VolumeSource{
			CSI: &corev1.CSIVolumeSource{
				Driver:   "blob.csi.azure.com",
				ReadOnly: boolPtr(true),
				VolumeAttributes: map[string]string{
					"storageAccount": src.StorageAccount,
					"containerName":  src.Container,
					"mountOptions":   "uid=1000,gid=1000,allow_other,--use-adls=false",
				},
			},
		},
	}
}

func boolPtr(b bool) *bool { return &b }

// sanitizeName converts a dataset name to a valid Kubernetes label / volume name
// by lowercasing and replacing non-alphanumeric characters with hyphens.
func sanitizeName(s string) string {
	b := []byte(s)
	for i, c := range b {
		if (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-' {
			continue
		}
		if c >= 'A' && c <= 'Z' {
			b[i] = c + 32
		} else {
			b[i] = '-'
		}
	}
	return string(b)
}
