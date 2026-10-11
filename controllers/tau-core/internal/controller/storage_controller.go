// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package controller

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	tauv1alpha1 "github.com/Azure/taugrid/controllers/tau-core/api/v1alpha1"
)

const (
	dataIngestionFinalizer = "tau.azure.com/data-ingestion-protection"

	// defaultNFSStorageClass is the Azure Files Premium NFS storage class with
	// nconnect=4, rsize/wsize=1048576 mount options configured at cluster level.
	defaultNFSStorageClass = "azurefile-premium-nfs"

	// defaultCheckpointStorageClass is Azure Premium SSD (managed-csi-premium)
	// for high-throughput local checkpoint staging.
	defaultCheckpointStorageClass = "managed-csi-premium"

	// defaultCheckpointCapacity is the PVC size reserved for checkpoint staging.
	defaultCheckpointCapacity = "200Gi"

	// storageRequeueInterval is the steady-state requeue period for drift detection.
	storageRequeueInterval = 5 * time.Minute
	storageRetryInterval   = 15 * time.Second
)

// DataIngestionReconciler manages storage infrastructure for DataIngestion CRs.
type DataIngestionReconciler struct {
	client.Client
	// SystemNamespace scopes the controller to a single namespace; empty means cluster-wide.
	SystemNamespace string
}

// +kubebuilder:rbac:groups=tau.azure.com,resources=dataingestions,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=tau.azure.com,resources=dataingestions/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=tau.azure.com,resources=dataingestions/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=persistentvolumeclaims,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=nodes,verbs=get;list;watch

func (r *DataIngestionReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithValues("dataingestion", req.NamespacedName.String())

	var di tauv1alpha1.DataIngestion
	if err := r.Get(ctx, req.NamespacedName, &di); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Deletion path: release finalizer so owner-ref GC deletes owned PVCs.
	if !di.DeletionTimestamp.IsZero() {
		return r.reconcileDelete(ctx, &di)
	}

	// Ensure finalizer is set before doing any real work.
	if !controllerutil.ContainsFinalizer(&di, dataIngestionFinalizer) {
		controllerutil.AddFinalizer(&di, dataIngestionFinalizer)
		if err := r.Update(ctx, &di); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	result, err := r.reconcileSpec(ctx, &di)
	if err != nil {
		logger.Error(err, "reconcile failed")
	}
	return result, err
}

// reconcileSpec drives the happy-path reconciliation loop.
func (r *DataIngestionReconciler) reconcileSpec(ctx context.Context, di *tauv1alpha1.DataIngestion) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	newStatus := tauv1alpha1.DataIngestionStatus{
		ObservedGeneration: di.Generation,
		Phase:              tauv1alpha1.DataIngestionPhaseProvisioning,
	}

	// ── 1. Datasets ──────────────────────────────────────────────────────────
	datasetStatuses, err := r.reconcileDatasets(ctx, di)
	if err != nil {
		return ctrl.Result{RequeueAfter: storageRetryInterval}, err
	}
	newStatus.Datasets = datasetStatuses

	// ── 2. Shared State PVC (Azure Files Premium NFS) ────────────────────────
	if di.Spec.SharedState != nil {
		pvcName, err := r.reconcileSharedStatePVC(ctx, di)
		if err != nil {
			return ctrl.Result{RequeueAfter: storageRetryInterval}, err
		}
		newStatus.SharedStatePVCName = pvcName
	}

	// ── 3. Checkpoint staging PVC (Azure Premium SSD, RWO) ───────────────────
	if di.Spec.Checkpoints != nil {
		pvcName, err := r.reconcileCheckpointPVC(ctx, di)
		if err != nil {
			return ctrl.Result{RequeueAfter: storageRetryInterval}, err
		}
		newStatus.CheckpointPVCName = pvcName
	}

	// ── 4. Update status ─────────────────────────────────────────────────────
	newStatus.Phase = tauv1alpha1.DataIngestionPhaseReady
	newStatus.Conditions = []metav1.Condition{
		{
			Type:               tauv1alpha1.ConditionReady,
			Status:             metav1.ConditionTrue,
			Reason:             "Provisioned",
			Message:            "All storage resources provisioned successfully",
			LastTransitionTime: metav1.Now(),
			ObservedGeneration: di.Generation,
		},
	}

	di.Status = newStatus
	if err := r.Status().Update(ctx, di); err != nil {
		logger.Error(err, "status update failed")
		return ctrl.Result{}, err
	}

	return ctrl.Result{RequeueAfter: storageRequeueInterval}, nil
}

// reconcileDatasets computes the effective load strategy for every dataset,
// implementing the NVMe capacity exhaustion fallback from Section 5.
func (r *DataIngestionReconciler) reconcileDatasets(ctx context.Context, di *tauv1alpha1.DataIngestion) ([]tauv1alpha1.DataIngestionDatasetStatus, error) {
	logger := log.FromContext(ctx)

	nvmeCap := di.Spec.NVMeCapacityGiB
	if nvmeCap == 0 {
		nvmeCap = 3000
	}

	// Sum requested HotLoad GiB to detect exhaustion before provisioning.
	totalHotLoadGiB := int64(0)
	for _, ds := range di.Spec.Datasets {
		if ds.LoadStrategy == tauv1alpha1.LoadStrategyHotLoad {
			totalHotLoadGiB += ds.SizeGB
		}
	}

	statuses := make([]tauv1alpha1.DataIngestionDatasetStatus, 0, len(di.Spec.Datasets))

	for _, ds := range di.Spec.Datasets {
		st := tauv1alpha1.DataIngestionDatasetStatus{
			Name:          ds.Name,
			Phase:         "Provisioning",
			EffectiveLoad: ds.LoadStrategy,
		}

		// ── NVMe Capacity Exhaustion Fallback (Section 5) ────────────────────
		// If the aggregate HotLoad dataset size exceeds the node NVMe capacity
		// the controller automatically degrades the strategy to StreamMount
		// (BlobFuse2 CSI network mount) to avoid node disk pressure.
		if ds.LoadStrategy == tauv1alpha1.LoadStrategyHotLoad && totalHotLoadGiB > nvmeCap {
			st.EffectiveLoad = tauv1alpha1.LoadStrategyStreamMount
			st.Message = fmt.Sprintf(
				"HotLoad downgraded to StreamMount: total requested %d GiB exceeds node NVMe capacity %d GiB",
				totalHotLoadGiB, nvmeCap,
			)
			logger.Info("NVMe capacity exceeded; falling back to StreamMount",
				"dataset", ds.Name,
				"totalRequestedGiB", totalHotLoadGiB,
				"nodeCapGiB", nvmeCap,
			)
		}

		// StreamMount uses an inline BlobFuse2 CSI volume — no PVC required.
		// HotLoad uses a pod-local emptyDir populated by the init container.
		// In both cases no persistent PVC is created for the dataset itself;
		// the pod injector wires the correct volume type at admission time.
		st.Phase = "Ready"
		statuses = append(statuses, st)
	}

	return statuses, nil
}

// reconcileSharedStatePVC ensures the Azure Files Premium NFS PVC exists.
// This provides ReadWriteMany access for distributed training coordination.
func (r *DataIngestionReconciler) reconcileSharedStatePVC(ctx context.Context, di *tauv1alpha1.DataIngestion) (string, error) {
	ss := di.Spec.SharedState
	pvcName := fmt.Sprintf("%s-shared-state", di.Name)

	storageClass := ss.StorageClassName
	if storageClass == "" {
		storageClass = defaultNFSStorageClass
	}
	capacityStr := ss.Capacity
	if capacityStr == "" {
		capacityStr = "100Gi"
	}

	pvc := &corev1.PersistentVolumeClaim{}
	err := r.Get(ctx, types.NamespacedName{Name: pvcName, Namespace: di.Namespace}, pvc)

	if errors.IsNotFound(err) {
		qty, parseErr := resource.ParseQuantity(capacityStr)
		if parseErr != nil {
			return "", fmt.Errorf("invalid SharedState capacity %q: %w", capacityStr, parseErr)
		}
		rwx := corev1.ReadWriteMany
		desired := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      pvcName,
				Namespace: di.Namespace,
				Labels: map[string]string{
					"tau.azure.com/data-ingestion": di.Name,
					"tau.azure.com/storage-tier":   "shared-state",
				},
				// Owner reference ensures PVC is garbage-collected with the DataIngestion.
				OwnerReferences: []metav1.OwnerReference{storageOwnerRef(di)},
			},
			Spec: corev1.PersistentVolumeClaimSpec{
				AccessModes:      []corev1.PersistentVolumeAccessMode{rwx},
				StorageClassName: &storageClass,
				Resources: corev1.VolumeResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceStorage: qty,
					},
				},
				// VolumeMode defaults to Filesystem; Azure Files NFS requires this.
				VolumeMode: volumeModePtr(corev1.PersistentVolumeFilesystem),
			},
		}
		if createErr := r.Create(ctx, desired); createErr != nil {
			return "", fmt.Errorf("create SharedState PVC: %w", createErr)
		}
		log.FromContext(ctx).Info("created SharedState PVC", "pvc", pvcName, "storageClass", storageClass)
		return pvcName, nil
	}

	return pvcName, err
}

// reconcileCheckpointPVC ensures the Premium SSD PVC for checkpoint staging exists.
// ReadWriteOnce (local disk) gives maximum write throughput while the async
// sidecar streams checkpoints to Azure Blob in the background.
func (r *DataIngestionReconciler) reconcileCheckpointPVC(ctx context.Context, di *tauv1alpha1.DataIngestion) (string, error) {
	pvcName := fmt.Sprintf("%s-checkpoints", di.Name)
	storageClass := defaultCheckpointStorageClass
	rwo := corev1.ReadWriteOnce

	pvc := &corev1.PersistentVolumeClaim{}
	err := r.Get(ctx, types.NamespacedName{Name: pvcName, Namespace: di.Namespace}, pvc)

	if errors.IsNotFound(err) {
		qty := resource.MustParse(defaultCheckpointCapacity)
		desired := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      pvcName,
				Namespace: di.Namespace,
				Labels: map[string]string{
					"tau.azure.com/data-ingestion": di.Name,
					"tau.azure.com/storage-tier":   "checkpoint",
				},
				OwnerReferences: []metav1.OwnerReference{storageOwnerRef(di)},
			},
			Spec: corev1.PersistentVolumeClaimSpec{
				AccessModes:      []corev1.PersistentVolumeAccessMode{rwo},
				StorageClassName: &storageClass,
				Resources: corev1.VolumeResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceStorage: qty,
					},
				},
				VolumeMode: volumeModePtr(corev1.PersistentVolumeFilesystem),
			},
		}
		if createErr := r.Create(ctx, desired); createErr != nil {
			return "", fmt.Errorf("create Checkpoint PVC: %w", createErr)
		}
		log.FromContext(ctx).Info("created Checkpoint PVC", "pvc", pvcName)
		return pvcName, nil
	}

	return pvcName, err
}

func (r *DataIngestionReconciler) reconcileDelete(ctx context.Context, di *tauv1alpha1.DataIngestion) (ctrl.Result, error) {
	if controllerutil.ContainsFinalizer(di, dataIngestionFinalizer) {
		controllerutil.RemoveFinalizer(di, dataIngestionFinalizer)
		if err := r.Update(ctx, di); err != nil {
			return ctrl.Result{}, err
		}
	}
	return ctrl.Result{}, nil
}

// SetupWithManager registers the controller with the controller-runtime manager.
// Call this from your main.go after mgr is constructed.
func (r *DataIngestionReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&tauv1alpha1.DataIngestion{}).
		Owns(&corev1.PersistentVolumeClaim{}).
		Complete(r)
}

// ── helpers ──────────────────────────────────────────────────────────────────

func storageOwnerRef(di *tauv1alpha1.DataIngestion) metav1.OwnerReference {
	t := true
	return metav1.OwnerReference{
		APIVersion:         tauv1alpha1.GroupVersion.String(),
		Kind:               tauv1alpha1.KindDataIngestion,
		Name:               di.Name,
		UID:                di.UID,
		Controller:         &t,
		BlockOwnerDeletion: &t,
	}
}

func volumeModePtr(m corev1.PersistentVolumeMode) *corev1.PersistentVolumeMode {
	return &m
}
