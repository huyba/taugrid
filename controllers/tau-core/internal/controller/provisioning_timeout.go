// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package controller

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

const provisioningTimeoutReason = "ProvisioningTimeout"

var workloadGVK = schema.GroupVersionKind{Group: "kueue.x-k8s.io", Version: "v1beta2", Kind: "Workload"}

// ProvisioningTimeoutReconciler deactivates Kueue Workloads that have held
// quota for longer than Timeout while their ProvisioningRequest AdmissionCheck
// is still Pending.
//
// Kueue reserves quota before it asks Cluster Autoscaler for capacity, and has
// no deadline for that answer. The AKS autoscaler does not fail a request it
// cannot satisfy; it keeps retrying, so the quota would stay reserved
// indefinitely. Setting spec.active=false makes Kueue evict the Workload, which
// releases the quota and deletes the ProvisioningRequest. The Job stays
// suspended and no pod is created.
type ProvisioningTimeoutReconciler struct {
	client.Client
	Timeout  time.Duration
	Recorder record.EventRecorder
	// Now is the clock; nil means time.Now.
	Now func() time.Time
}

// +kubebuilder:rbac:groups=kueue.x-k8s.io,resources=workloads,verbs=get;list;watch;patch
// +kubebuilder:rbac:groups=kueue.x-k8s.io,resources=admissionchecks,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch

func (r *ProvisioningTimeoutReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	workload := newQueueObject(workloadGVK)
	if err := r.Get(ctx, req.NamespacedName, workload); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	reservedAt, holding := holdingQuotaSince(workload)
	if !holding {
		return ctrl.Result{}, nil
	}
	check, err := r.pendingProvisioningCheck(ctx, workload)
	if err != nil || check == "" {
		return ctrl.Result{}, err
	}

	now := time.Now
	if r.Now != nil {
		now = r.Now
	}
	waited := now().Sub(reservedAt)
	if waited < r.Timeout {
		return ctrl.Result{RequeueAfter: r.Timeout - waited}, nil
	}

	patch := client.MergeFrom(workload.DeepCopy())
	if err := unstructured.SetNestedField(workload.Object, false, "spec", "active"); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.Patch(ctx, workload, patch); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	message := fmt.Sprintf("Deactivated: admission check %q found no capacity within %s; quota released", check, r.Timeout)
	if r.Recorder != nil {
		r.Recorder.Event(workload, corev1.EventTypeWarning, provisioningTimeoutReason, message)
	}
	log.FromContext(ctx).Info("deactivated workload after provisioning timeout",
		"workload", req.NamespacedName.String(), "admissionCheck", check, "waited", waited.String())
	return ctrl.Result{}, nil
}

// holdingQuotaSince returns when the Workload reserved quota, and false when it
// is not in the state this controller acts on: active, quota reserved, and
// neither admitted nor finished.
func holdingQuotaSince(workload *unstructured.Unstructured) (time.Time, bool) {
	if active, found, _ := unstructured.NestedBool(workload.Object, "spec", "active"); found && !active {
		return time.Time{}, false
	}
	if conditionIsTrue(workload, "Admitted") || conditionIsTrue(workload, "Finished") {
		return time.Time{}, false
	}
	conditions, _, _ := unstructured.NestedSlice(workload.Object, "status", "conditions")
	for _, raw := range conditions {
		cond, ok := raw.(map[string]any)
		if !ok || cond["type"] != "QuotaReserved" || cond["status"] != "True" {
			continue
		}
		since, _ := cond["lastTransitionTime"].(string)
		reservedAt, err := time.Parse(time.RFC3339, since)
		return reservedAt, err == nil
	}
	return time.Time{}, false
}

// pendingProvisioningCheck returns the name of a Pending admission check on the
// Workload that is handled by Kueue's ProvisioningRequest controller, or "".
func (r *ProvisioningTimeoutReconciler) pendingProvisioningCheck(ctx context.Context, workload *unstructured.Unstructured) (string, error) {
	checks, _, _ := unstructured.NestedSlice(workload.Object, "status", "admissionChecks")
	for _, raw := range checks {
		state, ok := raw.(map[string]any)
		if !ok || state["state"] != "Pending" {
			continue
		}
		name, _ := state["name"].(string)
		check := newQueueObject(admissionCheckGVK)
		if err := r.Get(ctx, client.ObjectKey{Name: name}, check); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return "", err
		}
		controllerName, _, _ := unstructured.NestedString(check.Object, "spec", "controllerName")
		if controllerName == provisioningRequestAdmissionCheckController {
			return name, nil
		}
	}
	return "", nil
}

func (r *ProvisioningTimeoutReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		Named("provisioning-timeout").
		For(newQueueObject(workloadGVK)).
		Complete(r)
}
