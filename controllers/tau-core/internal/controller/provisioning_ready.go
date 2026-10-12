// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package controller

import (
	"context"
	"fmt"
	"sort"
	"strings"

	tauv1alpha1 "github.com/Azure/taugrid/controllers/tau-core/api/v1alpha1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const provisioningRequestAdmissionCheckController = "kueue.x-k8s.io/provisioning-request"

var provisioningRequestConfigGVK = schema.GroupVersionKind{
	Group:   "kueue.x-k8s.io",
	Version: "v1beta2",
	Kind:    "ProvisioningRequestConfig",
}

// ProvisioningReadinessStatus is the result of a ProvisioningReadinessReader.Check call.
type ProvisioningReadinessStatus struct {
	Ready   bool
	Message string
}

// ProvisioningReadinessReader reports whether the ProvisioningRequest
// AdmissionCheck is correctly wired and Active for this cluster.
type ProvisioningReadinessReader interface {
	Check(context.Context) (ProvisioningReadinessStatus, error)
}

// KubernetesProvisioningReadiness reads Kueue objects via the API server to
// verify the ProvisioningRequest AdmissionCheck exists, is Active, and
// references a valid ProvisioningRequestConfig.
type KubernetesProvisioningReadiness struct {
	Reader client.Reader
}

func (r *TauClusterReconciler) provisioningReadinessCondition(
	ctx context.Context,
	generation int64,
) (metav1.Condition, error) {
	if r.ProvisioningReadiness == nil {
		return condition(
			tauv1alpha1.ConditionProvisioningReady,
			metav1.ConditionFalse,
			"NotConfigured",
			"capacity-aware admission is not enabled on this cluster",
			generation,
		), nil
	}

	status, err := r.ProvisioningReadiness.Check(ctx)
	conditionStatus := metav1.ConditionFalse
	reason := "ProvisioningAdmissionCheckNotReady"
	if status.Ready && err == nil {
		conditionStatus = metav1.ConditionTrue
		reason = "ProvisioningAdmissionCheckReady"
	}
	return condition(
		tauv1alpha1.ConditionProvisioningReady,
		conditionStatus,
		reason,
		status.Message,
		generation,
	), err
}

// Check lists all AdmissionCheck objects and finds the first one whose
// controllerName is kueue.x-k8s.io/provisioning-request. Returns not-ready
// if none is found, if the found check is not Active, or if its referenced
// ProvisioningRequestConfig is missing or has no provisioningClassName.
func (c *KubernetesProvisioningReadiness) Check(ctx context.Context) (ProvisioningReadinessStatus, error) {
	if c == nil || c.Reader == nil {
		return ProvisioningReadinessStatus{Message: "provisioning readiness reader is not configured"}, nil
	}

	var checks unstructured.UnstructuredList
	checks.SetGroupVersionKind(admissionCheckListGVK)
	if err := c.Reader.List(ctx, &checks); err != nil {
		return ProvisioningReadinessStatus{
			Message: "cannot list AdmissionChecks",
		}, fmt.Errorf("list AdmissionChecks: %w", err)
	}

	issues := make([]string, 0)
	found := false
	for i := range checks.Items {
		check := &checks.Items[i]
		controllerName, _, _ := unstructured.NestedString(check.Object, "spec", "controllerName")
		if strings.TrimSpace(controllerName) != provisioningRequestAdmissionCheckController {
			continue
		}
		found = true
		ready, issue, err := c.admissionCheckReady(ctx, check)
		if err != nil {
			return ProvisioningReadinessStatus{Message: issue}, err
		}
		if ready {
			return ProvisioningReadinessStatus{
				Ready:   true,
				Message: fmt.Sprintf("AdmissionCheck %q is Active with a valid ProvisioningRequestConfig", check.GetName()),
			}, nil
		}
		issues = append(issues, issue)
	}
	if !found {
		return ProvisioningReadinessStatus{
			Message: fmt.Sprintf("no AdmissionCheck uses controller %s; set capacityAwareAdmission.enabled=true in Helm values", provisioningRequestAdmissionCheckController),
		}, nil
	}
	sort.Strings(issues)
	return ProvisioningReadinessStatus{Message: strings.Join(issues, "; ")}, nil
}

func (c *KubernetesProvisioningReadiness) admissionCheckReady(
	ctx context.Context,
	check *unstructured.Unstructured,
) (bool, string, error) {
	name := check.GetName()
	if !conditionIsTrue(check, "Active") {
		return false, fmt.Sprintf("AdmissionCheck %q is not Active; verify the Kueue ProvisioningRequest controller is running", name), nil
	}

	apiGroup, _, _ := unstructured.NestedString(check.Object, "spec", "parameters", "apiGroup")
	kind, _, _ := unstructured.NestedString(check.Object, "spec", "parameters", "kind")
	configName, _, _ := unstructured.NestedString(check.Object, "spec", "parameters", "name")
	if apiGroup != "kueue.x-k8s.io" || kind != "ProvisioningRequestConfig" || strings.TrimSpace(configName) == "" {
		return false, fmt.Sprintf("AdmissionCheck %q does not reference a ProvisioningRequestConfig via spec.parameters", name), nil
	}

	config := &unstructured.Unstructured{}
	config.SetGroupVersionKind(provisioningRequestConfigGVK)
	if err := c.Reader.Get(ctx, client.ObjectKey{Name: configName}, config); err != nil {
		issue := fmt.Sprintf("AdmissionCheck %q references unreadable ProvisioningRequestConfig %q", name, configName)
		if apierrors.IsNotFound(err) {
			return false, issue, nil
		}
		return false, issue, fmt.Errorf("get ProvisioningRequestConfig %q: %w", configName, err)
	}

	provClass, _, _ := unstructured.NestedString(config.Object, "spec", "provisioningClassName")
	if strings.TrimSpace(provClass) == "" {
		return false, fmt.Sprintf("ProvisioningRequestConfig %q has no provisioningClassName", configName), nil
	}
	return true, "", nil
}
