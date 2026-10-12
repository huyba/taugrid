// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package controller

import (
	"context"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestProvisioningTimeoutReconcile(t *testing.T) {
	reservedAt := time.Date(2026, 10, 12, 20, 0, 0, 0, time.UTC)
	const timeout = 10 * time.Minute

	tests := []struct {
		name        string
		elapsed     time.Duration
		mutate      func(workload map[string]any)
		wantActive  bool
		wantRequeue time.Duration
		wantEvent   bool
	}{
		{
			name:        "pending within the timeout is left alone and rechecked at the deadline",
			elapsed:     4 * time.Minute,
			wantActive:  true,
			wantRequeue: 6 * time.Minute,
		},
		{
			name:      "pending past the timeout is deactivated",
			elapsed:   11 * time.Minute,
			wantEvent: true,
		},
		{
			name:    "admitted workload is never deactivated",
			elapsed: time.Hour,
			mutate: func(workload map[string]any) {
				setCondition(workload, "Admitted", "True")
				setCheckState(workload, "provisioning", "Ready")
			},
			wantActive: true,
		},
		{
			name:    "finished workload is never deactivated",
			elapsed: time.Hour,
			mutate: func(workload map[string]any) {
				setCondition(workload, "Finished", "True")
			},
			wantActive: true,
		},
		{
			name:    "workload without reserved quota is ignored",
			elapsed: time.Hour,
			mutate: func(workload map[string]any) {
				setCondition(workload, "QuotaReserved", "False")
			},
			wantActive: true,
		},
		{
			name:    "pending check from another controller is ignored",
			elapsed: time.Hour,
			mutate: func(workload map[string]any) {
				setCheckState(workload, "provisioning", "Ready")
				setCheckState(workload, "multikueue", "Pending")
			},
			wantActive: true,
		},
		{
			name:    "pending check that no longer exists is ignored",
			elapsed: time.Hour,
			mutate: func(workload map[string]any) {
				setCheckState(workload, "provisioning", "Ready")
				setCheckState(workload, "deleted-check", "Pending")
			},
			wantActive: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			workload := pendingProvisioningWorkload(reservedAt)
			if test.mutate != nil {
				test.mutate(workload.Object)
			}
			c := fake.NewClientBuilder().
				WithScheme(testScheme(t)).
				WithObjects(
					workload,
					timeoutTestAdmissionCheck("provisioning", provisioningRequestAdmissionCheckController),
					timeoutTestAdmissionCheck("multikueue", multiKueueAdmissionCheckController),
				).
				Build()
			recorder := record.NewFakeRecorder(1)
			reconciler := &ProvisioningTimeoutReconciler{
				Client:   c,
				Timeout:  timeout,
				Recorder: recorder,
				Now:      func() time.Time { return reservedAt.Add(test.elapsed) },
			}

			key := types.NamespacedName{Namespace: "team-a", Name: "job-train"}
			result, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: key})
			if err != nil {
				t.Fatalf("Reconcile: %v", err)
			}
			if result.RequeueAfter != test.wantRequeue {
				t.Errorf("RequeueAfter = %s, want %s", result.RequeueAfter, test.wantRequeue)
			}

			got := newQueueObject(workloadGVK)
			if err := c.Get(ctx, key, got); err != nil {
				t.Fatalf("Get workload: %v", err)
			}
			active, _, _ := unstructured.NestedBool(got.Object, "spec", "active")
			if active != test.wantActive {
				t.Errorf("spec.active = %t, want %t", active, test.wantActive)
			}
			if gotEvent := len(recorder.Events) == 1; gotEvent != test.wantEvent {
				t.Errorf("event recorded = %t, want %t", gotEvent, test.wantEvent)
			}
		})
	}
}

func TestProvisioningTimeoutReconcileIgnoresAlreadyInactiveAndMissingWorkloads(t *testing.T) {
	ctx := context.Background()
	reservedAt := time.Date(2026, 10, 12, 20, 0, 0, 0, time.UTC)
	workload := pendingProvisioningWorkload(reservedAt)
	if err := unstructured.SetNestedField(workload.Object, false, "spec", "active"); err != nil {
		t.Fatal(err)
	}
	c := fake.NewClientBuilder().
		WithScheme(testScheme(t)).
		WithObjects(workload, timeoutTestAdmissionCheck("provisioning", provisioningRequestAdmissionCheckController)).
		Build()
	recorder := record.NewFakeRecorder(1)
	reconciler := &ProvisioningTimeoutReconciler{
		Client:   c,
		Timeout:  time.Minute,
		Recorder: recorder,
		Now:      func() time.Time { return reservedAt.Add(time.Hour) },
	}

	for _, name := range []string{"job-train", "does-not-exist"} {
		result, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "team-a", Name: name}})
		if err != nil {
			t.Fatalf("Reconcile %s: %v", name, err)
		}
		if result != (ctrl.Result{}) {
			t.Errorf("Reconcile %s result = %+v, want empty", name, result)
		}
	}
	if len(recorder.Events) != 0 {
		t.Errorf("recorded %d events, want none", len(recorder.Events))
	}
}

// pendingProvisioningWorkload is a Workload that reserved quota at reservedAt
// and is still waiting on the "provisioning" admission check.
func pendingProvisioningWorkload(reservedAt time.Time) *unstructured.Unstructured {
	workload := newQueueObject(workloadGVK)
	workload.SetNamespace("team-a")
	workload.SetName("job-train")
	workload.Object["spec"] = map[string]any{"active": true}
	workload.Object["status"] = map[string]any{
		"conditions": []any{
			map[string]any{
				"type":               "QuotaReserved",
				"status":             "True",
				"lastTransitionTime": reservedAt.Format(time.RFC3339),
			},
			map[string]any{"type": "Admitted", "status": "False"},
		},
		"admissionChecks": []any{
			map[string]any{"name": "provisioning", "state": "Pending"},
		},
	}
	return workload
}

func timeoutTestAdmissionCheck(name, controllerName string) client.Object {
	check := newQueueObject(admissionCheckGVK)
	check.SetName(name)
	check.Object["spec"] = map[string]any{"controllerName": controllerName}
	return check
}

func setCondition(workload map[string]any, condType, status string) {
	conditions, _, _ := unstructured.NestedSlice(workload, "status", "conditions")
	for _, raw := range conditions {
		if cond, ok := raw.(map[string]any); ok && cond["type"] == condType {
			cond["status"] = status
			_ = unstructured.SetNestedSlice(workload, conditions, "status", "conditions")
			return
		}
	}
	conditions = append(conditions, map[string]any{"type": condType, "status": status})
	_ = unstructured.SetNestedSlice(workload, conditions, "status", "conditions")
}

func setCheckState(workload map[string]any, name, state string) {
	checks, _, _ := unstructured.NestedSlice(workload, "status", "admissionChecks")
	for _, raw := range checks {
		if check, ok := raw.(map[string]any); ok && check["name"] == name {
			check["state"] = state
			_ = unstructured.SetNestedSlice(workload, checks, "status", "admissionChecks")
			return
		}
	}
	checks = append(checks, map[string]any{"name": name, "state": state})
	_ = unstructured.SetNestedSlice(workload, checks, "status", "admissionChecks")
}
