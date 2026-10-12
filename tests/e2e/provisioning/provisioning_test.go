// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

// Package provisioning contains e2e tests for capacity-aware Kueue admission.
//
// Every scenario runs twice against the same autoscaled node pool:
//
//   - "Before": a quota-only ClusterQueue. Kueue admits on logical quota alone.
//   - "After": the same ClusterQueue gated by a ProvisioningRequest
//     AdmissionCheck. Kueue resumes a Job only once Cluster Autoscaler reports
//     its capacity as provisioned.
//
// Scenarios:
//
//   - Success: capacity can be provisioned and the Job runs.
//   - Timeout: capacity never arrives (the Job asks for more CPU than any node
//     has). Shows what is stuck, what holds quota, and who is starved.
//   - Cancellation: the Job is deleted while it waits for capacity.
//   - Cleanup: the Job finishes; nothing it reserved is left behind.
//
// The tests need the ProvisioningRequest CRD, a Cluster Autoscaler that
// processes it, and an autoscaled node pool, so the package is gated behind
// E2E_PROVISIONING=1 in addition to AI_RUNTIME_E2E=1.
package provisioning

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"

	e2e "github.com/Azure/taugrid/tests/e2e"
	"github.com/Azure/taugrid/tests/e2e/results"
)

const (
	localQueue        = "e2e-capacity-queue"
	afterCheck        = "e2e-capacity-after-check"
	provisioningClass = "best-effort-atomic-scale-up.autoscaling.x-k8s.io"

	// fitsCPU fits on one node of the pool. tooBigCPU fits on no node, so its
	// capacity never arrives; two such jobs exceed the 1000 CPU quota.
	fitsCPU   = "500m"
	tooBigCPU = "600"

	consumeAnnotation = "autoscaling.x-k8s.io/consume-provisioning-request"

	pollInterval = 2 * time.Second
)

// mode is one side of the before/after comparison.
type mode struct {
	namespace string
	// gated is true when the queue has the ProvisioningRequest AdmissionCheck.
	gated bool
}

var (
	before = mode{namespace: "e2e-capacity-before"}
	after  = mode{namespace: "e2e-capacity-after", gated: true}
)

// The shared e2e helpers read Workloads and LocalQueues at v1beta1. These tests
// use v1beta2 so they also run on Kueue releases that no longer serve v1beta1.
var (
	workloadGVR            = schema.GroupVersionResource{Group: "kueue.x-k8s.io", Version: "v1beta2", Resource: "workloads"}
	localQueueGVR          = schema.GroupVersionResource{Group: "kueue.x-k8s.io", Version: "v1beta2", Resource: "localqueues"}
	provisioningRequestGVR = schema.GroupVersionResource{Group: "autoscaling.x-k8s.io", Version: "v1", Resource: "provisioningrequests"}
)

func enabled() bool {
	return os.Getenv("AI_RUNTIME_E2E") == "1" && os.Getenv("E2E_PROVISIONING") == "1"
}

func TestMain(m *testing.M) {
	if !enabled() {
		os.Exit(m.Run())
	}
	os.Exit(runTests(m))
}

func runTests(m *testing.M) int {
	_, dynamicClient, err := e2e.BuildClients()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to build K8s clients: %v\n", err)
		return 1
	}
	ctx := context.Background()
	for _, fixture := range []string{"before-resources.yaml", "after-resources.yaml"} {
		if err := e2e.ApplyFixtureWithClient(ctx, dynamicClient, fixture); err != nil {
			fmt.Fprintf(os.Stderr, "Failed to apply %s: %v\n", fixture, err)
			return 1
		}
		defer e2e.DeleteFixtureWithClient(ctx, dynamicClient, fixture)
	}
	defer results.FlushAll()
	return m.Run()
}

// --- Tests ---

func TestBeforeSuccess(t *testing.T)      { successScenario(newContext(t), before) }
func TestAfterSuccess(t *testing.T)       { successScenario(newContext(t), after) }
func TestBeforeTimeout(t *testing.T)      { timeoutScenario(newContext(t), before) }
func TestAfterTimeout(t *testing.T)       { timeoutScenario(newContext(t), after) }
func TestBeforeCancellation(t *testing.T) { cancellationScenario(newContext(t), before) }
func TestAfterCancellation(t *testing.T)  { cancellationScenario(newContext(t), after) }
func TestBeforeCleanup(t *testing.T)      { cleanupScenario(newContext(t), before) }
func TestAfterCleanup(t *testing.T)       { cleanupScenario(newContext(t), after) }

func newContext(t *testing.T) *e2e.TestContext {
	t.Helper()
	tc := e2e.NewTestContext(t, context.Background())
	if !enabled() {
		t.Skip("Skipping: set E2E_PROVISIONING=1 and E2E_PROVISIONING_NODE_SELECTOR=<key>=<value> to run")
	}
	return tc
}

// successScenario: the capacity can be provisioned and the Job runs.
//
// Before, the workload is admitted on quota with no evidence of capacity. After,
// admission waits for the ProvisioningRequest to be Provisioned.
func successScenario(tc *e2e.TestContext, m mode) {
	registerDiagnostics(tc, m)
	submitJob(tc, m, "success", fitsCPU, "sleep infinity")

	if m.gated {
		workload := waitForWorkloadCondition(tc, m, "success", "QuotaReserved", "True", time.Minute)
		request := waitForRequestCondition(tc, m, workload.GetName(), "Provisioned", "True", 10*time.Minute)
		class, _, _ := unstructured.NestedString(request.Object, "spec", "provisioningClassName")
		assert.Equal(tc, provisioningClass, class)

		workload = waitForWorkloadCondition(tc, m, "success", "Admitted", "True", time.Minute)
		assert.Equal(tc, "Ready", admissionCheckState(workload, afterCheck))
		assert.False(tc, conditionTime(workload, "Admitted").Before(conditionTime(request, "Provisioned")),
			"workload must not be admitted before its capacity is provisioned")

		require.NoError(tc, tc.WaitForRunningJobPods(m.namespace, "success", 1, 3*time.Minute))
		pods := jobPods(tc, m, "success")
		require.Len(tc, pods, 1)
		assert.Equal(tc, request.GetName(), pods[0].Annotations[consumeAnnotation])
		return
	}

	workload := waitForWorkloadCondition(tc, m, "success", "Admitted", "True", time.Minute)
	assert.Empty(tc, admissionCheckState(workload, afterCheck), "quota-only admission has no capacity check")
	assert.Empty(tc, requestsOwnedBy(tc, m, workload.GetName()), "quota-only admission creates no ProvisioningRequest")
	// The pod is created first and the autoscaler reacts to it being Pending.
	require.NoError(tc, tc.WaitForRunningJobPods(m.namespace, "success", 1, 10*time.Minute))
}

// timeoutScenario: the capacity never arrives.
//
// Before, the workload is admitted and resumed anyway, leaving an Unschedulable
// pod. After, the Job stays suspended with no pod and the ProvisioningRequest
// says why. In both modes the quota stays reserved, a second Job is starved,
// and nothing changes until the stuck Job is deleted.
func timeoutScenario(tc *e2e.TestContext, m mode) {
	registerDiagnostics(tc, m)
	submitJob(tc, m, "too-big", tooBigCPU, "sleep infinity")

	if m.gated {
		workload := waitForWorkloadCondition(tc, m, "too-big", "QuotaReserved", "True", time.Minute)
		request := waitForRequestCondition(tc, m, workload.GetName(), "Provisioned", "False", 2*time.Minute)
		_, reason, _ := condition(request, "Provisioned")
		assert.Equal(tc, "CapacityIsNotFound", reason)
		workload, err := workloadForJob(tc, m, "too-big")
		require.NoError(tc, err)
		assert.Equal(tc, "Pending", admissionCheckState(workload, afterCheck))
		assert.True(tc, jobSuspended(tc, m, "too-big"), "job must stay suspended while capacity is missing")
		assert.Empty(tc, jobPods(tc, m, "too-big"), "no pod may be created before capacity is provisioned")
	} else {
		waitForWorkloadCondition(tc, m, "too-big", "Admitted", "True", time.Minute)
		waitForUnschedulablePod(tc, m, "too-big", time.Minute)
		assert.False(tc, jobSuspended(tc, m, "too-big"), "quota-only admission resumes the job without capacity")
	}

	// The reserved quota is unusable, and it blocks the next job.
	submitJob(tc, m, "starved", tooBigCPU, "sleep infinity")
	starved := waitForWorkloadCondition(tc, m, "starved", "QuotaReserved", "False", time.Minute)
	_, _, message := condition(starved, "QuotaReserved")
	assert.Contains(tc, message, "insufficient unused quota")

	// Nothing resolves this on its own within the observation window.
	require.Never(tc, func() bool {
		return !stillStuck(tc, m)
	}, 30*time.Second, 5*time.Second, "stuck job must keep its quota and the second job must stay starved")

	// Deleting the stuck job is what frees the quota.
	deleteJob(tc, m, "too-big")
	waitForWorkloadCondition(tc, m, "starved", "QuotaReserved", "True", time.Minute)
}

// cancellationScenario: the Job is deleted while it waits for capacity.
//
// Before, there is a Pending pod to tear down. After, no pod was ever created
// and the ProvisioningRequest is garbage-collected with the workload.
func cancellationScenario(tc *e2e.TestContext, m mode) {
	registerDiagnostics(tc, m)
	submitJob(tc, m, "cancel", tooBigCPU, "sleep infinity")

	workload := waitForWorkloadCondition(tc, m, "cancel", "QuotaReserved", "True", time.Minute)
	if m.gated {
		waitForRequestCondition(tc, m, workload.GetName(), "Accepted", "True", 2*time.Minute)
		assert.Empty(tc, jobPods(tc, m, "cancel"), "no pod exists while the request is pending")
	} else {
		waitForUnschedulablePod(tc, m, "cancel", time.Minute)
	}

	deleteJob(tc, m, "cancel")

	waitForWorkloadGone(tc, m, workload.GetName(), time.Minute)
	waitForNoRequests(tc, m, workload.GetName(), time.Minute)
	waitForNoJobPods(tc, m, "cancel", time.Minute)
	waitForQueueIdle(tc, m, time.Minute)
}

// cleanupScenario: the Job runs to completion; its reservations are released.
func cleanupScenario(tc *e2e.TestContext, m mode) {
	registerDiagnostics(tc, m)
	submitJob(tc, m, "cleanup", fitsCPU, "sleep 5")

	workload := waitForWorkloadCondition(tc, m, "cleanup", "QuotaReserved", "True", time.Minute)
	if m.gated {
		waitForRequestCondition(tc, m, workload.GetName(), "Provisioned", "True", 10*time.Minute)
	}

	waitForWorkloadCondition(tc, m, "cleanup", "Finished", "True", 10*time.Minute)
	waitForQueueIdle(tc, m, time.Minute)
	// Kueue removes the ProvisioningRequest once the workload has finished.
	waitForNoRequests(tc, m, workload.GetName(), 3*time.Minute)
}

// --- Helpers ---

func registerDiagnostics(tc *e2e.TestContext, m mode) {
	tc.Helper()
	tc.OnFailure(func() {
		tc.DumpPods(m.namespace, "")
		tc.DumpJobs(m.namespace)
		tc.DumpEvents(m.namespace)
	})
}

// submitJob creates a suspended single-pod Job on the mode's LocalQueue and
// registers cleanup that waits for its Workload to disappear, so the next test
// starts with the queue's quota free.
func submitJob(tc *e2e.TestContext, m mode, name, cpu, command string) {
	tc.Helper()
	suspend := true
	backoffLimit := int32(0)
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: m.namespace,
			Labels:    map[string]string{"kueue.x-k8s.io/queue-name": localQueue},
		},
		Spec: batchv1.JobSpec{
			Suspend:      &suspend,
			BackoffLimit: &backoffLimit,
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					Containers: []corev1.Container{{
						Name:    "worker",
						Image:   "busybox:1.36",
						Command: []string{"sh", "-c", command},
						Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse(cpu)},
						},
					}},
				},
			},
		},
	}
	_, err := tc.KubeClient().BatchV1().Jobs(m.namespace).Create(tc.Ctx(), job, metav1.CreateOptions{})
	require.NoError(tc, err)
	tc.Cleanup(func() {
		deleteJob(tc, m, name)
		deadline := time.Now().Add(time.Minute)
		for time.Now().Before(deadline) {
			workloads, err := tc.DynamicClient().Resource(workloadGVR).Namespace(m.namespace).List(tc.Ctx(), metav1.ListOptions{})
			if err == nil && !ownsAny(workloads.Items, "Job", name) {
				return
			}
			time.Sleep(pollInterval)
		}
	})
}

func deleteJob(tc *e2e.TestContext, m mode, name string) {
	tc.Helper()
	propagation := metav1.DeletePropagationBackground
	err := tc.KubeClient().BatchV1().Jobs(m.namespace).Delete(tc.Ctx(), name, metav1.DeleteOptions{PropagationPolicy: &propagation})
	if err != nil && !apierrors.IsNotFound(err) {
		tc.Errorf("deleting job %s/%s: %v", m.namespace, name, err)
	}
}

func jobSuspended(tc *e2e.TestContext, m mode, name string) bool {
	tc.Helper()
	job, err := tc.KubeClient().BatchV1().Jobs(m.namespace).Get(tc.Ctx(), name, metav1.GetOptions{})
	require.NoError(tc, err)
	return job.Spec.Suspend != nil && *job.Spec.Suspend
}

func jobPods(tc *e2e.TestContext, m mode, jobName string) []corev1.Pod {
	tc.Helper()
	pods, err := tc.KubeClient().CoreV1().Pods(m.namespace).List(tc.Ctx(), metav1.ListOptions{
		LabelSelector: fmt.Sprintf("job-name=%s", jobName),
	})
	require.NoError(tc, err)
	return pods.Items
}

// stillStuck reports whether the timeout state is unchanged: the too-big job
// holds its quota without a running pod and the starved job has no quota.
func stillStuck(tc *e2e.TestContext, m mode) bool {
	stuck, err := workloadForJob(tc, m, "too-big")
	if err != nil {
		return false
	}
	if status, _, _ := condition(stuck, "QuotaReserved"); status != "True" {
		return false
	}
	if admitted, _, _ := condition(stuck, "Admitted"); m.gated && admitted == "True" {
		return false
	}
	for _, pod := range jobPods(tc, m, "too-big") {
		if pod.Status.Phase != corev1.PodPending {
			return false
		}
	}
	starved, err := workloadForJob(tc, m, "starved")
	if err != nil {
		return false
	}
	status, _, _ := condition(starved, "QuotaReserved")
	return status == "False"
}

func workloadForJob(tc *e2e.TestContext, m mode, jobName string) (*unstructured.Unstructured, error) {
	job, err := tc.KubeClient().BatchV1().Jobs(m.namespace).Get(tc.Ctx(), jobName, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	workloads, err := tc.DynamicClient().Resource(workloadGVR).Namespace(m.namespace).List(tc.Ctx(), metav1.ListOptions{
		LabelSelector: fmt.Sprintf("kueue.x-k8s.io/job-uid=%s", job.UID),
	})
	if err != nil {
		return nil, err
	}
	if len(workloads.Items) == 0 {
		return nil, fmt.Errorf("no workload found for job %s/%s", m.namespace, jobName)
	}
	return &workloads.Items[0], nil
}

func requestsOwnedBy(tc *e2e.TestContext, m mode, workloadName string) []unstructured.Unstructured {
	tc.Helper()
	requests, err := tc.DynamicClient().Resource(provisioningRequestGVR).Namespace(m.namespace).List(tc.Ctx(), metav1.ListOptions{})
	require.NoError(tc, err)
	var owned []unstructured.Unstructured
	for _, request := range requests.Items {
		if ownsAny([]unstructured.Unstructured{request}, "Workload", workloadName) {
			owned = append(owned, request)
		}
	}
	return owned
}

// ownsAny reports whether any object has an owner reference of the given kind
// and name.
func ownsAny(objects []unstructured.Unstructured, kind, name string) bool {
	for _, obj := range objects {
		for _, ref := range obj.GetOwnerReferences() {
			if ref.Kind == kind && ref.Name == name {
				return true
			}
		}
	}
	return false
}

func findCondition(obj *unstructured.Unstructured, condType string) map[string]interface{} {
	conditions, _, _ := unstructured.NestedSlice(obj.Object, "status", "conditions")
	for _, raw := range conditions {
		if cond, ok := raw.(map[string]interface{}); ok && cond["type"] == condType {
			return cond
		}
	}
	return nil
}

func condition(obj *unstructured.Unstructured, condType string) (status, reason, message string) {
	cond := findCondition(obj, condType)
	status, _ = cond["status"].(string)
	reason, _ = cond["reason"].(string)
	message, _ = cond["message"].(string)
	return status, reason, message
}

func conditionTime(obj *unstructured.Unstructured, condType string) time.Time {
	raw, _ := findCondition(obj, condType)["lastTransitionTime"].(string)
	parsed, _ := time.Parse(time.RFC3339, raw)
	return parsed
}

func admissionCheckState(workload *unstructured.Unstructured, name string) string {
	checks, _, _ := unstructured.NestedSlice(workload.Object, "status", "admissionChecks")
	for _, raw := range checks {
		if check, ok := raw.(map[string]interface{}); ok && check["name"] == name {
			state, _ := check["state"].(string)
			return state
		}
	}
	return ""
}

// poll calls check every pollInterval until it returns true or the timeout
// expires. check also returns what it last saw, for the failure message.
func poll(tc *e2e.TestContext, timeout time.Duration, what string, check func() (bool, string)) {
	tc.Helper()
	deadline := time.Now().Add(timeout)
	last := "nothing observed"
	for {
		done, seen := check()
		if done {
			return
		}
		if seen != "" {
			last = seen
		}
		if time.Now().After(deadline) {
			tc.Fatalf("timed out after %s waiting for %s; last seen: %s", timeout, what, last)
		}
		time.Sleep(pollInterval)
	}
}

func waitForWorkloadCondition(tc *e2e.TestContext, m mode, jobName, condType, want string, timeout time.Duration) *unstructured.Unstructured {
	tc.Helper()
	var found *unstructured.Unstructured
	poll(tc, timeout, fmt.Sprintf("workload of job %s/%s to have %s=%s", m.namespace, jobName, condType, want), func() (bool, string) {
		workload, err := workloadForJob(tc, m, jobName)
		if err != nil {
			return false, err.Error()
		}
		status, reason, message := condition(workload, condType)
		if status == want {
			found = workload
			return true, ""
		}
		return false, fmt.Sprintf("%s=%q reason=%q message=%q", condType, status, reason, message)
	})
	return found
}

// waitForRequestCondition waits for the ProvisioningRequest owned by the
// workload to report the condition, and fails immediately on Failed=True.
func waitForRequestCondition(tc *e2e.TestContext, m mode, workloadName, condType, want string, timeout time.Duration) *unstructured.Unstructured {
	tc.Helper()
	var found *unstructured.Unstructured
	what := fmt.Sprintf("ProvisioningRequest of workload %s/%s to have %s=%s", m.namespace, workloadName, condType, want)
	poll(tc, timeout, what, func() (bool, string) {
		owned := requestsOwnedBy(tc, m, workloadName)
		if len(owned) == 0 {
			return false, "no ProvisioningRequest owned by the workload"
		}
		request := &owned[0]
		if status, reason, message := condition(request, "Failed"); status == "True" {
			tc.Fatalf("ProvisioningRequest %s failed: %s: %s", request.GetName(), reason, message)
		}
		status, reason, _ := condition(request, condType)
		if status == want {
			found = request
			return true, ""
		}
		return false, fmt.Sprintf("%s %s=%q reason=%q", request.GetName(), condType, status, reason)
	})
	return found
}

func waitForUnschedulablePod(tc *e2e.TestContext, m mode, jobName string, timeout time.Duration) {
	tc.Helper()
	poll(tc, timeout, fmt.Sprintf("a pod of job %s/%s to be Pending and Unschedulable", m.namespace, jobName), func() (bool, string) {
		pods := jobPods(tc, m, jobName)
		if len(pods) == 0 {
			return false, "no pods"
		}
		pod := pods[0]
		for _, cond := range pod.Status.Conditions {
			if cond.Type == corev1.PodScheduled && cond.Status == corev1.ConditionFalse && cond.Reason == corev1.PodReasonUnschedulable {
				return pod.Status.Phase == corev1.PodPending, fmt.Sprintf("pod %s phase=%s", pod.Name, pod.Status.Phase)
			}
		}
		return false, fmt.Sprintf("pod %s phase=%s, not marked Unschedulable", pod.Name, pod.Status.Phase)
	})
}

func waitForWorkloadGone(tc *e2e.TestContext, m mode, workloadName string, timeout time.Duration) {
	tc.Helper()
	poll(tc, timeout, fmt.Sprintf("workload %s/%s to be deleted", m.namespace, workloadName), func() (bool, string) {
		_, err := tc.DynamicClient().Resource(workloadGVR).Namespace(m.namespace).Get(tc.Ctx(), workloadName, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return true, ""
		}
		if err != nil {
			return false, err.Error()
		}
		return false, "workload still exists"
	})
}

func waitForNoRequests(tc *e2e.TestContext, m mode, workloadName string, timeout time.Duration) {
	tc.Helper()
	poll(tc, timeout, fmt.Sprintf("no ProvisioningRequest owned by workload %s/%s", m.namespace, workloadName), func() (bool, string) {
		owned := requestsOwnedBy(tc, m, workloadName)
		if len(owned) == 0 {
			return true, ""
		}
		return false, fmt.Sprintf("%s still exists", owned[0].GetName())
	})
}

func waitForNoJobPods(tc *e2e.TestContext, m mode, jobName string, timeout time.Duration) {
	tc.Helper()
	poll(tc, timeout, fmt.Sprintf("pods of job %s/%s to be deleted", m.namespace, jobName), func() (bool, string) {
		pods := jobPods(tc, m, jobName)
		return len(pods) == 0, fmt.Sprintf("%d pods remain", len(pods))
	})
}

// waitForQueueIdle waits until the LocalQueue has nothing pending, reserving
// quota, or admitted.
func waitForQueueIdle(tc *e2e.TestContext, m mode, timeout time.Duration) {
	tc.Helper()
	poll(tc, timeout, fmt.Sprintf("localqueue %s/%s to be idle", m.namespace, localQueue), func() (bool, string) {
		queue, err := tc.DynamicClient().Resource(localQueueGVR).Namespace(m.namespace).Get(tc.Ctx(), localQueue, metav1.GetOptions{})
		if err != nil {
			return false, err.Error()
		}
		pending, _, _ := unstructured.NestedInt64(queue.Object, "status", "pendingWorkloads")
		reserving, _, _ := unstructured.NestedInt64(queue.Object, "status", "reservingWorkloads")
		admitted, _, _ := unstructured.NestedInt64(queue.Object, "status", "admittedWorkloads")
		return pending == 0 && reserving == 0 && admitted == 0,
			fmt.Sprintf("pending=%d reserving=%d admitted=%d", pending, reserving, admitted)
	})
}
