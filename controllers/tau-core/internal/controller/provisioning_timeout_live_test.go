// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package controller

import (
	"context"
	"os"
	"testing"
	"time"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
)

// TestProvisioningTimeoutLive runs only the ProvisioningTimeoutReconciler
// against the current kubeconfig, for clusters where the full controller is not
// deployed. It is a harness for tests/e2e/provisioning, not a test in itself:
//
//	TAU_PROVISIONING_TIMEOUT_LIVE=45s TAU_PROVISIONING_TIMEOUT_LIVE_RUN=5m \
//	  go test -run '^TestProvisioningTimeoutLive$' -timeout 10m ./internal/controller/
func TestProvisioningTimeoutLive(t *testing.T) {
	raw := os.Getenv("TAU_PROVISIONING_TIMEOUT_LIVE")
	if raw == "" {
		t.Skip("Skipping: set TAU_PROVISIONING_TIMEOUT_LIVE=<duration> to run the reconciler against the current cluster")
	}
	timeout, err := time.ParseDuration(raw)
	if err != nil || timeout <= 0 {
		t.Fatalf("TAU_PROVISIONING_TIMEOUT_LIVE=%q is not a positive duration", raw)
	}
	runFor := 5 * time.Minute
	if raw := os.Getenv("TAU_PROVISIONING_TIMEOUT_LIVE_RUN"); raw != "" {
		if runFor, err = time.ParseDuration(raw); err != nil {
			t.Fatalf("TAU_PROVISIONING_TIMEOUT_LIVE_RUN=%q: %v", raw, err)
		}
	}

	ctrl.SetLogger(zap.New(zap.UseDevMode(true)))
	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:                 testScheme(t),
		Metrics:                metricsserver.Options{BindAddress: "0"},
		HealthProbeBindAddress: "0",
	})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	if err := (&ProvisioningTimeoutReconciler{
		Client:   mgr.GetClient(),
		Timeout:  timeout,
		Recorder: mgr.GetEventRecorderFor("tau-core-controller"),
	}).SetupWithManager(mgr); err != nil {
		t.Fatalf("SetupWithManager: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), runFor)
	defer cancel()
	if err := mgr.Start(ctx); err != nil {
		t.Fatalf("manager: %v", err)
	}
}
