// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package v1alpha1

import "k8s.io/apimachinery/pkg/runtime"

func init() {
	SchemeBuilder.Register(func(scheme *runtime.Scheme) error {
		scheme.AddKnownTypes(GroupVersion, &DataFetch{}, &DataFetchList{})
		return nil
	})
}
