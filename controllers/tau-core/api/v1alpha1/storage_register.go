// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package v1alpha1

func init() {
	SchemeBuilder.Register(&DataIngestion{}, &DataIngestionList{})
}
