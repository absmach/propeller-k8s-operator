package v1alpha1

import (
	propellerv1 "github.com/absmach/propeller/api/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type WasmTaskSpec struct {
	propellerv1.TaskSpec `json:",inline"`
}

type WasmTaskStatus struct {
	propellerv1.TaskStatus `json:",inline"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Function",type=string,JSONPath=`.spec.functionName`
// +kubebuilder:printcolumn:name="Proplet",type=string,JSONPath=`.status.assignedProplet`
// +kubebuilder:printcolumn:name="Start Time",type=date,JSONPath=`.status.startedAt`
// +kubebuilder:printcolumn:name="Finish Time",type=date,JSONPath=`.status.finishedAt`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

type WasmTask struct {
	metav1.TypeMeta `json:",inline"`

	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty,omitzero"`

	// +required
	Spec WasmTaskSpec `json:"spec"`

	// +optional
	Status WasmTaskStatus `json:"status,omitempty,omitzero"`
}

// +kubebuilder:object:root=true

type WasmTaskList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []WasmTask `json:"items"`
}

func init() {
	SchemeBuilder.Register(&WasmTask{}, &WasmTaskList{})
}
