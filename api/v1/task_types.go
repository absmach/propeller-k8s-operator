/*
Copyright 2025.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

type (
	PropletKind       string
	TaskPhase         string
	TaskConditionType string
)

const (
	K8sProplet      PropletKind = "k8s"
	ExternalProplet PropletKind = "external"
	AnyProplet      PropletKind = "any"

	TaskPendingPhase   TaskPhase = "pending"
	TashScheduledPhase TaskPhase = "scheduled"
	TaskRunningPhase   TaskPhase = "running"
	TaskCompletedPhase TaskPhase = "completed"
	TaskFailedPhase    TaskPhase = "failed"

	ScheduledType TaskConditionType = "Scheduled"
	StartedType   TaskConditionType = "Started"
	CompletedType TaskConditionType = "Completed"
)

type PropletSelector struct {
	PropletID         string            `json:"propletId,omitempty"`
	MatchLabels       map[string]string `json:"matchLabels,omitempty"`
	MatchDeviceTypes  []string          `json:"matchDeviceTypes,omitempty"`
	MatchCapabilities []string          `json:"matchCapabilities,omitempty"`
}

type PropletResources struct {
	// CPU capacity (e.g., "1000m" for 1 CPU core)
	CPU string `json:"cpu,omitempty"`
	// Memory capacity (e.g., "1Gi")
	Memory string `json:"memory,omitempty"`
	// Custom resource constraints
	Custom map[string]string `json:"custom,omitempty"`
}

// TaskSpec defines the desired state of Task
type TaskSpec struct {
	// INSERT ADDITIONAL SPEC FIELDS - desired state of cluster
	// Important: Run "make" to regenerate code after modifying this file
	// The following markers will use OpenAPI v3 schema to validate the value
	// More info: https://book.kubebuilder.io/reference/markers/crd-validation.html

	Name            string           `json:"name"`
	ImageURL        string           `json:"imageUrl,omitempty"`
	File            []byte           `json:"file,omitempty"`
	CLIArgs         []string         `json:"cliArgs,omitempty"`
	Inputs          []string         `json:"inputs,omitempty"`
	PropletSelector *PropletSelector `json:"propletSelector,omitzero"`
	// +kubebuilder:validation:Enum=k8s;external;any
	PreferredPropletType PropletKind       `json:"preferredPropletType,omitempty"`
	ResourceRequirements *PropletResources `json:"resourceRequirements,omitzero"`
}

type TaskCondition struct {
	Type               TaskConditionType      `json:"type"`
	Status             metav1.ConditionStatus `json:"status"`
	LastTransitionTime metav1.Time            `json:"lastTransitionTime"`
	Reason             string                 `json:"reason,omitempty"`
	Message            string                 `json:"message,omitempty"`
}

// TaskStatus defines the observed state of Task.
type TaskStatus struct {
	// INSERT ADDITIONAL STATUS FIELD - define observed state of cluster
	// Important: Run "make" to regenerate code after modifying this file

	Phase           TaskPhase       `json:"phase"`
	AssignedProplet string          `json:"assignedProplet,omitempty"`
	CreatedAt       *metav1.Time    `json:"createdAt,omitzero"`
	UpdatedAt       *metav1.Time    `json:"updatedAt,omitzero"`
	StartedAt       *metav1.Time    `json:"startedAt,omitzero"`
	FinishedAt      *metav1.Time    `json:"finishedAt,omitzero"`
	Results         string          `json:"results,omitempty"`
	Error           string          `json:"error,omitempty"`
	Conditions      []TaskCondition `json:"conditions,omitzero"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Proplet",type=string,JSONPath=`.status.assignedProplet`
// +kubebuilder:printcolumn:name="Start Time",type=date,JSONPath=`.status.startedAt`
// +kubebuilder:printcolumn:name="Duration",type=string,JSONPath=`.status.finishedAt`

// Task is the Schema for the tasks API
type Task struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty,omitzero"`

	// spec defines the desired state of Task
	// +required
	Spec TaskSpec `json:"spec"`

	// status defines the observed state of Task
	// +optional
	Status TaskStatus `json:"status,omitempty,omitzero"`
}

// +kubebuilder:object:root=true

// TaskList contains a list of Task
type TaskList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Task `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Task{}, &TaskList{})
}
