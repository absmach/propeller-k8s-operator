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
	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

type (
	// +kubebuilder:validation:Enum=k8s;external;any
	PropletKind string
	// +kubebuilder:validation:Enum=pending;scheduled;running;completed;failed
	TaskPhase string
	// +kubebuilder:validation:Enum=Scheduled;Started;Completed
	TaskConditionType string
)

const (
	K8sProplet      PropletKind = "k8s"
	ExternalProplet PropletKind = "external"
	AnyProplet      PropletKind = "any"

	TaskPendingPhase   TaskPhase = "pending"
	TaskScheduledPhase TaskPhase = "scheduled"
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
	// +kubebuilder:validation:Pattern=`^([0-9]+(\.[0-9]*)?(m|))|([0-9]+m?)$`
	CPU string `json:"cpu,omitempty"`
	// +kubebuilder:validation:Pattern=`^[0-9]+(\.[0-9]*)?(Ki|Mi|Gi|Ti|Pi|Ei|k|M|G|T|P|E)?$`
	Memory string            `json:"memory,omitempty"`
	Custom map[string]string `json:"custom,omitempty"`
}

type TaskSpec struct {
	// INSERT ADDITIONAL SPEC FIELDS - desired state of cluster
	// Important: Run "make" to regenerate code after modifying this file
	// The following markers will use OpenAPI v3 schema to validate the value
	// More info: https://book.kubebuilder.io/reference/markers/crd-validation.html

	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	Name string `json:"name"`
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	FunctionName    string           `json:"functionName"`
	ImageURL        string           `json:"imageUrl,omitempty"`
	File            []byte           `json:"file,omitempty"`
	CLIArgs         []string         `json:"cliArgs,omitempty"`
	Inputs          []uint64         `json:"inputs,omitempty"`
	PropletSelector *PropletSelector `json:"propletSelector,omitempty,omitzero"`
	// +kubebuilder:validation:Enum=k8s;external;any
	// +kubebuilder:default="any"
	PreferredPropletType PropletKind          `json:"preferredPropletType,omitempty"`
	ResourceRequirements *PropletResources    `json:"resourceRequirements,omitempty,omitzero"`
	Env                  map[string]string    `json:"env,omitempty"`
	Daemon               bool                 `json:"daemon,omitempty"`
	Mode                 string               `json:"mode,omitempty"`
	MonitoringProfile    *MonitoringProfile   `json:"monitoringProfile,omitempty"`
	RestartPolicy        corev1.RestartPolicy `json:"restartPolicy,omitempty"`
}

type MonitoringProfile struct {
	Enabled  bool     `json:"enabled,omitempty"`
	Interval int      `json:"interval,omitempty"`
	Metrics  []string `json:"metrics,omitempty"`
}

type TaskCondition struct {
	Type               TaskConditionType      `json:"type"`
	Status             metav1.ConditionStatus `json:"status"`
	LastTransitionTime metav1.Time            `json:"lastTransitionTime"`
	Reason             string                 `json:"reason,omitempty"`
	Message            string                 `json:"message,omitempty"`
}

type TaskStatus struct {
	// INSERT ADDITIONAL STATUS FIELD - define observed state of cluster
	// Important: Run "make" to regenerate code after modifying this file

	// +kubebuilder:default="pending"
	Phase           TaskPhase    `json:"phase"`
	AssignedProplet string       `json:"assignedProplet,omitempty"`
	CreatedAt       *metav1.Time `json:"createdAt,omitempty,omitzero"`
	UpdatedAt       *metav1.Time `json:"updatedAt,omitempty,omitzero"`
	StartedAt       *metav1.Time `json:"startedAt,omitempty,omitzero"`
	FinishedAt      *metav1.Time `json:"finishedAt,omitempty,omitzero"`
	// +kubebuilder:pruning:PreserveUnknownFields
	Results    *apiextensionsv1.JSON `json:"results,omitempty"`
	Error      string                `json:"error,omitempty"`
	Conditions []TaskCondition       `json:"conditions,omitzero"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Function",type=string,JSONPath=`.spec.functionName`
// +kubebuilder:printcolumn:name="Proplet",type=string,JSONPath=`.status.assignedProplet`
// +kubebuilder:printcolumn:name="Start Time",type=date,JSONPath=`.status.startedAt`
// +kubebuilder:printcolumn:name="Finish Time",type=date,JSONPath=`.status.finishedAt`
// +kubebuilder:printcolumn:name="Preferred Type",type=string,JSONPath=`.spec.preferredPropletType`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

type Task struct {
	metav1.TypeMeta `json:",inline"`

	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty,omitzero"`

	// +required
	Spec TaskSpec `json:"spec"`

	// +optional
	Status TaskStatus `json:"status,omitempty,omitzero"`
}

// +kubebuilder:object:root=true

type TaskList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Task `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Task{}, &TaskList{})
}
