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
	// +kubebuilder:validation:Enum=pending;scheduled;running;completed;failed;skipped;interrupted
	TaskPhase string
	// +kubebuilder:validation:Enum=Scheduled;Started;Completed
	TaskConditionType string
	// +kubebuilder:validation:Enum=standard;federated
	TaskKind string
	// +kubebuilder:validation:Enum=infer;train
	TaskMode string
	// +kubebuilder:validation:Enum=success;failure
	RunIfCondition string
)

const (
	K8sProplet      PropletKind = "k8s"
	ExternalProplet PropletKind = "external"
	AnyProplet      PropletKind = "any"

	TaskPendingPhase     TaskPhase = "pending"
	TaskScheduledPhase   TaskPhase = "scheduled"
	TaskRunningPhase     TaskPhase = "running"
	TaskCompletedPhase   TaskPhase = "completed"
	TaskFailedPhase      TaskPhase = "failed"
	TaskSkippedPhase     TaskPhase = "skipped"
	TaskInterruptedPhase TaskPhase = "interrupted"

	ScheduledType TaskConditionType = "Scheduled"
	StartedType   TaskConditionType = "Started"
	CompletedType TaskConditionType = "Completed"

	TaskKindStandard  TaskKind = "standard"
	TaskKindFederated TaskKind = "federated"

	TaskModeInfer TaskMode = "infer"
	TaskModeTrain TaskMode = "train"

	RunIfSuccess RunIfCondition = "success"
	RunIfFailure RunIfCondition = "failure"
)

const DefaultPriority = 50
const DefaultTimezone = "UTC"

type PropletSelector struct {
	PropletID         string            `json:"propletId,omitempty"`
	MatchLabels       map[string]string `json:"matchLabels,omitempty"`
	MatchDeviceTypes  []string          `json:"matchDeviceTypes,omitempty"`
	MatchCapabilities []string          `json:"matchCapabilities,omitempty"`
}

type PropletResources struct {
	// CPU capacity (e.g., "1000m" for 1 CPU core)
	// +kubebuilder:validation:Pattern=`^([0-9]+(\.[0-9]*)?(m|))|([0-9]+m?)$`
	CPU string `json:"cpu,omitempty"`
	// Memory capacity (e.g., "1Gi")
	// +kubebuilder:validation:Pattern=`^[0-9]+(\.[0-9]*)?(Ki|Mi|Gi|Ti|Pi|Ei|k|M|G|T|P|E)?$`
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

	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	Name string `json:"name"`
	// FunctionName is the name of the WASM function to invoke. Optional; if
	// omitted the proplet uses the task Name as the function identifier, which
	// is the behaviour of the main propeller manager.
	FunctionName string `json:"functionName,omitempty"`
	// +kubebuilder:validation:Enum=standard;federated
	// +kubebuilder:default="standard"
	Kind     TaskKind `json:"kind,omitempty"`
	ImageURL string   `json:"imageUrl,omitempty"`
	File     []byte   `json:"file,omitempty"`
	CLIArgs  []string `json:"cliArgs,omitempty"`
	// Inputs are task input arguments. Each element is a string but numeric
	// values are accepted and coerced — matching the FlexStrings behaviour of
	// the main propeller manager.
	Inputs          []string         `json:"inputs,omitempty"`
	PropletSelector *PropletSelector `json:"propletSelector,omitempty,omitzero"`
	// +kubebuilder:validation:Enum=k8s;external;any
	// +kubebuilder:default="any"
	PreferredPropletType PropletKind       `json:"preferredPropletType,omitempty"`
	ResourceRequirements *PropletResources `json:"resourceRequirements,omitempty,omitzero"`
	Env                  map[string]string `json:"env,omitempty"`
	Daemon               bool              `json:"daemon,omitempty"`
	// Broadcast sends the task to all available proplets simultaneously instead of a single selected one.
	// Mirrors task.Task.Broadcast in the propeller codebase.
	Broadcast         bool                 `json:"broadcast,omitempty"`
	Mode              TaskMode             `json:"mode,omitempty"`
	MonitoringProfile *MonitoringProfile   `json:"monitoringProfile,omitempty"`
	RestartPolicy     corev1.RestartPolicy `json:"restartPolicy,omitempty"`

	// Confidential computing fields
	Encrypted       bool   `json:"encrypted,omitempty"`
	KBSResourcePath string `json:"kbsResourcePath,omitempty"`

	// Workflow/DAG fields
	// DependsOn specifies task IDs that must complete before this task runs
	DependsOn []string `json:"dependsOn,omitempty"`
	// RunIf specifies when to run: "success" (default) or "failure"
	// +kubebuilder:validation:Enum=success;failure
	RunIf RunIfCondition `json:"runIf,omitempty"`
	// WorkflowID groups tasks into a workflow for DAG execution
	WorkflowID string `json:"workflowId,omitempty"`
	// JobID groups tasks into a job for batch execution
	JobID string `json:"jobId,omitempty"`

	// Scheduling fields
	// Schedule is a cron expression for recurring tasks
	Schedule string `json:"schedule,omitempty"`
	// IsRecurring indicates if the task should repeat after completion
	IsRecurring bool `json:"isRecurring,omitempty"`
	// Timezone for schedule interpretation (default: UTC)
	Timezone string `json:"timezone,omitempty"`
	// Priority (higher values = higher priority, default: 50)
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=100
	// +kubebuilder:default=50
	Priority int `json:"priority,omitempty"`

	// Metadata is arbitrary key-value data attached to the task.
	// Must be valid JSON and under 64 KB. Mirrors task.Task.Metadata in the propeller codebase.
	// +kubebuilder:pruning:PreserveUnknownFields
	// +kubebuilder:validation:Optional
	Metadata *apiextensionsv1.JSON `json:"metadata,omitempty"`
}

// MonitoringProfile defines monitoring configuration for task execution
type MonitoringProfile struct {
	Enabled                bool             `json:"enabled,omitempty"`
	Interval               *metav1.Duration `json:"interval,omitempty"`
	CollectCPU             bool             `json:"collectCpu,omitempty"`
	CollectMemory          bool             `json:"collectMemory,omitempty"`
	CollectDiskIO          bool             `json:"collectDiskIo,omitempty"`
	CollectThreads         bool             `json:"collectThreads,omitempty"`
	CollectFileDescriptors bool             `json:"collectFileDescriptors,omitempty"`
	ExportToMQTT           bool             `json:"exportToMqtt,omitempty"`
	RetainHistory          bool             `json:"retainHistory,omitempty"`
	HistorySize            int              `json:"historySize,omitempty"`
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

	// +kubebuilder:default="pending"
	Phase           TaskPhase    `json:"phase"`
	AssignedProplet string       `json:"assignedProplet,omitempty"`
	CreatedAt       *metav1.Time `json:"createdAt,omitempty,omitzero"`
	UpdatedAt       *metav1.Time `json:"updatedAt,omitempty,omitzero"`
	StartedAt       *metav1.Time `json:"startedAt,omitempty,omitzero"`
	FinishedAt      *metav1.Time `json:"finishedAt,omitempty,omitzero"`
	// NextRun is the next scheduled execution time for recurring tasks
	NextRun *metav1.Time `json:"nextRun,omitempty,omitzero"`
	// +kubebuilder:pruning:PreserveUnknownFields
	Results    *apiextensionsv1.JSON `json:"results,omitempty"`
	Error      string                `json:"error,omitempty"`
	Conditions []TaskCondition       `json:"conditions,omitzero"`
	// LatestMetrics is the most recent metrics sample reported by the proplet for this task.
	LatestMetrics *TaskMetricsSnapshot `json:"latestMetrics,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Mode",type=string,JSONPath=`.spec.mode`
// +kubebuilder:printcolumn:name="Proplet",type=string,JSONPath=`.status.assignedProplet`
// +kubebuilder:printcolumn:name="Start Time",type=date,JSONPath=`.status.startedAt`
// +kubebuilder:printcolumn:name="Finish Time",type=date,JSONPath=`.status.finishedAt`
// +kubebuilder:printcolumn:name="Preferred Type",type=string,JSONPath=`.spec.preferredPropletType`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

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

// stateTransitionMap defines valid task phase transitions
var stateTransitionMap = map[TaskPhase][]TaskPhase{
	TaskPendingPhase:     {TaskScheduledPhase, TaskRunningPhase, TaskCompletedPhase, TaskFailedPhase, TaskSkippedPhase},
	TaskScheduledPhase:   {TaskRunningPhase, TaskCompletedPhase, TaskFailedPhase, TaskSkippedPhase},
	TaskRunningPhase:     {TaskCompletedPhase, TaskFailedPhase, TaskInterruptedPhase},
	TaskCompletedPhase:   {TaskPendingPhase}, // Allow restart for recurring tasks
	TaskFailedPhase:      {TaskPendingPhase}, // Allow retry
	TaskSkippedPhase:     {},                 // Terminal state
	TaskInterruptedPhase: {TaskPendingPhase}, // Allow resume
}

// ValidStateTransition checks if a transition from src to dst phase is allowed
func ValidStateTransition(src, dst TaskPhase) bool {
	validTransitions, ok := stateTransitionMap[src]
	if !ok {
		return false
	}
	for _, valid := range validTransitions {
		if valid == dst {
			return true
		}
	}
	return false
}
