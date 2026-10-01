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
	"k8s.io/apimachinery/pkg/runtime"
)

type (
	// +kubebuilder:validation:Enum=parallel;sequential;configurable
	ExecutionMode string
	// +kubebuilder:validation:Enum=Pending;Running;Completed;Failed
	JobPhase string
)

const (
	ExecutionModeParallel     ExecutionMode = "parallel"
	ExecutionModeSequential   ExecutionMode = "sequential"
	ExecutionModeConfigurable ExecutionMode = "configurable"

	JobPhasePending   JobPhase = "Pending"
	JobPhaseRunning   JobPhase = "Running"
	JobPhaseCompleted JobPhase = "Completed"
	JobPhaseFailed    JobPhase = "Failed"
)

// PropellerJobSpec defines the desired state of PropellerJob
type PropellerJobSpec struct {
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`

	// ExecutionMode determines how tasks are executed
	// +kubebuilder:validation:Enum=parallel;sequential;configurable
	// +kubebuilder:default="configurable"
	ExecutionMode ExecutionMode `json:"executionMode,omitempty"`

	// Tasks is a list of task specifications to execute as part of this job
	Tasks []TaskSpec `json:"tasks,omitempty"`

	// TaskRefs references existing Task resources by name
	TaskRefs []string `json:"taskRefs,omitempty"`
}

// PropellerJobStatus defines the observed state of PropellerJob
type PropellerJobStatus struct {
	// +kubebuilder:default="Pending"
	Phase            JobPhase     `json:"phase"`
	StartTime        *metav1.Time `json:"startTime,omitempty"`
	FinishTime       *metav1.Time `json:"finishTime,omitempty"`
	CreatedAt        *metav1.Time `json:"createdAt,omitempty"`
	UpdatedAt        *metav1.Time `json:"updatedAt,omitempty"`
	TaskCount        int          `json:"taskCount,omitempty"`
	CompletedCount   int          `json:"completedCount,omitempty"`
	FailedCount      int          `json:"failedCount,omitempty"`
	SkippedCount     int          `json:"skippedCount,omitempty"`
	InterruptedCount int          `json:"interruptedCount,omitempty"`
	// Conditions represent the latest available observations of the job's state
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,shortName=pjob
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Mode",type=string,JSONPath=`.spec.executionMode`
// +kubebuilder:printcolumn:name="Tasks",type=integer,JSONPath=`.status.taskCount`
// +kubebuilder:printcolumn:name="Completed",type=integer,JSONPath=`.status.completedCount`
// +kubebuilder:printcolumn:name="Failed",type=integer,JSONPath=`.status.failedCount`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// PropellerJob is the Schema for the propellerjobs API
// PropellerJob groups multiple Tasks for coordinated execution with DAG support
type PropellerJob struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   PropellerJobSpec   `json:"spec,omitempty"`
	Status PropellerJobStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// PropellerJobList contains a list of PropellerJob
type PropellerJobList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []PropellerJob `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &PropellerJob{}, &PropellerJobList{})
		return nil
	})
}
