package v1

import (
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type FederatedJobSpec struct {
	ExperimentID  string            `json:"experimentId"`
	ModelRef      string            `json:"modelRef"`
	TaskWasmImage string            `json:"taskWasmImage"`
	Participants  []ParticipantSpec `json:"participants"`
	// +kubebuilder:pruning:PreserveUnknownFields
	Hyperparams    *apiextensionsv1.JSON `json:"hyperparams,omitempty"`
	KOfN           int                   `json:"kOfN"`
	TimeoutSeconds int                   `json:"timeoutSeconds"`
	Rounds         RoundConfig           `json:"rounds"`
	Aggregator     AggregatorConfig      `json:"aggregator"`
}

type ParticipantSpec struct {
	PropletID string `json:"propletId"`
}

type RoundConfig struct {
	Total    int    `json:"total"`
	Strategy string `json:"strategy,omitempty"`
}

type AggregatorConfig struct {
	Algorithm string `json:"algorithm"`
	// +kubebuilder:pruning:PreserveUnknownFields
	Config *apiextensionsv1.JSON `json:"config,omitempty"`
}

type FederatedJobStatus struct {
	Phase              string              `json:"phase,omitempty"`
	CurrentRound       int                 `json:"currentRound,omitempty"`
	CompletedRounds    int                 `json:"completedRounds,omitempty"`
	Conditions         []metav1.Condition  `json:"conditions,omitempty"`
	Participants       []ParticipantStatus `json:"participants,omitempty"`
	AggregatedModelRef string              `json:"aggregatedModelRef,omitempty"`
}

type ParticipantStatus struct {
	PropletID  string       `json:"propletId"`
	Status     string       `json:"status,omitempty"`
	LastUpdate *metav1.Time `json:"lastUpdate,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Current Round",type=integer,JSONPath=`.status.currentRound`
// +kubebuilder:printcolumn:name="Completed Rounds",type=integer,JSONPath=`.status.completedRounds`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
// FederatedJob is the Schema for the federatedjobs API
type FederatedJob struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata"`

	Spec   FederatedJobSpec   `json:"spec"`
	Status FederatedJobStatus `json:"status"`
}

// +kubebuilder:object:root=true
type FederatedJobList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata"`

	Items []FederatedJob `json:"items"`
}

//nolint:gochecknoinits // init() is required for Kubernetes scheme registration
func init() {
	SchemeBuilder.Register(&FederatedJob{}, &FederatedJobList{})
}
