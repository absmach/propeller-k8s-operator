package v1

import (
	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type TrainingRoundSpec struct {
	RoundID         string                      `json:"roundId"`
	FederatedJobRef corev1.LocalObjectReference `json:"federatedJobRef"`
	ModelRef        string                      `json:"modelRef"`
	TaskWasmImage   string                      `json:"taskWasmImage"`
	Participants    []string                    `json:"participants"`
	// +kubebuilder:pruning:PreserveUnknownFields
	Hyperparams    *apiextensionsv1.JSON `json:"hyperparams,omitempty"`
	KOfN           int                   `json:"kOfN"`
	TimeoutSeconds int                   `json:"timeoutSeconds"`
}

type TrainingRoundStatus struct {
	Phase              string                   `json:"phase,omitempty"`
	StartTime          *metav1.Time             `json:"startTime,omitempty"`
	EndTime            *metav1.Time             `json:"endTime,omitempty"`
	Participants       []RoundParticipantStatus `json:"participants,omitempty"`
	UpdatesReceived    int                      `json:"updatesReceived,omitempty"`
	UpdatesRequired    int                      `json:"updatesRequired,omitempty"`
	AggregatedModelRef string                   `json:"aggregatedModelRef,omitempty"`
	Conditions         []metav1.Condition       `json:"conditions,omitempty"`
}

type RoundParticipantStatus struct {
	PropletID      string                  `json:"propletId"`
	TaskRef        *corev1.ObjectReference `json:"taskRef,omitempty"`
	Status         string                  `json:"status,omitempty"`
	UpdateReceived bool                    `json:"updateReceived,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Job",type=string,JSONPath=`.spec.federatedJobRef.name`
// +kubebuilder:printcolumn:name="Updates",type=integer,JSONPath=`.status.updatesReceived`
// +kubebuilder:printcolumn:name="Required",type=integer,JSONPath=`.status.updatesRequired`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
type TrainingRound struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata"`

	Spec TrainingRoundSpec `json:"spec"`
	// +optional
	Status TrainingRoundStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type TrainingRoundList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata"`

	Items []TrainingRound `json:"items"`
}

func init() {
	SchemeBuilder.Register(&TrainingRound{}, &TrainingRoundList{})
}
