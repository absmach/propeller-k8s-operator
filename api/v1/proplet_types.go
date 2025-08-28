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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type (
	PropletPhase         string
	PropletConditionType string
)

const (
	PropletInitializingPhase PropletPhase = "Initializing"
	PropletRunningPhase      PropletPhase = "Running"
	PropletOfflinePhase      PropletPhase = "Offline"

	PropletConditionReady     PropletConditionType = "Ready"
	PropletConditionConnected PropletConditionType = "Connected"
	PropletConditionHealthy   PropletConditionType = "Healthy"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

type ExternalPropletSpec struct {
	// DeviceType specifies the type of external device (e.g., esp32, raspberry-pi)
	DeviceType   string   `json:"deviceType,omitempty"`
	Capabilities []string `json:"capabilities,omitempty"`
}

type K8sPropletSpec struct {
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	Image string `json:"image"`
	// +kubebuilder:validation:Enum=debug;info;warn;error
	// +kubebuilder:default="info"
	LogLevel string `json:"logLevel,omitempty"`
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=100
	// +kubebuilder:default=1
	Replicas *int32 `json:"replicas,omitempty"`
}

type ConnectionConfig struct {
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	DomainID string `json:"domainId"`
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	ChannelID string `json:"channelId"`
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	ClientID string `json:"clientId"`
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	ClientKey string `json:"clientKey"`
	// +kubebuilder:validation:Required
	MQTTAddress string `json:"mqttAddress"`
	// +kubebuilder:default="30s"
	MQTTTimeout *metav1.Duration `json:"mqttTimeout,omitempty,omitzero"`
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=2
	// +kubebuilder:default=0
	MQTTQoS uint8 `json:"mqttQos,omitempty"`
}

// PropletSpec defines the desired state of Proplet
type PropletSpec struct {
	// INSERT ADDITIONAL SPEC FIELDS - desired state of cluster
	// Important: Run "make" to regenerate code after modifying this file
	// The following markers will use OpenAPI v3 schema to validate the value
	// More info: https://book.kubebuilder.io/reference/markers/crd-validation.html

	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Enum=k8s;external
	Type     PropletKind          `json:"type"`
	External *ExternalPropletSpec `json:"external,omitempty,omitzero"`
	K8s      *K8sPropletSpec      `json:"k8s,omitempty,omitzero"`
	Resource corev1.ResourceList  `json:"resources,omitempty,omitzero"`
	// +kubebuilder:validation:Required
	ConnectionConfig ConnectionConfig `json:"connectionConfig"`
}

type PropletCondition struct {
	Type               PropletConditionType   `json:"type"`
	Status             metav1.ConditionStatus `json:"status"`
	LastTransitionTime metav1.Time            `json:"lastTransitionTime"`
	Reason             string                 `json:"reason,omitempty"`
	Message            string                 `json:"message,omitempty"`
}

type K8sStatus struct {
	ReadyReplicas     int32 `json:"readyReplicas"`
	AvailableReplicas int32 `json:"availableReplicas"`
}

// PropletStatus defines the observed state of Proplet.
type PropletStatus struct {
	// INSERT ADDITIONAL STATUS FIELD - define observed state of cluster
	// Important: Run "make" to regenerate code after modifying this file

	// +kubebuilder:default="Initializing"
	Phase      PropletPhase       `json:"phase"`
	Conditions []PropletCondition `json:"conditions,omitempty"`
	LastSeen   *metav1.Time       `json:"lastSeen,omitempty"`
	// +kubebuilder:default=0
	TaskCount          uint64            `json:"taskCount"`
	AvailableResources *PropletResources `json:"availableResources,omitempty"`
	K8sStatus          *K8sStatus        `json:"k8sStatus,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced
// +kubebuilder:printcolumn:name="Type",type=string,JSONPath=`.spec.type`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Tasks",type=integer,JSONPath=`.status.taskCount`
// +kubebuilder:printcolumn:name="Last Seen",type=date,JSONPath=`.status.lastSeen`

// Proplet is the Schema for the proplets API
type Proplet struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty,omitzero"`

	// spec defines the desired state of Proplet
	// +required
	Spec PropletSpec `json:"spec"`

	// status defines the observed state of Proplet
	// +optional
	Status PropletStatus `json:"status,omitempty,omitzero"`
}

// +kubebuilder:object:root=true

// PropletList contains a list of Proplet
type PropletList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Proplet `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Proplet{}, &PropletList{})
}
