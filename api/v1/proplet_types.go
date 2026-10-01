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
	"k8s.io/apimachinery/pkg/runtime"
)

// PropletMetadata holds runtime metadata reported by a proplet over MQTT.
type PropletMetadata struct {
	Description      string   `json:"description,omitempty"`
	Tags             []string `json:"tags,omitempty"`
	Location         string   `json:"location,omitempty"`
	IP               string   `json:"ip,omitempty"`
	Environment      string   `json:"environment,omitempty"`
	OS               string   `json:"os,omitempty"`
	Hostname         string   `json:"hostname,omitempty"`
	CPUArch          string   `json:"cpuArch,omitempty"`
	TotalMemoryBytes uint64   `json:"totalMemoryBytes,omitempty"`
	PropletVersion   string   `json:"propletVersion,omitempty"`
	WasmRuntime      string   `json:"wasmRuntime,omitempty"`
}

// PropletMetricsSnapshot holds the latest metrics sample received from a proplet.
// CPU and memory percentages are stored as millipercent (value × 1000) to avoid
// floating-point fields which are not well-supported in CRD schemas.
type PropletMetricsSnapshot struct {
	Timestamp *metav1.Time `json:"timestamp,omitempty"`
	// CPUMilliPercent is CPU utilisation × 1000 (e.g. 50000 = 50 %).
	CPUMilliPercent int64 `json:"cpuMilliPercent,omitempty"`
	// MemoryMilliPercent is memory utilisation × 1000.
	MemoryMilliPercent int64  `json:"memoryMilliPercent,omitempty"`
	MemoryBytes        uint64 `json:"memoryBytes,omitempty"`
}

// TaskMetricsSnapshot holds the latest metrics sample for a running task.
// CPU and memory percentages are stored as millipercent (value × 1000).
type TaskMetricsSnapshot struct {
	Timestamp *metav1.Time `json:"timestamp,omitempty"`
	// CPUMilliPercent is CPU utilisation × 1000 (e.g. 50000 = 50 %).
	CPUMilliPercent int64 `json:"cpuMilliPercent,omitempty"`
	// MemoryMilliPercent is memory utilisation × 1000.
	MemoryMilliPercent int64  `json:"memoryMilliPercent,omitempty"`
	MemoryBytes        uint64 `json:"memoryBytes,omitempty"`
}

type (
	// +kubebuilder:validation:Enum=Initializing;Running;Offline
	PropletPhase string
	// +kubebuilder:validation:Enum=Ready;Connected;Healthy
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

// PropletEnvConfig holds optional environment overrides for a k8s proplet.
type PropletEnvConfig struct {
	// LivelinessInterval sets PROPLET_LIVELINESS_INTERVAL (default 10s).
	LivelinessInterval *metav1.Duration `json:"livelinessInterval,omitempty"`
	// MetricsInterval sets PROPLET_METRICS_INTERVAL (default 10s).
	MetricsInterval *metav1.Duration `json:"metricsInterval,omitempty"`
	// MetricsPort sets PROPLET_METRICS_PORT (default 9092).
	MetricsPort *int32 `json:"metricsPort,omitempty"`
	// MetricsEnabled sets PROPLET_METRICS_ENABLED (default true).
	MetricsEnabled *bool `json:"metricsEnabled,omitempty"`
	// ExternalWasmRuntime sets PROPLET_EXTERNAL_WASM_RUNTIME (path to an
	// external wasmtime binary). A pointer so an explicit empty string
	// (forcing the proplet's built-in runtime, distinct from an external
	// wasmtime subprocess) can be distinguished from the field being unset.
	ExternalWasmRuntime *string `json:"externalWasmRuntime,omitempty"`
	// HalEnabled sets PROPLET_HAL_ENABLED.
	HalEnabled *bool `json:"halEnabled,omitempty"`
	// HttpEnabled sets PROPLET_HTTP_ENABLED.
	HttpEnabled *bool `json:"httpEnabled,omitempty"`
	// UsbEnabled sets PROPLET_USB_ENABLED.
	UsbEnabled *bool `json:"usbEnabled,omitempty"`
	// OtelURL sets PROPLET_OTEL_URL for OpenTelemetry.
	OtelURL string `json:"otelUrl,omitempty"`
	// TraceRatio sets PROPLET_TRACE_RATIO as a string (e.g. "0.5").
	TraceRatio string `json:"traceRatio,omitempty"`
	// Tags sets PROPLET_TAGS as a comma-separated list.
	Tags string `json:"tags,omitempty"`
	// Location sets PROPLET_LOCATION.
	Location string `json:"location,omitempty"`
	// Description sets PROPLET_DESCRIPTION.
	Description string `json:"description,omitempty"`
	// KbsURI sets PROPLET_KBS_URI for confidential computing.
	KbsURI string `json:"kbsUri,omitempty"`
	// AaConfigPath sets PROPLET_AA_CONFIG_PATH for confidential computing.
	AaConfigPath string `json:"aaConfigPath,omitempty"`
	// MqttTLSCACert sets PROPLET_MQTT_TLS_CA_CERT.
	MqttTLSCACert string `json:"mqttTlsCaCert,omitempty"`
	// MqttTLSClientCert sets PROPLET_MQTT_TLS_CLIENT_CERT.
	MqttTLSClientCert string `json:"mqttTlsClientCert,omitempty"`
	// MqttTLSClientKey sets PROPLET_MQTT_TLS_CLIENT_KEY.
	MqttTLSClientKey string `json:"mqttTlsClientKey,omitempty"`
	// MqttTLSInsecureSkipVerify sets PROPLET_MQTT_TLS_INSECURE_SKIP_VERIFY.
	MqttTLSInsecureSkipVerify *bool `json:"mqttTlsInsecureSkipVerify,omitempty"`
}

// K8sPropletSpec defines the configuration for a Kubernetes-backed Proplet.
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
	// PluginDir is the path inside the proplet container where WASM plugin files are loaded from.
	// Maps to the PROPLET_PLUGIN_DIR environment variable. Leave empty to disable plugins.
	PluginDir string `json:"pluginDir,omitempty"`
	// Env holds optional environment overrides for the proplet container.
	Env *PropletEnvConfig `json:"env,omitempty,omitzero"`
}

// ConnectionConfig defines the MQTT connection configuration for a Proplet.
type ConnectionConfig struct {
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	TenantID string `json:"tenantId"`
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	ChannelID string `json:"channelId"`
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	EntityID string `json:"entityId"`
	// APIKey is the MQTT API key. Exactly one of APIKey or
	// APIKeySecretRef must be set.
	APIKey string `json:"apiKey,omitempty"`
	// APIKeySecretRef references a Kubernetes Secret whose key holds the
	// MQTT API key. Exactly one of APIKey or APIKeySecretRef must
	// be set.
	APIKeySecretRef *corev1.SecretKeySelector `json:"apiKeySecretRef,omitempty"`
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
	// +kubebuilder:validation:Optional
	Resource corev1.ResourceList `json:"resources,omitempty,omitzero"`
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
	// +kubebuilder:validation:Minimum=0
	ReadyReplicas int32 `json:"readyReplicas"`
	// +kubebuilder:validation:Minimum=0
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
	Alive      bool               `json:"alive"`
	// AliveHistory stores recent heartbeat timestamps for liveness tracking
	AliveHistory []metav1.Time `json:"aliveHistory,omitempty"`
	// +kubebuilder:default=0
	TaskCount          uint64            `json:"taskCount"`
	AvailableResources *PropletResources `json:"availableResources,omitempty"`
	K8sStatus          *K8sStatus        `json:"k8sStatus,omitempty"`
	// Metadata holds runtime information reported by the proplet over MQTT.
	Metadata *PropletMetadata `json:"metadata,omitempty"`
	// LatestMetrics is the most recent metrics sample received from the proplet.
	LatestMetrics *PropletMetricsSnapshot `json:"latestMetrics,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced
// +kubebuilder:printcolumn:name="Type",type=string,JSONPath=`.spec.type`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`
// +kubebuilder:printcolumn:name="Tasks",type=integer,JSONPath=`.status.taskCount`
// +kubebuilder:printcolumn:name="Replicas",type=string,JSONPath=`.status.k8sStatus.readyReplicas`
// +kubebuilder:printcolumn:name="Last Seen",type=date,JSONPath=`.status.lastSeen`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

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
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &Proplet{}, &PropletList{})
		return nil
	})
}
