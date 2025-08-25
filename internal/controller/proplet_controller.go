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

package controller

import (
	"context"
	"errors"
	"fmt"
	"time"

	propellerv1 "github.com/absmach/propeller/api/v1"
	"github.com/absmach/propeller/internal/mqtt"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

var superMQBaseTopic = "m/%s/c/%s/messages"

// PropletReconciler reconciles a Proplet object
type PropletReconciler struct {
	client.Client
	Scheme             *runtime.Scheme
	livelinessInterval time.Duration
	lastSeenThreshold  time.Duration
	pubsub             mqtt.PubSub
	baseTopic          string
}

// +kubebuilder:rbac:groups=propeller.propeller.abstractmachines.fr,resources=proplets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=propeller.propeller.abstractmachines.fr,resources=proplets/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=propeller.propeller.abstractmachines.fr,resources=proplets/finalizers,verbs=update

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the Proplet object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.21.0/pkg/reconcile
func (r *PropletReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := logf.FromContext(ctx).WithValues("proplet", req.NamespacedName)

	var proplet propellerv1.Proplet
	if err := r.Get(ctx, req.NamespacedName, &proplet); err != nil {
		if apierrors.IsNotFound(err) {
			logger.Info("Proplet resource not found, ignoring")

			return ctrl.Result{}, nil
		}

		logger.Error(err, "unable to fetch Proplet")

		return ctrl.Result{}, err
	}

	switch proplet.Spec.Type {
	case propellerv1.K8sProplet:
		return r.reconcileK8sProplet(ctx, &proplet)
	case propellerv1.ExternalProplet:
		return r.reconcileExternalProplet(ctx, &proplet)
	default:
		err := fmt.Errorf("unknown proplet type: %s", proplet.Spec.Type)
		logger.Error(err, "unknown proplet type")

		return ctrl.Result{}, err
	}
}

func (r *PropletReconciler) reconcileK8sProplet(ctx context.Context, proplet *propellerv1.Proplet) (ctrl.Result, error) {
	logger := logf.FromContext(ctx).WithValues("proplet", proplet.Name, "type", proplet.Spec.Type)

	if proplet.Spec.K8s == nil {
		return ctrl.Result{}, fmt.Errorf("k8s spec is required for k8s proplet type")
	}

	deployment := &appsv1.Deployment{}
	deploymentName := types.NamespacedName{
		Name:      fmt.Sprintf("%s-proplet", proplet.Name),
		Namespace: proplet.Namespace,
	}

	err := r.Get(ctx, deploymentName, deployment)
	if err != nil && apierrors.IsNotFound(err) {
		// Create new deployment
		deployment = r.buildPropletDeployment(proplet)
		if err := controllerutil.SetControllerReference(proplet, deployment, r.Scheme); err != nil {
			return ctrl.Result{}, err
		}

		logger.Info("Creating proplet deployment")

		if err := r.Create(ctx, deployment); err != nil {
			logger.Error(err, "Failed to create deployment")

			return ctrl.Result{}, err
		}
	} else if err != nil {
		logger.Error(err, "Failed to get deployment")

		return ctrl.Result{}, err
	}

	if err := r.updateK8sPropletStatus(ctx, proplet, deployment); err != nil {
		logger.Error(err, "Failed to update proplet status")

		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

func (r *PropletReconciler) buildPropletDeployment(proplet *propellerv1.Proplet) *appsv1.Deployment {
	replicas := int32(1)
	if proplet.Spec.K8s.Replicas != nil {
		replicas = *proplet.Spec.K8s.Replicas
	}

	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-proplet", proplet.Name),
			Namespace: proplet.Namespace,
			Labels: map[string]string{
				"app.kubernetes.io/name":       "proplet",
				"app.kubernetes.io/instance":   proplet.Name,
				"app.kubernetes.io/component":  "worker",
				"propeller.absmach.fr/proplet": proplet.Name,
			},
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"propeller.absmach.fr/proplet": proplet.Name,
				},
			},
			Strategy: appsv1.DeploymentStrategy{
				RollingUpdate: &appsv1.RollingUpdateDeployment{
					MaxSurge: &intstr.IntOrString{
						Type:   intstr.String,
						StrVal: "25%",
					},
					MaxUnavailable: &intstr.IntOrString{
						Type:   intstr.String,
						StrVal: "25%",
					},
				},
				Type: appsv1.RollingUpdateDeploymentStrategyType,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"propeller.absmach.fr/proplet": proplet.Name,
					},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:            fmt.Sprintf("%s-proplet", proplet.Name),
							Image:           proplet.Spec.K8s.Image,
							ImagePullPolicy: corev1.PullAlways,
							Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{
									corev1.ResourceCPU:    *proplet.Spec.Resource.Cpu(),
									corev1.ResourceMemory: *proplet.Spec.Resource.Memory(),
								},
							},
							Env: []corev1.EnvVar{
								{
									Name:  "PROPLET_LOG_LEVEL",
									Value: proplet.Spec.K8s.LogLevel,
								},
								{
									Name:  "PROPLET_MQTT_ADDRESS",
									Value: proplet.Spec.ConnectionConfig.MQTTAddress,
								},
								{
									Name:  "PROPLET_MQTT_TIMEOUT",
									Value: proplet.Spec.ConnectionConfig.MQTTTimeout.Duration.String(),
								},
								{
									Name:  "PROPLET_MQTT_QOS",
									Value: fmt.Sprintf("%d", proplet.Spec.ConnectionConfig.MQTTQoS),
								},
								{
									Name:  "PROPLET_DOMAIN_ID",
									Value: proplet.Spec.ConnectionConfig.DomainID,
								},
								{
									Name:  "PROPLET_CHANNEL_ID",
									Value: proplet.Spec.ConnectionConfig.ChannelID,
								},
								{
									Name:  "PROPLET_CLIENT_ID",
									Value: proplet.Spec.ConnectionConfig.ClientID,
								},
								{
									Name:  "PROPLET_CLIENT_KEY",
									Value: proplet.Spec.ConnectionConfig.ClientKey,
								},
							},
						},
					},
				},
			},
		},
	}
}

func (r *PropletReconciler) updateK8sPropletStatus(ctx context.Context, proplet *propellerv1.Proplet, deployment *appsv1.Deployment) error {
	proplet.Status.K8sStatus = &propellerv1.K8sStatus{
		ReadyReplicas:     deployment.Status.ReadyReplicas,
		AvailableReplicas: deployment.Status.AvailableReplicas,
	}

	if deployment.Status.ReadyReplicas > 0 {
		proplet.Status.Phase = propellerv1.PropletRunningPhase
	} else {
		proplet.Status.Phase = propellerv1.PropletInitializingPhase
	}

	now := metav1.Now()
	proplet.Status.Conditions = []propellerv1.PropletCondition{
		{
			Type:               propellerv1.PropletConditionReady,
			Status:             metav1.ConditionTrue,
			LastTransitionTime: now,
			Reason:             "DeploymentReady",
			Message:            fmt.Sprintf("Deployment has %d ready replicas", deployment.Status.ReadyReplicas),
		},
	}
	proplet.Status.LastSeen = &now

	if len(deployment.Spec.Template.Spec.Containers) > 0 {
		proplet.Status.AvailableResources = &propellerv1.PropletResources{
			CPU:    deployment.Spec.Template.Spec.Containers[0].Resources.Requests.Cpu().String(),
			Memory: deployment.Spec.Template.Spec.Containers[0].Resources.Requests.Memory().String(),
		}
	}

	return r.Status().Update(ctx, proplet)
}

func (r *PropletReconciler) reconcileExternalProplet(ctx context.Context, proplet *propellerv1.Proplet) (ctrl.Result, error) {
	logger := logf.FromContext(ctx).WithValues("proplet", proplet.Name, "type", proplet.Spec.Type)

	if proplet.Spec.External == nil {
		return ctrl.Result{}, fmt.Errorf("external spec is required for external proplet type")
	}

	if proplet.Status.LastSeen != nil {
		timeSinceLastSeen := time.Since(proplet.Status.LastSeen.Time)
		if timeSinceLastSeen > r.lastSeenThreshold {
			proplet.Status.Phase = propellerv1.PropletOfflinePhase
		} else {
			proplet.Status.Phase = propellerv1.PropletRunningPhase
		}
	} else {
		proplet.Status.Phase = propellerv1.PropletInitializingPhase
	}

	if err := r.Status().Update(ctx, proplet); err != nil {
		logger.Error(err, "Failed to update external proplet status")

		return ctrl.Result{}, err
	}

	return ctrl.Result{RequeueAfter: r.livelinessInterval}, nil
}

func (r *PropletReconciler) mqttLivenessHandler(ctx context.Context, msg map[string]any) error {
	propletId, ok := msg["proplet_id"].(string)
	if !ok {
		return errors.New("invalid proplet id")
	}
	if propletId == "" {
		return errors.New("proplet id is empty")
	}
	namespace, ok := msg["namespace"].(string)
	if !ok {
		return errors.New("invalid namespace")
	}
	if namespace == "" {
		return errors.New("namespace is empty")
	}
	logger := logf.FromContext(ctx).WithValues("proplet_id", propletId)

	var proplets propellerv1.PropletList
	if err := r.List(ctx, &proplets, client.InNamespace(namespace)); err != nil {
		return err
	}

	var proplet *propellerv1.Proplet
	for i := range proplets.Items {
		if proplets.Items[i].Spec.ConnectionConfig.ClientID == propletId {
			proplet = &proplets.Items[i]
			break
		}
	}

	if proplet == nil {
		logger.Info("Proplet resource not found, ignoring")

		return nil
	}

	now := metav1.Now()
	proplet.Status.LastSeen = &now

	if proplet.Spec.Type == propellerv1.K8sProplet {
		deployment := &appsv1.Deployment{}
		deploymentName := types.NamespacedName{
			Name:      fmt.Sprintf("%s-proplet", proplet.Name),
			Namespace: proplet.Namespace,
		}

		if err := r.Get(ctx, deploymentName, deployment); err == nil {
			proplet.Status.K8sStatus = &propellerv1.K8sStatus{
				ReadyReplicas:     deployment.Status.ReadyReplicas,
				AvailableReplicas: deployment.Status.AvailableReplicas,
			}
			if deployment.Status.ReadyReplicas > 0 {
				proplet.Status.Phase = propellerv1.PropletRunningPhase
			} else {
				proplet.Status.Phase = propellerv1.PropletInitializingPhase
			}
			proplet.Status.Conditions = []propellerv1.PropletCondition{
				{
					Type:               propellerv1.PropletConditionReady,
					Status:             metav1.ConditionTrue,
					LastTransitionTime: now,
					Reason:             "DeploymentReady",
					Message:            fmt.Sprintf("Deployment has %d ready replicas", deployment.Status.ReadyReplicas),
				},
			}
			if len(deployment.Spec.Template.Spec.Containers) > 0 {
				proplet.Status.AvailableResources = &propellerv1.PropletResources{
					CPU:    deployment.Spec.Template.Spec.Containers[0].Resources.Requests.Cpu().String(),
					Memory: deployment.Spec.Template.Spec.Containers[0].Resources.Requests.Memory().String(),
				}
			}
		}
	}

	if proplet.Status.LastSeen != nil {
		timeSinceLastSeen := time.Since(proplet.Status.LastSeen.Time)
		if timeSinceLastSeen > r.lastSeenThreshold {
			proplet.Status.Phase = propellerv1.PropletOfflinePhase
		} else {
			proplet.Status.Phase = propellerv1.PropletRunningPhase
		}
	} else {
		proplet.Status.Phase = propellerv1.PropletInitializingPhase
	}

	proplet.Status.LastSeen = &now

	if err := r.Status().Update(ctx, proplet); err != nil {
		logger.Error(err, "Failed to update external proplet status")

		return err
	}

	return nil
}

func (r *PropletReconciler) mqttResultHandler(ctx context.Context, msg map[string]any) error {
	taskID, ok := msg["task_id"].(string)
	if !ok {
		return errors.New("invalid task id")
	}
	if taskID == "" {
		return errors.New("task id is empty")
	}

	var tasks propellerv1.TaskList
	if err := r.List(ctx, &tasks, client.InNamespace("default")); err != nil {
		return err
	}

	task := tasks.Items[0]

	task.Status.Results = fmt.Sprintf("%v", msg["results"])
	task.Status.Phase = propellerv1.TaskCompletedPhase
	task.Status.FinishedAt = &metav1.Time{Time: time.Now()}

	if err := r.Status().Update(ctx, &task); err != nil {
		return err
	}

	return nil
}

func (r *PropletReconciler) mqttHandler() func(topic string, msg map[string]any) error {
	return func(topic string, msg map[string]any) error {
		switch topic {
		case r.baseTopic + "/control/proplet/alive":
			return r.mqttLivenessHandler(context.Background(), msg)
		case r.baseTopic + "/control/proplet/results":
			return r.mqttResultHandler(context.Background(), msg)
		}

		return nil
	}
}

// SetupWithManager sets up the controller with the Manager.
func (r *PropletReconciler) SetupWithManager(
	domainID, channelID string, mgr ctrl.Manager, livelinessInterval, lastSeenThreshold time.Duration, pubsub mqtt.PubSub,
) error {
	r.livelinessInterval = livelinessInterval
	r.lastSeenThreshold = lastSeenThreshold
	r.pubsub = pubsub
	r.baseTopic = fmt.Sprintf(superMQBaseTopic, domainID, channelID)

	if err := r.pubsub.Subscribe(r.baseTopic+"/#", r.mqttHandler()); err != nil {
		return err
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&propellerv1.Proplet{}).
		Named("proplet").
		Complete(r)
}
