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
	"reflect"
	"slices"
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

const PropletFinalizerName = "propeller.propeller.abstractmachines.fr/finalizer"

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
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete

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

	if proplet.DeletionTimestamp != nil {
		return r.handlePropletDeletion(ctx, &proplet)
	}

	// Add finalizer if not present
	if !controllerutil.ContainsFinalizer(&proplet, PropletFinalizerName) {
		controllerutil.AddFinalizer(&proplet, PropletFinalizerName)
		if err := r.Update(ctx, &proplet); err != nil {
			logger.Error(err, "Failed to add finalizer")

			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}

	var result ctrl.Result
	var err error

	switch proplet.Spec.Type {
	case propellerv1.K8sProplet:
		result, err = r.reconcileK8sProplet(ctx, &proplet)
	case propellerv1.ExternalProplet:
		result, err = r.reconcileExternalProplet(ctx, &proplet)
	default:
		err = fmt.Errorf("unknown proplet type: %s", proplet.Spec.Type)
		logger.Error(err, "unknown proplet type")

		return ctrl.Result{RequeueAfter: time.Minute * 5}, err
	}

	if err != nil {
		logger.Error(err, "Reconciliation failed")
		// On error, retry with exponential backoff, capped at 10 minutes
		if result.RequeueAfter == 0 {
			result.RequeueAfter = time.Minute
		}
		if result.RequeueAfter > time.Minute*10 {
			result.RequeueAfter = time.Minute * 10
		}
	}

	return result, err
}

func (r *PropletReconciler) handlePropletDeletion(ctx context.Context, proplet *propellerv1.Proplet) (ctrl.Result, error) {
	logger := logf.FromContext(ctx).WithValues("proplet", proplet.Name, "type", proplet.Spec.Type)
	logger.Info("Handling proplet deletion")

	if proplet.Spec.Type == propellerv1.K8sProplet {
		deploymentName := types.NamespacedName{
			Name:      fmt.Sprintf("%s-proplet", proplet.Name),
			Namespace: proplet.Namespace,
		}

		deployment := &appsv1.Deployment{}
		err := r.Get(ctx, deploymentName, deployment)
		if err == nil {
			logger.Info("Deleting associated deployment")

			if err := r.Delete(ctx, deployment); err != nil {
				logger.Error(err, "Failed to delete deployment")

				return ctrl.Result{}, err
			}
		} else if !apierrors.IsNotFound(err) {
			logger.Error(err, "Failed to get deployment for deletion")

			return ctrl.Result{}, err
		}
	}

	if controllerutil.ContainsFinalizer(proplet, PropletFinalizerName) {
		controllerutil.RemoveFinalizer(proplet, PropletFinalizerName)
		if err := r.Update(ctx, proplet); err != nil {
			logger.Error(err, "Failed to remove finalizer")

			return ctrl.Result{}, err
		}
	}

	logger.Info("Proplet deletion completed")

	return ctrl.Result{}, nil
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
	} else {
		// Check if deployment needs to be updated
		desiredDeployment := r.buildPropletDeployment(proplet)
		if err := controllerutil.SetControllerReference(proplet, desiredDeployment, r.Scheme); err != nil {
			return ctrl.Result{}, err
		}

		if r.deploymentNeedsUpdate(deployment, desiredDeployment) {
			logger.Info("Updating proplet deployment")

			// Update important fields
			deployment.Spec.Replicas = desiredDeployment.Spec.Replicas
			deployment.Spec.Template = desiredDeployment.Spec.Template
			deployment.Labels = desiredDeployment.Labels

			if err := r.Update(ctx, deployment); err != nil {
				logger.Error(err, "Failed to update deployment")
				return ctrl.Result{}, err
			}
		}
	}

	if err := r.updateK8sPropletStatus(ctx, proplet, deployment); err != nil {
		logger.Error(err, "Failed to update proplet status")
		// Don't fail the reconciliation if status update fails, but retry sooner
		return ctrl.Result{RequeueAfter: time.Second * 30}, nil
	}

	// Determine requeue interval based on deployment status
	requeueInterval := r.livelinessInterval
	if deployment.Status.ReadyReplicas == 0 {
		// If no replicas are ready, check more frequently
		requeueInterval = time.Minute
	}

	return ctrl.Result{RequeueAfter: requeueInterval}, nil
}

func (r *PropletReconciler) buildPropletDeployment(proplet *propellerv1.Proplet) *appsv1.Deployment {
	replicas := int32(1)
	if proplet.Spec.K8s.Replicas != nil {
		replicas = *proplet.Spec.K8s.Replicas
	}

	deployment := &appsv1.Deployment{
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
									Name:  "PROPLET_TENANT_ID",
									Value: proplet.Spec.ConnectionConfig.DomainID,
								},
								{
									Name:  "PROPLET_CHANNEL_ID",
									Value: proplet.Spec.ConnectionConfig.ChannelID,
								},
								{
									Name:  "PROPLET_ENTITY_ID",
									Value: proplet.Spec.ConnectionConfig.ClientID,
								},
								{
									Name:  "PROPLET_API_KEY",
									Value: proplet.Spec.ConnectionConfig.ClientKey,
								},
							},
						},
					},
				},
			},
		},
	}

	return deployment
}

func (r *PropletReconciler) deploymentNeedsUpdate(current, desired *appsv1.Deployment) bool {
	if *current.Spec.Replicas != *desired.Spec.Replicas {
		return true
	}

	if len(current.Spec.Template.Spec.Containers) > 0 && len(desired.Spec.Template.Spec.Containers) > 0 {
		if current.Spec.Template.Spec.Containers[0].Image != desired.Spec.Template.Spec.Containers[0].Image {
			return true
		}

		if !reflect.DeepEqual(current.Spec.Template.Spec.Containers[0].Env, desired.Spec.Template.Spec.Containers[0].Env) {
			return true
		}

		if !reflect.DeepEqual(current.Spec.Template.Spec.Containers[0].Resources, desired.Spec.Template.Spec.Containers[0].Resources) {
			return true
		}
	}

	if !reflect.DeepEqual(current.Labels, desired.Labels) {
		return true
	}

	return false
}

func (r *PropletReconciler) updateK8sPropletStatus(ctx context.Context, proplet *propellerv1.Proplet, deployment *appsv1.Deployment) error {
	logger := logf.FromContext(ctx).WithValues("proplet", proplet.Name, "type", proplet.Spec.Type)

	proplet.Status.K8sStatus = &propellerv1.K8sStatus{
		ReadyReplicas:     deployment.Status.ReadyReplicas,
		AvailableReplicas: deployment.Status.AvailableReplicas,
	}

	now := metav1.Now()

	desiredReplicas := int32(1)
	if deployment.Spec.Replicas != nil {
		desiredReplicas = *deployment.Spec.Replicas
	}

	switch {
	case deployment.Status.ReadyReplicas == desiredReplicas && deployment.Status.ReadyReplicas > 0:
		proplet.Status.Phase = propellerv1.PropletRunningPhase
		r.setCondition(proplet, propellerv1.PropletConditionReady, metav1.ConditionTrue, "DeploymentReady",
			fmt.Sprintf("All %d replicas are ready and available", deployment.Status.ReadyReplicas))
	case deployment.Status.ReadyReplicas > 0:
		proplet.Status.Phase = propellerv1.PropletInitializingPhase
		r.setCondition(proplet, propellerv1.PropletConditionReady, metav1.ConditionFalse, "DeploymentPartiallyReady",
			fmt.Sprintf("Deployment has %d ready replicas out of %d desired", deployment.Status.ReadyReplicas, desiredReplicas))
	case deployment.Status.Replicas > 0:
		proplet.Status.Phase = propellerv1.PropletInitializingPhase
		r.setCondition(proplet, propellerv1.PropletConditionReady, metav1.ConditionFalse, "DeploymentNotReady",
			fmt.Sprintf("Deployment has %d replicas but none are ready", deployment.Status.Replicas))
	default:
		proplet.Status.Phase = propellerv1.PropletInitializingPhase
		r.setCondition(proplet, propellerv1.PropletConditionReady, metav1.ConditionFalse, "DeploymentScalingUp",
			"Deployment is scaling up from zero replicas")
	}

	for _, cond := range deployment.Status.Conditions {
		switch cond.Type {
		case appsv1.DeploymentAvailable:
			switch cond.Status == corev1.ConditionTrue {
			case true:
				r.setCondition(proplet, propellerv1.PropletConditionHealthy, metav1.ConditionTrue, "DeploymentAvailable",
					fmt.Sprintf("Deployment is available: %s", cond.Message))
			default:
				r.setCondition(proplet, propellerv1.PropletConditionHealthy, metav1.ConditionFalse, "DeploymentNotAvailable",
					fmt.Sprintf("Deployment is not available: %s", cond.Message))
			}

		case appsv1.DeploymentProgressing:
			if cond.Status == corev1.ConditionFalse {
				r.setCondition(proplet, propellerv1.PropletConditionHealthy, metav1.ConditionFalse, "DeploymentStalled",
					fmt.Sprintf("Deployment is not progressing: %s", cond.Message))
			}
		case appsv1.DeploymentReplicaFailure:
			if cond.Status == corev1.ConditionTrue {
				r.setCondition(proplet, propellerv1.PropletConditionHealthy, metav1.ConditionFalse, "ReplicaFailure",
					fmt.Sprintf("Deployment has replica failures: %s", cond.Message))
			}
		}
	}

	// Add connection status for k8s proplets (they're connected if deployment exists)
	r.setCondition(proplet, propellerv1.PropletConditionConnected, metav1.ConditionTrue, "DeploymentExists",
		"K8s proplet is managed through deployment")
	proplet.Status.LastSeen = &now

	// Update task count
	if err := r.updateTaskCount(ctx, proplet); err != nil {
		logger.Error(err, "Failed to update task count")
		// Don't fail reconciliation for task count errors
	}

	if len(deployment.Spec.Template.Spec.Containers) > 0 {
		container := deployment.Spec.Template.Spec.Containers[0]
		proplet.Status.AvailableResources = &propellerv1.PropletResources{
			CPU:    container.Resources.Requests.Cpu().String(),
			Memory: container.Resources.Requests.Memory().String(),
		}

		if len(container.Resources.Requests) > 2 {
			custom := make(map[string]string)
			for name, quantity := range container.Resources.Requests {
				if name != corev1.ResourceCPU && name != corev1.ResourceMemory {
					custom[string(name)] = quantity.String()
				}
			}
			if len(custom) > 0 {
				proplet.Status.AvailableResources.Custom = custom
			}
		}
	}

	return r.updatePropletStatus(ctx, proplet)
}

func (r *PropletReconciler) reconcileExternalProplet(ctx context.Context, proplet *propellerv1.Proplet) (ctrl.Result, error) {
	logger := logf.FromContext(ctx).WithValues("proplet", proplet.Name, "type", proplet.Spec.Type)

	if proplet.Spec.External == nil {
		return ctrl.Result{}, fmt.Errorf("external spec is required for external proplet type")
	}

	switch {
	case proplet.Status.LastSeen != nil:
		timeSinceLastSeen := time.Since(proplet.Status.LastSeen.Time)
		switch {
		case timeSinceLastSeen > r.lastSeenThreshold:
			proplet.Status.Phase = propellerv1.PropletOfflinePhase
			r.setCondition(proplet, propellerv1.PropletConditionConnected, metav1.ConditionFalse, "PropletOffline",
				fmt.Sprintf("Proplet offline for %s (threshold: %s)", timeSinceLastSeen.String(), r.lastSeenThreshold.String()))
			r.setCondition(proplet, propellerv1.PropletConditionReady, metav1.ConditionFalse, "PropletOffline",
				"External proplet is offline")
		default:
			proplet.Status.Phase = propellerv1.PropletRunningPhase
			r.setCondition(proplet, propellerv1.PropletConditionConnected, metav1.ConditionTrue, "PropletOnline",
				fmt.Sprintf("Proplet last seen %s ago", timeSinceLastSeen.String()))
			r.setCondition(proplet, propellerv1.PropletConditionReady, metav1.ConditionTrue, "PropletReady",
				"External proplet is ready and connected")
		}
	default:
		proplet.Status.Phase = propellerv1.PropletInitializingPhase
		r.setCondition(proplet, propellerv1.PropletConditionReady, metav1.ConditionFalse, "PropletInitializing",
			"Waiting for first connection from external proplet")
		r.removeCondition(proplet, propellerv1.PropletConditionConnected)
	}

	// Update resource availability for external proplets
	if proplet.Spec.External != nil {
		if proplet.Status.AvailableResources == nil {
			proplet.Status.AvailableResources = &propellerv1.PropletResources{}
		}

		// Set resources from spec if available
		if len(proplet.Spec.Resource) > 0 {
			if cpu := proplet.Spec.Resource.Cpu(); cpu != nil {
				proplet.Status.AvailableResources.CPU = cpu.String()
			}
			if memory := proplet.Spec.Resource.Memory(); memory != nil {
				proplet.Status.AvailableResources.Memory = memory.String()
			}

			// Add custom resources
			custom := make(map[string]string)
			for name, quantity := range proplet.Spec.Resource {
				if name != corev1.ResourceCPU && name != corev1.ResourceMemory {
					custom[string(name)] = quantity.String()
				}
			}
			if len(custom) > 0 {
				proplet.Status.AvailableResources.Custom = custom
			}
		}
	}

	if err := r.updateTaskCount(ctx, proplet); err != nil {
		logger.Error(err, "Failed to update task count")
		// Don't fail reconciliation for task count errors
	}

	if err := r.updatePropletStatus(ctx, proplet); err != nil {
		logger.Error(err, "Failed to update external proplet status")
		// Don't fail the reconciliation if status update fails, but retry sooner
		return ctrl.Result{RequeueAfter: time.Second * 30}, nil
	}

	// Determine requeue interval based on proplet status
	requeueInterval := r.livelinessInterval
	if proplet.Status.Phase == propellerv1.PropletOfflinePhase {
		// If proplet is offline, check more frequently
		requeueInterval = time.Minute
	}

	return ctrl.Result{RequeueAfter: requeueInterval}, nil
}

func (r *PropletReconciler) updatePropletStatus(ctx context.Context, proplet *propellerv1.Proplet) error {
	logger := logf.FromContext(ctx).WithValues("proplet", proplet.Name)

	const maxRetries = 3
	for i := range maxRetries {
		if err := r.Status().Update(ctx, proplet); err != nil {
			if apierrors.IsConflict(err) && i < maxRetries-1 {
				logger.Info("Status update conflict, retrying", "attempt", i+1)
				// Get fresh version of the proplet
				fresh := &propellerv1.Proplet{}
				if getErr := r.Get(ctx, types.NamespacedName{
					Name:      proplet.Name,
					Namespace: proplet.Namespace,
				}, fresh); getErr != nil {
					return getErr
				}

				fresh.Status = proplet.Status
				*proplet = *fresh

				time.Sleep(time.Millisecond * 100 * time.Duration(i+1))
				continue
			}
			return err
		}
		return nil
	}
	return fmt.Errorf("failed to update status after %d retries", maxRetries)
}

// setCondition sets or updates a condition in the proplet's status
func (r *PropletReconciler) setCondition(proplet *propellerv1.Proplet, conditionType propellerv1.PropletConditionType, status metav1.ConditionStatus, reason, message string) {
	now := metav1.Now()

	for i, existing := range proplet.Status.Conditions {
		if existing.Type == conditionType {
			// Only update if status changed or message changed significantly
			if existing.Status != status || existing.Reason != reason {
				proplet.Status.Conditions[i] = propellerv1.PropletCondition{
					Type:               conditionType,
					Status:             status,
					LastTransitionTime: now,
					Reason:             reason,
					Message:            message,
				}
			} else if existing.Message != message {
				// Update message without changing transition time
				proplet.Status.Conditions[i].Message = message
			}
			return
		}
	}

	// Condition doesn't exist, add it
	proplet.Status.Conditions = append(proplet.Status.Conditions, propellerv1.PropletCondition{
		Type:               conditionType,
		Status:             status,
		LastTransitionTime: now,
		Reason:             reason,
		Message:            message,
	})
}

// removeCondition removes a condition from the proplet's status
func (r *PropletReconciler) removeCondition(proplet *propellerv1.Proplet, conditionType propellerv1.PropletConditionType) {
	for i, existing := range proplet.Status.Conditions {
		if existing.Type == conditionType {
			proplet.Status.Conditions = append(proplet.Status.Conditions[:i], proplet.Status.Conditions[i+1:]...)
			return
		}
	}
}

// updateTaskCount counts and updates the number of tasks assigned to this proplet
func (r *PropletReconciler) updateTaskCount(ctx context.Context, proplet *propellerv1.Proplet) error {
	var tasks propellerv1.TaskList
	if err := r.List(ctx, &tasks); err != nil {
		return fmt.Errorf("failed to list tasks: %w", err)
	}

	taskCount := uint64(0)
	activeTasks := uint64(0)

	for _, task := range tasks.Items {
		// Check if task is assigned to this proplet
		switch task.Status.AssignedProplet {
		case proplet.Name:
			taskCount++
			// Count tasks that are not completed or failed
			if task.Status.Phase != propellerv1.TaskCompletedPhase && task.Status.Phase != propellerv1.TaskFailedPhase {
				activeTasks++
			}
		case "":
			// For unassigned tasks, we could track potential matches
			// but currently we only count actually assigned tasks
			_ = r.propletMatchesTaskSelector(proplet, &task)
		}
	}

	proplet.Status.TaskCount = taskCount

	if activeTasks > 0 {
		r.setCondition(proplet, propellerv1.PropletConditionHealthy, metav1.ConditionTrue, "ProcessingTasks",
			fmt.Sprintf("Proplet is processing %d active tasks", activeTasks))
	}

	return nil
}

// propletMatchesTaskSelector checks if a proplet matches a task's selector requirements
func (r *PropletReconciler) propletMatchesTaskSelector(proplet *propellerv1.Proplet, task *propellerv1.Task) bool {
	if task.Spec.PropletSelector == nil {
		return true
	}

	selector := task.Spec.PropletSelector

	// Check specific proplet ID
	if selector.PropletID != "" && selector.PropletID != proplet.Name {
		return false
	}

	// Check preferred proplet type
	if task.Spec.PreferredPropletType != propellerv1.AnyProplet {
		if (task.Spec.PreferredPropletType == propellerv1.K8sProplet && proplet.Spec.Type != propellerv1.K8sProplet) ||
			(task.Spec.PreferredPropletType == propellerv1.ExternalProplet && proplet.Spec.Type != propellerv1.ExternalProplet) {
			return false
		}
	}

	// Check match labels
	if len(selector.MatchLabels) > 0 {
		for key, value := range selector.MatchLabels {
			if proplet.Labels == nil || proplet.Labels[key] != value {
				return false
			}
		}
	}

	// For external proplets, check device type and capabilities
	if proplet.Spec.Type == propellerv1.ExternalProplet && proplet.Spec.External != nil {
		// Check device types
		if len(selector.MatchDeviceTypes) > 0 {
			if !slices.Contains(selector.MatchDeviceTypes, proplet.Spec.External.DeviceType) {
				return false
			}
		}

		// Check capabilities
		if len(selector.MatchCapabilities) > 0 {
			propletCapabilities := make(map[string]bool)
			for _, cap := range proplet.Spec.External.Capabilities {
				propletCapabilities[cap] = true
			}

			for _, requiredCap := range selector.MatchCapabilities {
				if !propletCapabilities[requiredCap] {
					return false
				}
			}
		}
	}

	return true
}

func (r *PropletReconciler) mqttLivenessHandler(ctx context.Context, msg map[string]any) error {
	propletId, ok := msg["proplet_id"].(string)
	if !ok {
		return errors.New("invalid proplet id")
	}
	if propletId == "" {
		return errors.New("proplet id is empty")
	}
	logger := logf.FromContext(ctx).WithValues("proplet_id", propletId)

	// Search across all namespaces since namespace is not provided in MQTT message
	var proplets propellerv1.PropletList
	if err := r.List(ctx, &proplets); err != nil {
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

	if err := r.updatePropletStatus(ctx, proplet); err != nil {
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

	logger := logf.FromContext(ctx).WithValues("task_id", taskID)

	// Search across all namespaces to find the task
	var tasks propellerv1.TaskList
	if err := r.List(ctx, &tasks); err != nil {
		return err
	}

	// Find the task with matching ID
	var task *propellerv1.Task
	for i := range tasks.Items {
		if tasks.Items[i].Name == taskID || tasks.Items[i].UID == types.UID(taskID) {
			task = &tasks.Items[i]
			break
		}
	}

	if task == nil {
		logger.Info("Task resource not found, ignoring")
		return nil
	}

	task.Status.Results = fmt.Sprintf("%v", msg["results"])
	task.Status.Phase = propellerv1.TaskCompletedPhase
	task.Status.FinishedAt = &metav1.Time{Time: time.Now()}

	if err := r.Status().Update(ctx, task); err != nil {
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
