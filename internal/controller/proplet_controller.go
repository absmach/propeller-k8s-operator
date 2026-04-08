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
	"sync"
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
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

var superMQBaseTopic = "m/%s/c/%s"

const PropletFinalizerName = "propeller.propeller.abstractmachines.fr/finalizer"

const aliveHistoryLimit = 10

// PropletReconciler reconciles a Proplet object.
type PropletReconciler struct {
	client.Client
	Scheme             *runtime.Scheme
	livelinessInterval time.Duration
	lastSeenThreshold  time.Duration
	pubsub             mqtt.PubSub
	baseTopic          string

	// propletEvents is the bridge between the MQTT goroutine and the reconcile
	// queue.  The MQTT liveness handler writes a GenericEvent here; the
	// WatchesRawSource below forwards it to the work queue so that the
	// Reconcile function — not the MQTT goroutine — performs all API writes.
	propletEvents chan event.GenericEvent

	// pendingHeartbeats stores the most recent heartbeat timestamp for a
	// proplet, keyed by its Kubernetes UID.  Written by the MQTT goroutine
	// and consumed (LoadAndDelete) inside Reconcile.
	pendingHeartbeats sync.Map
}

// +kubebuilder:rbac:groups=propeller.propeller.abstractmachines.fr,resources=proplets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=propeller.propeller.abstractmachines.fr,resources=proplets/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=propeller.propeller.abstractmachines.fr,resources=proplets/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete

func (r *PropletReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := logf.FromContext(ctx).WithValues("proplet", req.NamespacedName)

	var proplet propellerv1.Proplet
	if err := r.Get(ctx, req.NamespacedName, &proplet); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// Apply any heartbeat that arrived via MQTT since the last reconcile.
	// This must happen before deletion/type-specific logic so that the final
	// Status().Update() call in each branch persists the freshest data.
	r.applyPendingHeartbeat(&proplet)

	if proplet.DeletionTimestamp != nil {
		return r.handlePropletDeletion(ctx, &proplet)
	}

	if !controllerutil.ContainsFinalizer(&proplet, PropletFinalizerName) {
		controllerutil.AddFinalizer(&proplet, PropletFinalizerName)
		if err := r.Update(ctx, &proplet); err != nil {
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
		return ctrl.Result{RequeueAfter: 5 * time.Minute}, err
	}

	if err != nil {
		logger.Error(err, "reconciliation failed")
		if result.RequeueAfter == 0 {
			result.RequeueAfter = time.Minute
		}
		if result.RequeueAfter > 10*time.Minute {
			result.RequeueAfter = 10 * time.Minute
		}
	}

	return result, err
}

// applyPendingHeartbeat checks the pendingHeartbeats map for a timestamp stored
// by the MQTT liveness handler and, if present, updates LastSeen and AliveHistory
// on the in-memory proplet object.  The caller is responsible for persisting
// these changes via Status().Update().
func (r *PropletReconciler) applyPendingHeartbeat(proplet *propellerv1.Proplet) {
	ts, ok := r.pendingHeartbeats.LoadAndDelete(string(proplet.UID))
	if !ok {
		return
	}
	beatTime := ts.(metav1.Time)
	proplet.Status.LastSeen = &beatTime
	proplet.Status.AliveHistory = append(proplet.Status.AliveHistory, beatTime)
	if len(proplet.Status.AliveHistory) > aliveHistoryLimit {
		proplet.Status.AliveHistory = proplet.Status.AliveHistory[len(proplet.Status.AliveHistory)-aliveHistoryLimit:]
	}
}

func (r *PropletReconciler) handlePropletDeletion(ctx context.Context, proplet *propellerv1.Proplet) (ctrl.Result, error) {
	logger := logf.FromContext(ctx).WithValues("proplet", proplet.Name)
	logger.Info("handling proplet deletion")

	if proplet.Spec.Type == propellerv1.K8sProplet {
		deploymentName := types.NamespacedName{
			Name:      fmt.Sprintf("%s-proplet", proplet.Name),
			Namespace: proplet.Namespace,
		}
		deployment := &appsv1.Deployment{}
		if err := r.Get(ctx, deploymentName, deployment); err == nil {
			if err := r.Delete(ctx, deployment); err != nil {
				return ctrl.Result{}, err
			}
		} else if !apierrors.IsNotFound(err) {
			return ctrl.Result{}, err
		}
	}

	if controllerutil.ContainsFinalizer(proplet, PropletFinalizerName) {
		controllerutil.RemoveFinalizer(proplet, PropletFinalizerName)
		if err := r.Update(ctx, proplet); err != nil {
			return ctrl.Result{}, err
		}
	}

	return ctrl.Result{}, nil
}

func (r *PropletReconciler) reconcileK8sProplet(ctx context.Context, proplet *propellerv1.Proplet) (ctrl.Result, error) {
	logger := logf.FromContext(ctx).WithValues("proplet", proplet.Name)

	if proplet.Spec.K8s == nil {
		return ctrl.Result{}, fmt.Errorf("k8s spec is required for k8s proplet type")
	}

	deployment := &appsv1.Deployment{}
	deploymentName := types.NamespacedName{
		Name:      fmt.Sprintf("%s-proplet", proplet.Name),
		Namespace: proplet.Namespace,
	}

	err := r.Get(ctx, deploymentName, deployment)
	switch {
	case err != nil && apierrors.IsNotFound(err):
		deployment = r.buildPropletDeployment(proplet)
		if err := controllerutil.SetControllerReference(proplet, deployment, r.Scheme); err != nil {
			return ctrl.Result{}, err
		}
		logger.Info("creating proplet deployment")
		if err := r.Create(ctx, deployment); err != nil {
			return ctrl.Result{}, err
		}
	case err != nil:
		return ctrl.Result{}, err
	default:
		desired := r.buildPropletDeployment(proplet)
		if err := controllerutil.SetControllerReference(proplet, desired, r.Scheme); err != nil {
			return ctrl.Result{}, err
		}
		if r.deploymentNeedsUpdate(deployment, desired) {
			logger.Info("updating proplet deployment")
			deployment.Spec.Replicas = desired.Spec.Replicas
			deployment.Spec.Template = desired.Spec.Template
			deployment.Labels = desired.Labels
			if err := r.Update(ctx, deployment); err != nil {
				return ctrl.Result{}, err
			}
		}
	}

	if err := r.updateK8sPropletStatus(ctx, proplet, deployment); err != nil {
		logger.Error(err, "failed to update proplet status")
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}

	requeueInterval := r.livelinessInterval
	if deployment.Status.ReadyReplicas == 0 {
		requeueInterval = time.Minute
	}

	return ctrl.Result{RequeueAfter: requeueInterval}, nil
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
				Type: appsv1.RollingUpdateDeploymentStrategyType,
				RollingUpdate: &appsv1.RollingUpdateDeployment{
					MaxSurge:       &intstr.IntOrString{Type: intstr.String, StrVal: "25%"},
					MaxUnavailable: &intstr.IntOrString{Type: intstr.String, StrVal: "25%"},
				},
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{"propeller.absmach.fr/proplet": proplet.Name},
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
								{Name: "PROPLET_LOG_LEVEL", Value: proplet.Spec.K8s.LogLevel},
								{Name: "PROPLET_MQTT_ADDRESS", Value: proplet.Spec.ConnectionConfig.MQTTAddress},
								{Name: "PROPLET_MQTT_TIMEOUT", Value: proplet.Spec.ConnectionConfig.MQTTTimeout.Duration.String()},
								{Name: "PROPLET_MQTT_QOS", Value: fmt.Sprintf("%d", proplet.Spec.ConnectionConfig.MQTTQoS)},
								{Name: "PROPLET_DOMAIN_ID", Value: proplet.Spec.ConnectionConfig.DomainID},
								{Name: "PROPLET_CHANNEL_ID", Value: proplet.Spec.ConnectionConfig.ChannelID},
								{Name: "PROPLET_CLIENT_ID", Value: proplet.Spec.ConnectionConfig.ClientID},
								{Name: "PROPLET_CLIENT_KEY", Value: proplet.Spec.ConnectionConfig.ClientKey},
							},
						},
					},
				},
			},
		},
	}
}

func (r *PropletReconciler) deploymentNeedsUpdate(current, desired *appsv1.Deployment) bool {
	if *current.Spec.Replicas != *desired.Spec.Replicas {
		return true
	}
	if len(current.Spec.Template.Spec.Containers) > 0 && len(desired.Spec.Template.Spec.Containers) > 0 {
		cc := current.Spec.Template.Spec.Containers[0]
		dc := desired.Spec.Template.Spec.Containers[0]
		if cc.Image != dc.Image {
			return true
		}
		if !reflect.DeepEqual(cc.Env, dc.Env) {
			return true
		}
		if !reflect.DeepEqual(cc.Resources, dc.Resources) {
			return true
		}
	}
	return !reflect.DeepEqual(current.Labels, desired.Labels)
}

func (r *PropletReconciler) updateK8sPropletStatus(ctx context.Context, proplet *propellerv1.Proplet, deployment *appsv1.Deployment) error {
	logger := logf.FromContext(ctx).WithValues("proplet", proplet.Name)

	proplet.Status.K8sStatus = &propellerv1.K8sStatus{
		ReadyReplicas:     deployment.Status.ReadyReplicas,
		AvailableReplicas: deployment.Status.AvailableReplicas,
	}

	desiredReplicas := int32(1)
	if deployment.Spec.Replicas != nil {
		desiredReplicas = *deployment.Spec.Replicas
	}

	switch {
	case deployment.Status.ReadyReplicas == desiredReplicas && deployment.Status.ReadyReplicas > 0:
		proplet.Status.Phase = propellerv1.PropletRunningPhase
		r.setCondition(proplet, propellerv1.PropletConditionReady, metav1.ConditionTrue, "DeploymentReady",
			fmt.Sprintf("all %d replicas are ready", deployment.Status.ReadyReplicas))
	case deployment.Status.ReadyReplicas > 0:
		proplet.Status.Phase = propellerv1.PropletInitializingPhase
		r.setCondition(proplet, propellerv1.PropletConditionReady, metav1.ConditionFalse, "DeploymentPartiallyReady",
			fmt.Sprintf("%d/%d replicas ready", deployment.Status.ReadyReplicas, desiredReplicas))
	case deployment.Status.Replicas > 0:
		proplet.Status.Phase = propellerv1.PropletInitializingPhase
		r.setCondition(proplet, propellerv1.PropletConditionReady, metav1.ConditionFalse, "DeploymentNotReady",
			fmt.Sprintf("%d replicas exist but none are ready", deployment.Status.Replicas))
	default:
		proplet.Status.Phase = propellerv1.PropletInitializingPhase
		r.setCondition(proplet, propellerv1.PropletConditionReady, metav1.ConditionFalse, "DeploymentScalingUp",
			"scaling up from zero replicas")
	}

	for _, cond := range deployment.Status.Conditions {
		switch cond.Type {
		case appsv1.DeploymentAvailable:
			if cond.Status == corev1.ConditionTrue {
				r.setCondition(proplet, propellerv1.PropletConditionHealthy, metav1.ConditionTrue, "DeploymentAvailable", cond.Message)
			} else {
				r.setCondition(proplet, propellerv1.PropletConditionHealthy, metav1.ConditionFalse, "DeploymentNotAvailable", cond.Message)
			}
		case appsv1.DeploymentProgressing:
			if cond.Status == corev1.ConditionFalse {
				r.setCondition(proplet, propellerv1.PropletConditionHealthy, metav1.ConditionFalse, "DeploymentStalled", cond.Message)
			}
		case appsv1.DeploymentReplicaFailure:
			if cond.Status == corev1.ConditionTrue {
				r.setCondition(proplet, propellerv1.PropletConditionHealthy, metav1.ConditionFalse, "ReplicaFailure", cond.Message)
			}
		}
	}

	r.setCondition(proplet, propellerv1.PropletConditionConnected, metav1.ConditionTrue, "DeploymentExists",
		"K8s proplet managed via deployment")

	if len(deployment.Spec.Template.Spec.Containers) > 0 {
		container := deployment.Spec.Template.Spec.Containers[0]
		proplet.Status.AvailableResources = &propellerv1.PropletResources{
			CPU:    container.Resources.Requests.Cpu().String(),
			Memory: container.Resources.Requests.Memory().String(),
		}
	}

	if err := r.updateTaskCount(ctx, proplet); err != nil {
		logger.Error(err, "failed to update task count")
	}

	return r.updatePropletStatus(ctx, proplet)
}

func (r *PropletReconciler) reconcileExternalProplet(ctx context.Context, proplet *propellerv1.Proplet) (ctrl.Result, error) {
	logger := logf.FromContext(ctx).WithValues("proplet", proplet.Name)

	if proplet.Spec.External == nil {
		return ctrl.Result{}, fmt.Errorf("external spec is required for external proplet type")
	}

	switch {
	case proplet.Status.LastSeen != nil:
		timeSinceLastSeen := time.Since(proplet.Status.LastSeen.Time)
		if timeSinceLastSeen > r.lastSeenThreshold {
			proplet.Status.Phase = propellerv1.PropletOfflinePhase
			r.setCondition(proplet, propellerv1.PropletConditionConnected, metav1.ConditionFalse, "PropletOffline",
				fmt.Sprintf("offline for %s (threshold: %s)", timeSinceLastSeen, r.lastSeenThreshold))
			r.setCondition(proplet, propellerv1.PropletConditionReady, metav1.ConditionFalse, "PropletOffline",
				"external proplet is offline")
		} else {
			proplet.Status.Phase = propellerv1.PropletRunningPhase
			r.setCondition(proplet, propellerv1.PropletConditionConnected, metav1.ConditionTrue, "PropletOnline",
				fmt.Sprintf("last seen %s ago", timeSinceLastSeen))
			r.setCondition(proplet, propellerv1.PropletConditionReady, metav1.ConditionTrue, "PropletReady",
				"external proplet is connected")
		}
	default:
		proplet.Status.Phase = propellerv1.PropletInitializingPhase
		r.setCondition(proplet, propellerv1.PropletConditionReady, metav1.ConditionFalse, "PropletInitializing",
			"waiting for first heartbeat")
		r.removeCondition(proplet, propellerv1.PropletConditionConnected)
	}

	if len(proplet.Spec.Resource) > 0 {
		if proplet.Status.AvailableResources == nil {
			proplet.Status.AvailableResources = &propellerv1.PropletResources{}
		}
		if cpu := proplet.Spec.Resource.Cpu(); cpu != nil {
			proplet.Status.AvailableResources.CPU = cpu.String()
		}
		if mem := proplet.Spec.Resource.Memory(); mem != nil {
			proplet.Status.AvailableResources.Memory = mem.String()
		}
		custom := make(map[string]string)
		for name, qty := range proplet.Spec.Resource {
			if name != corev1.ResourceCPU && name != corev1.ResourceMemory {
				custom[string(name)] = qty.String()
			}
		}
		if len(custom) > 0 {
			proplet.Status.AvailableResources.Custom = custom
		}
	}

	if err := r.updateTaskCount(ctx, proplet); err != nil {
		logger.Error(err, "failed to update task count")
	}

	if err := r.updatePropletStatus(ctx, proplet); err != nil {
		logger.Error(err, "failed to update external proplet status")
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}

	requeueInterval := r.livelinessInterval
	if proplet.Status.Phase == propellerv1.PropletOfflinePhase {
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
				logger.Info("status update conflict, retrying", "attempt", i+1)
				fresh := &propellerv1.Proplet{}
				if getErr := r.Get(ctx, types.NamespacedName{Name: proplet.Name, Namespace: proplet.Namespace}, fresh); getErr != nil {
					return getErr
				}
				if fresh.Status.LastSeen != nil {
					if proplet.Status.LastSeen == nil || fresh.Status.LastSeen.After(proplet.Status.LastSeen.Time) {
						proplet.Status.LastSeen = fresh.Status.LastSeen
					}
				}
				fresh.Status = proplet.Status
				*proplet = *fresh
				time.Sleep(100 * time.Millisecond * time.Duration(i+1))
				continue
			}
			return err
		}
		return nil
	}
	return fmt.Errorf("failed to update proplet status after %d retries", maxRetries)
}

func (r *PropletReconciler) setCondition(proplet *propellerv1.Proplet, conditionType propellerv1.PropletConditionType, status metav1.ConditionStatus, reason, message string) {
	now := metav1.Now()
	for i, existing := range proplet.Status.Conditions {
		if existing.Type == conditionType {
			if existing.Status != status || existing.Reason != reason {
				proplet.Status.Conditions[i] = propellerv1.PropletCondition{
					Type:               conditionType,
					Status:             status,
					LastTransitionTime: now,
					Reason:             reason,
					Message:            message,
				}
			} else if existing.Message != message {
				proplet.Status.Conditions[i].Message = message
			}
			return
		}
	}
	proplet.Status.Conditions = append(proplet.Status.Conditions, propellerv1.PropletCondition{
		Type:               conditionType,
		Status:             status,
		LastTransitionTime: now,
		Reason:             reason,
		Message:            message,
	})
}

func (r *PropletReconciler) removeCondition(proplet *propellerv1.Proplet, conditionType propellerv1.PropletConditionType) {
	for i, existing := range proplet.Status.Conditions {
		if existing.Type == conditionType {
			proplet.Status.Conditions = append(proplet.Status.Conditions[:i], proplet.Status.Conditions[i+1:]...)
			return
		}
	}
}

func (r *PropletReconciler) updateTaskCount(ctx context.Context, proplet *propellerv1.Proplet) error {
	var tasks propellerv1.TaskList
	if err := r.List(ctx, &tasks); err != nil {
		return fmt.Errorf("failed to list tasks: %w", err)
	}

	var total, active uint64
	for _, task := range tasks.Items {
		if task.Status.AssignedProplet != proplet.Name {
			continue
		}
		total++
		// Only non-terminal phases count as active; Skipped and Interrupted
		// are terminal and must not inflate the active-task counter.
		if !isTerminalPhase(task.Status.Phase) {
			active++
		}
	}

	proplet.Status.TaskCount = total
	if active > 0 {
		r.setCondition(proplet, propellerv1.PropletConditionHealthy, metav1.ConditionTrue, "ProcessingTasks",
			fmt.Sprintf("processing %d active tasks", active))
	}
	return nil
}


// mqttLivenessHandler is invoked by the MQTT goroutine for every heartbeat
// message on the /control/proplet/alive topic.  It must not write to the
// Kubernetes API directly.  Instead it:
//  1. Records the heartbeat timestamp in pendingHeartbeats (keyed by UID).
//  2. Enqueues a GenericEvent so the Reconcile loop picks up the change.
func (r *PropletReconciler) mqttLivenessHandler(ctx context.Context, msg map[string]any) error {
	propletClientID, ok := msg["proplet_id"].(string)
	if !ok || propletClientID == "" {
		return errors.New("missing or empty proplet_id in liveness message")
	}

	var proplets propellerv1.PropletList
	if err := r.List(ctx, &proplets); err != nil {
		return err
	}

	for i := range proplets.Items {
		if proplets.Items[i].Spec.ConnectionConfig.ClientID != propletClientID {
			continue
		}
		p := &proplets.Items[i]
		r.pendingHeartbeats.Store(string(p.UID), metav1.Now())
		select {
		case r.propletEvents <- event.GenericEvent{Object: p}:
		default:
			// Channel is full; the periodic requeue will pick up the heartbeat.
		}
		return nil
	}

	// Proplet not yet registered — ignore.
	return nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *PropletReconciler) SetupWithManager(
	domainID, channelID string, mgr ctrl.Manager, livelinessInterval, lastSeenThreshold time.Duration, pubsub mqtt.PubSub,
) error {
	r.livelinessInterval = livelinessInterval
	r.lastSeenThreshold = lastSeenThreshold
	r.pubsub = pubsub
	r.baseTopic = fmt.Sprintf(superMQBaseTopic, domainID, channelID)
	r.propletEvents = make(chan event.GenericEvent, 256)

	// Subscribe only to the liveness topic; task result topics are handled by
	// TaskReconciler to respect separation of responsibilities.
	if r.pubsub != nil {
		if err := r.pubsub.Subscribe(
			r.baseTopic+"/control/proplet/alive",
			func(_ string, msg map[string]any) error {
				return r.mqttLivenessHandler(context.Background(), msg)
			},
		); err != nil {
			return err
		}
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&propellerv1.Proplet{}).
		Named("proplet").
		// MQTT-triggered events enter the reconcile queue via this channel source
		// rather than through direct API writes from the MQTT goroutine.
		WatchesRawSource(source.Channel(r.propletEvents, &handler.EnqueueRequestForObject{})).
		Complete(r)
}
