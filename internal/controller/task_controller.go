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
	"slices"
	"time"

	propellerv1 "github.com/absmach/propeller/api/v1"
	"github.com/absmach/propeller/internal/mqtt"
	"github.com/absmach/propeller/internal/scheduler"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

// TaskReconciler reconciles a Task object
type TaskReconciler struct {
	client.Client
	Scheme    *runtime.Scheme
	scheduler scheduler.Scheduler
	domainID  string
	channelID string
	pubsub    mqtt.PubSub
	baseTopic string
}

// +kubebuilder:rbac:groups=propeller.propeller.abstractmachines.fr,resources=tasks,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=propeller.propeller.abstractmachines.fr,resources=tasks/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=propeller.propeller.abstractmachines.fr,resources=tasks/finalizers,verbs=update

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the Task object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.21.0/pkg/reconcile
func (r *TaskReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := logf.FromContext(ctx).WithValues("task", req.NamespacedName)

	var task propellerv1.Task
	if err := r.Get(ctx, req.NamespacedName, &task); err != nil {
		if apierrors.IsNotFound(err) {
			logger.Info("Task not found, ignoring")

			return ctrl.Result{}, nil
		}

		logger.Error(err, "unable to fetch Task")

		return ctrl.Result{}, err
	}

	switch task.Status.Phase {
	case "": // New task
		return r.scheduleTask(ctx, &task)
	case propellerv1.TaskPendingPhase:
		return r.executeTask(ctx, &task)
	case propellerv1.TaskRunningPhase:
		return r.monitorTask(ctx, &task)
	case propellerv1.TaskCompletedPhase, propellerv1.TaskFailedPhase:
		return ctrl.Result{}, nil
	default:
		logger.Error(errors.New("unknown task phase"), "unknown task phase", "phase", task.Status.Phase)

		return ctrl.Result{}, nil
	}
}

func (r *TaskReconciler) scheduleTask(ctx context.Context, task *propellerv1.Task) (ctrl.Result, error) {
	logger := logf.FromContext(ctx).WithValues("task", task.Name, "action", "schedule")

	proplets, err := r.findSuitableProplets(ctx, task)
	if err != nil {
		logger.Error(err, "Failed to find suitable proplets")

		return ctrl.Result{}, err
	}

	if len(proplets) == 0 {
		logger.Info("No suitable proplets found, waiting")
		task.Status.Phase = propellerv1.TaskPendingPhase

		task.Status.Conditions = append(task.Status.Conditions, propellerv1.TaskCondition{
			Type:               propellerv1.ScheduledType,
			Status:             metav1.ConditionFalse,
			LastTransitionTime: metav1.Now(),
			Reason:             "NoSuitableProplets",
			Message:            "No proplets available that match the task requirements",
		})

		if err := r.Status().Update(ctx, task); err != nil {
			return ctrl.Result{}, err
		}

		return ctrl.Result{RequeueAfter: time.Second * 30}, nil
	}

	selectedProplet, err := r.scheduler.SelectProplet(*task, proplets)
	if err != nil {
		logger.Error(err, "Failed to select proplet")

		return ctrl.Result{}, err
	}

	now := metav1.Now()
	task.Status = propellerv1.TaskStatus{
		Phase:           propellerv1.TaskPendingPhase,
		AssignedProplet: selectedProplet.Name,
		StartedAt:       &now,
		FinishedAt:      &now,
		Error:           "",
		Conditions: []propellerv1.TaskCondition{{
			Type:               propellerv1.ScheduledType,
			Status:             metav1.ConditionTrue,
			LastTransitionTime: metav1.Now(),
			Reason:             "PropletSelected",
			Message:            fmt.Sprintf("Task scheduled to proplet %s", selectedProplet.Name),
		}},
	}

	if err := r.Status().Update(ctx, task); err != nil {
		return ctrl.Result{}, err
	}

	logger.Info("Task scheduled successfully", "proplet", selectedProplet.Name)

	return ctrl.Result{RequeueAfter: time.Second * 5}, nil
}

func (r *TaskReconciler) findSuitableProplets(ctx context.Context, task *propellerv1.Task) ([]propellerv1.Proplet, error) {
	var propletList propellerv1.PropletList
	if err := r.List(ctx, &propletList, client.InNamespace(task.Namespace)); err != nil {
		return nil, err
	}

	var suitableProplets []propellerv1.Proplet
	for _, proplet := range propletList.Items {
		if r.isPropletSuitable(&proplet, task) {
			suitableProplets = append(suitableProplets, proplet)
		}
	}

	return suitableProplets, nil
}

func (r *TaskReconciler) isPropletSuitable(proplet *propellerv1.Proplet, task *propellerv1.Task) bool {
	if proplet.Status.Phase != propellerv1.PropletRunningPhase {
		return false
	}

	// Check proplet type preference
	if task.Spec.PreferredPropletType != propellerv1.AnyProplet {
		if (task.Spec.PreferredPropletType == propellerv1.K8sProplet &&
			proplet.Spec.Type != propellerv1.K8sProplet) ||
			(task.Spec.PreferredPropletType == propellerv1.ExternalProplet &&
				proplet.Spec.Type != propellerv1.ExternalProplet) {
			return false
		}
	}

	// Check selector requirements
	if task.Spec.PropletSelector != nil {
		if len(task.Spec.PropletSelector.MatchDeviceTypes) > 0 {
			if proplet.Spec.Type != propellerv1.ExternalProplet ||
				proplet.Spec.External == nil {
				return false
			}

			if !slices.Contains(task.Spec.PropletSelector.MatchDeviceTypes, proplet.Spec.External.DeviceType) {
				return false
			}
		}

		// Check capabilities
		if len(task.Spec.PropletSelector.MatchCapabilities) > 0 {
			if proplet.Spec.Type != propellerv1.ExternalProplet ||
				proplet.Spec.External == nil {
				return false
			}

			for _, reqCapability := range task.Spec.PropletSelector.MatchCapabilities {
				if !slices.Contains(proplet.Spec.External.Capabilities, reqCapability) {
					return false
				}
			}
		}

		// Check labels
		if len(task.Spec.PropletSelector.MatchLabels) > 0 {
			for key, value := range task.Spec.PropletSelector.MatchLabels {
				if proplet.Labels[key] != value {
					return false
				}
			}
		}
	}

	return true
}

func (r *TaskReconciler) executeTask(ctx context.Context, task *propellerv1.Task) (ctrl.Result, error) {
	logger := logf.FromContext(ctx).WithValues("task", task.Name, "action", "execute")

	if task.Status.AssignedProplet == "" {
		logger.Error(errors.New("no proplet assigned"), "no proplet assigned")

		return r.scheduleTask(ctx, task)
	}

	topic := r.baseTopic + "/control/manager/start"
	payload := map[string]any{
		"id":        string(task.UID),
		"name":      task.Spec.FunctionName,
		"state":     0,
		"image_url": task.Spec.ImageURL,
		"file":      task.Spec.File,
		"inputs":    []uint64{10, 20},
		"cli_args":  task.Spec.CLIArgs,
	}

	if err := r.pubsub.Publish(topic, payload); err != nil {
		logger.Error(err, "Failed to publish task start command")

		return ctrl.Result{}, err
	}

	now := metav1.Now()
	task.Status.Phase = propellerv1.TaskRunningPhase
	task.Status.StartedAt = &now
	task.Status.Conditions = append(task.Status.Conditions, propellerv1.TaskCondition{
		Type:               propellerv1.StartedType,
		Status:             metav1.ConditionTrue,
		LastTransitionTime: now,
		Reason:             "TaskStarted",
		Message:            "Task execution started on proplet",
	})

	if err := r.Status().Update(ctx, task); err != nil {
		return ctrl.Result{}, err
	}

	logger.Info("Task execution started", "proplet", task.Status.AssignedProplet)

	return ctrl.Result{RequeueAfter: time.Minute * 2}, nil
}

func (r *TaskReconciler) monitorTask(ctx context.Context, task *propellerv1.Task) (ctrl.Result, error) {
	if task.Status.StartedAt != nil {
		elapsed := time.Since(task.Status.StartedAt.Time)
		if elapsed > time.Hour {
			task.Status.Phase = propellerv1.TaskFailedPhase
			task.Status.Error = "Task execution timeout"

			now := metav1.Now()
			task.Status.FinishedAt = &now

			if err := r.Status().Update(ctx, task); err != nil {
				return ctrl.Result{}, err
			}

			return ctrl.Result{}, nil
		}
	}

	return ctrl.Result{RequeueAfter: time.Second * 1}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *TaskReconciler) SetupWithManager(domainID, channelID string, mgr ctrl.Manager, pubsub mqtt.PubSub) error {
	r.scheduler = scheduler.NewRoundRobin()
	r.domainID = domainID
	r.channelID = channelID
	r.pubsub = pubsub
	r.baseTopic = fmt.Sprintf(superMQBaseTopic, domainID, channelID)

	return ctrl.NewControllerManagedBy(mgr).
		For(&propellerv1.Task{}).
		Named("task").
		Complete(r)
}
