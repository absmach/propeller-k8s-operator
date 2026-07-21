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
	"encoding/json"
	"fmt"
	"sync"
	"time"

	propellerapiv1 "github.com/absmach/propeller/api/v1"
	propellercron "github.com/absmach/propeller/internal/cron"
	"github.com/absmach/propeller/internal/dag"
	"github.com/absmach/propeller/internal/mqtt"
	"github.com/absmach/propeller/internal/scheduler"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

// indexDependsOn is the field index key for Task.Spec.DependsOn.
const indexDependsOn = "spec.dependsOn"

// TaskFinalizerName is the finalizer added to external tasks so we can send
// a stop command before the resource is removed from etcd.
const TaskFinalizerName = "propeller.propeller.absmach.eu/task-finalizer"

// mqttTaskUpdate carries the payload from an MQTT result or status message
// delivered by a proplet.  It is stored in pendingResults by the MQTT goroutine
// and consumed (LoadAndDelete) inside Reconcile so that all API writes remain
// within the reconcile loop.
type mqttTaskUpdate struct {
	phase  propellerapiv1.TaskPhase
	result json.RawMessage
	errMsg string
}

// TaskReconciler reconciles a Task object.
type TaskReconciler struct {
	client.Client

	Scheme    *runtime.Scheme
	Namespace string
	sched     scheduler.Scheduler
	pubsub    mqtt.PubSub
	tenantID  string
	channelID string
	baseTopic string

	// taskEvents bridges MQTT goroutines to the reconcile queue.
	// The MQTT result/status handlers write a GenericEvent here; the
	// WatchesRawSource below forwards it to the work queue so that all
	// API writes stay inside Reconcile.
	taskEvents chan event.GenericEvent

	// pendingResults stores MQTT-delivered task updates keyed by string(task.UID).
	// Written by MQTT handlers, consumed (LoadAndDelete) inside Reconcile.
	pendingResults sync.Map

	// pendingMetrics stores the latest metrics snapshot keyed by string(task.UID).
	// Written by the MQTT task_metrics handler, consumed inside Reconcile.
	pendingMetrics sync.Map
}

// +kubebuilder:rbac:groups=propeller.propeller.absmach.eu,resources=tasks,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=propeller.propeller.absmach.eu,resources=tasks/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=propeller.propeller.absmach.eu,resources=tasks/finalizers,verbs=update
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get
// +kubebuilder:rbac:groups=propeller.propeller.absmach.eu,resources=proplets,verbs=get;list

func (r *TaskReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	task := &propellerapiv1.Task{}
	if err := r.Get(ctx, req.NamespacedName, task); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Handle deletion: send stop command to external proplets before removing
	// the finalizer so the workload is not orphaned on the proplet.
	if task.DeletionTimestamp != nil {
		return r.handleDeletion(ctx, task)
	}

	// Consume any MQTT update (result/status) that arrived from a proplet since
	// the last reconcile.  If one is present, persist the new phase and return;
	// the status-change event will trigger the next reconcile to process it.
	// A conflict error means the informer cache is stale — re-queue without logging
	// an error since the next reconcile will retry with fresh data.
	if update, updated := r.applyMQTTUpdate(task); updated {
		if err := r.Status().Update(ctx, task); err != nil {
			r.pendingResults.Store(string(task.UID), update)
			return ctrl.Result{RequeueAfter: time.Second}, nil
		}
		return ctrl.Result{}, nil
	}

	// Apply any pending metrics snapshot delivered by the MQTT goroutine.
	r.applyPendingMetrics(task)

	if task.Status.Phase == "" {
		now := metav1.Now()
		task.Status.Phase = propellerapiv1.TaskPendingPhase
		task.Status.CreatedAt = &now
		task.Status.UpdatedAt = &now
		if task.Status.Conditions == nil {
			task.Status.Conditions = []propellerapiv1.TaskCondition{}
		}
		if err := r.Status().Update(ctx, task); err != nil {
			return ctrl.Result{}, err
		}
	}

	switch task.Status.Phase {
	case propellerapiv1.TaskPendingPhase:
		return r.handlePending(ctx, task)
	case propellerapiv1.TaskScheduledPhase, propellerapiv1.TaskRunningPhase:
		return r.handleRunning(ctx, task)
	case propellerapiv1.TaskCompletedPhase, propellerapiv1.TaskFailedPhase:
		return r.handleTerminal(ctx, task)
	case propellerapiv1.TaskSkippedPhase, propellerapiv1.TaskInterruptedPhase:
		return ctrl.Result{}, nil
	default:
		logger.Info("unknown phase, ignoring", "phase", task.Status.Phase)
		return ctrl.Result{}, nil
	}
}

// handleDeletion sends an MQTT stop to the proplet when a running/scheduled
// external task is deleted, then removes the finalizer.
func (r *TaskReconciler) handleDeletion(ctx context.Context, task *propellerapiv1.Task) (ctrl.Result, error) {
	if controllerutil.ContainsFinalizer(task, TaskFinalizerName) {
		phase := task.Status.Phase
		if (phase == propellerapiv1.TaskRunningPhase || phase == propellerapiv1.TaskScheduledPhase) &&
			r.pubsub != nil {
			stopPayload := map[string]any{
				"id":         string(task.UID),
				"proplet_id": task.Status.AssignedProplet,
				"broadcast":  task.Spec.Broadcast,
			}
			topic := r.baseTopic + "/control/manager/stop"
			if err := r.pubsub.Publish(topic, stopPayload); err != nil {
				log.FromContext(ctx).Error(err, "failed to publish task stop command on deletion")
			}
		}
		controllerutil.RemoveFinalizer(task, TaskFinalizerName)
		if err := r.Update(ctx, task); err != nil {
			return ctrl.Result{}, err
		}
	}
	return ctrl.Result{}, nil
}

// applyMQTTUpdate checks pendingResults for an update stored by an MQTT
// handler and, if present, applies it to the in-memory task object.
// Returns the update and true when an update was applied; the caller must
// then persist the change via Status().Update().  On failure, the caller
// should re-store the returned update so it is not permanently lost.
func (r *TaskReconciler) applyMQTTUpdate(task *propellerapiv1.Task) (mqttTaskUpdate, bool) {
	raw, ok := r.pendingResults.LoadAndDelete(string(task.UID))
	if !ok {
		return mqttTaskUpdate{}, false
	}
	update := raw.(mqttTaskUpdate)
	if update.phase == "" {
		return mqttTaskUpdate{}, false
	}
	if !propellerapiv1.ValidStateTransition(task.Status.Phase, update.phase) {
		return mqttTaskUpdate{}, false
	}
	now := metav1.Now()
	task.Status.Phase = update.phase
	// Only terminal phases represent actual completion; intermediate phases
	// (e.g. Running) must not overwrite FinishedAt.
	if update.phase == propellerapiv1.TaskCompletedPhase ||
		update.phase == propellerapiv1.TaskFailedPhase ||
		update.phase == propellerapiv1.TaskInterruptedPhase {
		task.Status.FinishedAt = &now
	}
	if update.errMsg != "" {
		task.Status.Error = update.errMsg
	}
	if update.result != nil {
		task.Status.Results = &apiextensionsv1.JSON{Raw: update.result}
	}
	condStatus := metav1.ConditionTrue
	condReason := "Completed"
	condMsg := "Task completed successfully"
	if update.phase == propellerapiv1.TaskFailedPhase {
		condStatus = metav1.ConditionFalse
		condReason = "Failed"
		condMsg = update.errMsg
		if condMsg == "" {
			condMsg = "task failed"
		}
	}
	r.updateCondition(task, propellerapiv1.CompletedType, condStatus, condReason, condMsg)
	return update, true
}

// handlePending checks dependencies, selects a proplet, and dispatches the task.
func (r *TaskReconciler) handlePending(ctx context.Context, task *propellerapiv1.Task) (ctrl.Result, error) {
	// If this task has a cron schedule and NextRun has not been set yet, compute
	// it now so handleTerminal can requeue at the right time after completion.
	if task.Spec.Schedule != "" && task.Status.NextRun == nil {
		sched, err := propellercron.ParseCronExpression(task.Spec.Schedule)
		if err == nil {
			tz := task.Spec.Timezone
			if tz == "" {
				tz = "UTC"
			}
			next := propellercron.CalculateNextRun(sched, time.Now(), tz)
			t := metav1.NewTime(next)
			task.Status.NextRun = &t
			if err := r.Status().Update(ctx, task); err != nil {
				return ctrl.Result{}, err
			}
		}
	}

	// Dependency gate: wait until all declared dependencies are terminal.
	// No requeue is issued here — the Watches(Task, enqueueDependents) watch
	// will trigger reconcile when a dependency reaches a terminal phase.
	if len(task.Spec.DependsOn) > 0 {
		allDone, skip, err := r.evaluateDeps(ctx, task)
		if err != nil {
			return ctrl.Result{}, err
		}
		if skip {
			return r.transitionToSkipped(ctx, task, "run_if condition not met after dependencies completed")
		}
		if !allDone {
			return ctrl.Result{}, nil
		}
	}

	// Broadcast: publish to all proplets without selecting a specific one.
	if task.Spec.Broadcast {
		return r.startBroadcastTask(ctx, task)
	}

	propletID, err := r.selectProplet(ctx, task)
	if err != nil {
		log.FromContext(ctx).Info("no available proplet, requeueing", "reason", err.Error())
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}

	backend, err := r.determineBackend(ctx, task.Namespace, propletID)
	if err != nil {
		return ctrl.Result{}, err
	}

	// WASM file tasks go via MQTT even for k8s-backed proplets,
	// since the proplet's Wasmtime runtime handles execution.
	if backend == propellerapiv1.K8sProplet && len(task.Spec.File) == 0 {
		return r.startK8sJob(ctx, task, propletID)
	}
	return r.startExternalTask(ctx, task, propletID)
}

// startBroadcastTask publishes a start command with no proplet_id so that all
// proplets subscribed to the channel will pick up the task.
func (r *TaskReconciler) startBroadcastTask(ctx context.Context, task *propellerapiv1.Task) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	if r.pubsub == nil {
		task.Status.Phase = propellerapiv1.TaskFailedPhase
		now := metav1.Now()
		task.Status.FinishedAt = &now
		task.Status.Error = "MQTT pubsub not configured"
		r.updateCondition(task, propellerapiv1.CompletedType, metav1.ConditionFalse, "MQTTNotConfigured", task.Status.Error)
		return ctrl.Result{}, r.Status().Update(ctx, task)
	}

	if !controllerutil.ContainsFinalizer(task, TaskFinalizerName) {
		controllerutil.AddFinalizer(task, TaskFinalizerName)
		if err := r.Update(ctx, task); err != nil {
			return ctrl.Result{}, err
		}
	}

	topic := r.baseTopic + "/control/manager/start"
	funcName := task.Spec.FunctionName
	if funcName == "" {
		funcName = task.Name
	}

	const scheduledState = 1
	payload := map[string]any{
		"id":                string(task.UID),
		"name":              funcName,
		"state":             scheduledState,
		"kind":              task.Spec.Kind,
		"image_url":         task.Spec.ImageURL,
		"file":              task.Spec.File,
		"inputs":            task.Spec.Inputs,
		"cli_args":          task.Spec.CLIArgs,
		"env":               task.Spec.Env,
		"daemon":            task.Spec.Daemon,
		"mode":              task.Spec.Mode,
		"broadcast":         true,
		"encrypted":         task.Spec.Encrypted,
		"kbs_resource_path": task.Spec.KBSResourcePath,
		"priority":          task.Spec.Priority,
	}

	if task.Spec.Metadata != nil {
		var meta map[string]any
		if err := json.Unmarshal(task.Spec.Metadata.Raw, &meta); err != nil {
			logger.Error(err, "failed to unmarshal task metadata, skipping")
		} else {
			payload["metadata"] = meta
		}
	}

	if task.Spec.MonitoringProfile != nil {
		mp := task.Spec.MonitoringProfile
		profile := map[string]any{
			"enabled":                  mp.Enabled,
			"collect_cpu":              mp.CollectCPU,
			"collect_memory":           mp.CollectMemory,
			"collect_disk_io":          mp.CollectDiskIO,
			"collect_threads":          mp.CollectThreads,
			"collect_file_descriptors": mp.CollectFileDescriptors,
			"export_to_mqtt":           mp.ExportToMQTT,
			"retain_history":           mp.RetainHistory,
			"history_size":             mp.HistorySize,
		}
		if mp.Interval != nil {
			profile["interval"] = uint64(mp.Interval.Seconds())
		}
		payload["monitoring_profile"] = profile
	}

	if err := r.pubsub.Publish(topic, payload); err != nil {
		logger.Error(err, "failed to publish broadcast task start command")
		return ctrl.Result{}, err
	}

	now := metav1.Now()
	task.Status.Phase = propellerapiv1.TaskRunningPhase
	task.Status.StartedAt = &now
	r.updateCondition(task, propellerapiv1.StartedType, metav1.ConditionTrue, "Running", "Broadcast task dispatched via MQTT")
	if err := r.Status().Update(ctx, task); err != nil {
		return ctrl.Result{RequeueAfter: time.Second}, nil
	}
	return ctrl.Result{}, nil
}

// evaluateDeps inspects all tasks listed in task.Spec.DependsOn and returns
// (allTerminal, shouldSkip, error).
func (r *TaskReconciler) evaluateDeps(ctx context.Context, task *propellerapiv1.Task) (bool, bool, error) {
	completed := make(map[string]bool)
	failed := make(map[string]bool)

	for _, depName := range task.Spec.DependsOn {
		dep := &propellerapiv1.Task{}
		if err := r.Get(ctx, client.ObjectKey{Name: depName, Namespace: task.Namespace}, dep); err != nil {
			if apierrors.IsNotFound(err) {
				return false, false, nil // dep not yet created
			}
			return false, false, err
		}
		switch dep.Status.Phase {
		case propellerapiv1.TaskCompletedPhase:
			completed[depName] = true
		case propellerapiv1.TaskFailedPhase, propellerapiv1.TaskSkippedPhase, propellerapiv1.TaskInterruptedPhase:
			failed[depName] = true
		}
	}

	allTerminal := dag.AllDepsTerminal(task.Spec.DependsOn, completed, failed)
	if !allTerminal {
		return false, false, nil
	}
	shouldSkip := dag.ShouldSkip(task.Spec.DependsOn, string(task.Spec.RunIf), completed, failed)
	return true, shouldSkip, nil
}

// selectProplet resolves which proplet should execute the task.
// It honours an explicit PropletSelector.PropletID when set (but still
// verifies the proplet is alive), otherwise it delegates to the round-robin
// scheduler across running proplets.
func (r *TaskReconciler) selectProplet(ctx context.Context, task *propellerapiv1.Task) (string, error) {
	if task.Spec.PropletSelector != nil && task.Spec.PropletSelector.PropletID != "" {
		pinned := task.Spec.PropletSelector.PropletID
		proplet := &propellerapiv1.Proplet{}
		if err := r.Get(ctx, client.ObjectKey{Name: pinned, Namespace: task.Namespace}, proplet); err != nil {
			return "", fmt.Errorf("pinned proplet %q not found: %w", pinned, err)
		}
		if proplet.Status.Phase != propellerapiv1.PropletRunningPhase {
			return "", fmt.Errorf("pinned proplet %q is not running (phase: %s)", pinned, proplet.Status.Phase)
		}
		return pinned, nil
	}

	if r.sched == nil {
		return "", fmt.Errorf("no scheduler configured")
	}

	propletList := &propellerapiv1.PropletList{}
	if err := r.List(ctx, propletList, client.InNamespace(task.Namespace)); err != nil {
		return "", err
	}

	selected, err := r.sched.SelectProplet(*task, propletList.Items)
	if err != nil {
		return "", err
	}
	return selected.Name, nil
}

func (r *TaskReconciler) handleRunning(ctx context.Context, task *propellerapiv1.Task) (ctrl.Result, error) {
	// K8s-Job-backed tasks: Owns(&batchv1.Job{}) drives change-triggered reconcile.
	// A safety requeue is kept at a long interval to catch edge cases.
	jobName := task.Name + "-job"
	job := &batchv1.Job{}
	if err := r.Get(ctx, client.ObjectKey{Name: jobName, Namespace: task.Namespace}, job); err != nil {
		if apierrors.IsNotFound(err) {
			// External task: completion arrives via MQTT result event (no polling).
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	if job.Status.Succeeded > 0 {
		return r.handleJobSucceeded(ctx, task, job)
	}
	if job.Status.Failed > 0 {
		return r.handleJobFailed(ctx, task, job)
	}

	// Safety requeue — Owns() will normally drive this earlier.
	return ctrl.Result{RequeueAfter: 5 * time.Minute}, nil
}

// handleTerminal handles completed/failed tasks that have IsRecurring set.
// It computes the next run time from Spec.Schedule when needed and requeues
// until that time arrives, then resets the task back to Pending.
func (r *TaskReconciler) handleTerminal(ctx context.Context, task *propellerapiv1.Task) (ctrl.Result, error) {
	if !task.Spec.IsRecurring || task.Spec.Schedule == "" {
		return ctrl.Result{}, nil
	}

	// Compute NextRun if it has not been set yet (e.g. first completion).
	if task.Status.NextRun == nil {
		sched, err := propellercron.ParseCronExpression(task.Spec.Schedule)
		if err != nil {
			log.FromContext(ctx).Error(err, "invalid cron expression in spec", "schedule", task.Spec.Schedule)
			return ctrl.Result{}, nil
		}
		tz := task.Spec.Timezone
		if tz == "" {
			tz = "UTC"
		}
		// Use FinishedAt as the base so next-run is aligned to when the task
		// actually completed, not when the reconcile loop happened to wake up.
		// Fall back to time.Now() only if FinishedAt was not recorded.
		base := time.Now()
		if task.Status.FinishedAt != nil {
			base = task.Status.FinishedAt.Time
		}
		next := propellercron.CalculateNextRun(sched, base, tz)
		t := metav1.NewTime(next)
		task.Status.NextRun = &t
		if err := r.Status().Update(ctx, task); err != nil {
			return ctrl.Result{}, err
		}
	}

	delay := time.Until(task.Status.NextRun.Time)
	if delay > 0 {
		return ctrl.Result{RequeueAfter: delay}, nil
	}

	// NextRun has arrived — reset the task to Pending for the next execution.
	task.Status.Phase = propellerapiv1.TaskPendingPhase
	task.Status.AssignedProplet = ""
	task.Status.StartedAt = nil
	task.Status.FinishedAt = nil
	task.Status.Error = ""
	task.Status.Results = nil
	task.Status.Conditions = []propellerapiv1.TaskCondition{}
	task.Status.NextRun = nil // will be recomputed in handlePending

	return ctrl.Result{}, r.Status().Update(ctx, task)
}

func (r *TaskReconciler) transitionToSkipped(ctx context.Context, task *propellerapiv1.Task, reason string) (ctrl.Result, error) {
	now := metav1.Now()
	task.Status.Phase = propellerapiv1.TaskSkippedPhase
	task.Status.FinishedAt = &now
	r.updateCondition(task, propellerapiv1.CompletedType, metav1.ConditionFalse, "Skipped", reason)
	return ctrl.Result{}, r.Status().Update(ctx, task)
}

func (r *TaskReconciler) determineBackend(ctx context.Context, namespace, propletID string) (propellerapiv1.PropletKind, error) {
	if propletID == "" {
		return propellerapiv1.ExternalProplet, nil
	}

	proplet := &propellerapiv1.Proplet{}
	if err := r.Get(ctx, client.ObjectKey{Name: propletID, Namespace: namespace}, proplet); err != nil {
		return propellerapiv1.ExternalProplet, client.IgnoreNotFound(err)
	}
	return proplet.Spec.Type, nil
}

func (r *TaskReconciler) startK8sJob(ctx context.Context, task *propellerapiv1.Task, propletID string) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	image := task.Spec.ImageURL
	if image == "" {
		return ctrl.Result{}, fmt.Errorf("no container image specified: set spec.imageUrl for k8s-backed tasks")
	}

	configMapName := task.Name + "-config"
	configMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      configMapName,
			Namespace: task.Namespace,
		},
		Data: task.Spec.Env,
	}
	if len(task.Spec.File) > 0 {
		if configMap.Data == nil {
			configMap.Data = map[string]string{}
		}
		configMap.Data["wasm_file_provided"] = "true"
	}
	// SetControllerReference resolves GVK from the scheme — task.TypeMeta is
	// empty for objects returned by client.Get so manual OwnerReference
	// construction would produce invalid (empty APIVersion/Kind) entries.
	if err := controllerutil.SetControllerReference(task, configMap, r.Scheme); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.Create(ctx, configMap); client.IgnoreAlreadyExists(err) != nil {
		return ctrl.Result{}, err
	}

	resReqs, err := r.buildResourceRequirements(task.Spec.ResourceRequirements)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("invalid resource requirements: %w", err)
	}

	restartPolicy := corev1.RestartPolicyOnFailure
	if task.Spec.RestartPolicy != "" {
		restartPolicy = task.Spec.RestartPolicy
	} else if task.Spec.Daemon {
		restartPolicy = corev1.RestartPolicyAlways
	}

	jobName := task.Name + "-job"
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobName,
			Namespace: task.Namespace,
		},
		Spec: batchv1.JobSpec{
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					RestartPolicy: restartPolicy,
					Containers: []corev1.Container{
						{
							Name:  "task",
							Image: image,
							Args:  task.Spec.CLIArgs,
							EnvFrom: []corev1.EnvFromSource{
								{
									ConfigMapRef: &corev1.ConfigMapEnvSource{
										LocalObjectReference: corev1.LocalObjectReference{Name: configMapName},
									},
								},
							},
							Env: []corev1.EnvVar{
								{Name: "PROPLET_ID", Value: propletID},
								{Name: "TASK_ID", Value: string(task.UID)},
							},
							Resources: resReqs,
						},
					},
				},
			},
		},
	}
	if err := controllerutil.SetControllerReference(task, job, r.Scheme); err != nil {
		return ctrl.Result{}, err
	}

	if err := r.Create(ctx, job); client.IgnoreAlreadyExists(err) != nil {
		logger.Error(err, "failed to create job", "job", jobName)
		return ctrl.Result{}, err
	}

	now := metav1.Now()
	task.Status.Phase = propellerapiv1.TaskRunningPhase
	task.Status.AssignedProplet = propletID
	task.Status.StartedAt = &now
	r.updateCondition(task, propellerapiv1.StartedType, metav1.ConditionTrue, "Running", "Task is running via K8s Job")
	if err := r.Status().Update(ctx, task); err != nil {
		return ctrl.Result{RequeueAfter: time.Second}, nil
	}

	return ctrl.Result{}, nil
}

func (r *TaskReconciler) startExternalTask(ctx context.Context, task *propellerapiv1.Task, propletID string) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	// Use the proplet's MQTT entity_id for the proplet_id field,
	// since that's what the proplet runtime compares against.
	proplet := &propellerapiv1.Proplet{}
	mqttPropletID := propletID
	if err := r.Get(ctx, client.ObjectKey{Name: propletID, Namespace: task.Namespace}, proplet); err == nil {
		if proplet.Spec.ConnectionConfig.EntityID != "" {
			mqttPropletID = proplet.Spec.ConnectionConfig.EntityID
		}
	}

	if r.pubsub == nil {
		task.Status.Phase = propellerapiv1.TaskFailedPhase
		now := metav1.Now()
		task.Status.FinishedAt = &now
		task.Status.Error = "MQTT pubsub not configured"
		r.updateCondition(task, propellerapiv1.CompletedType, metav1.ConditionFalse, "MQTTNotConfigured", task.Status.Error)
		return ctrl.Result{}, r.Status().Update(ctx, task)
	}

	// Add finalizer so we can send a stop command when the Task is deleted.
	if !controllerutil.ContainsFinalizer(task, TaskFinalizerName) {
		controllerutil.AddFinalizer(task, TaskFinalizerName)
		if err := r.Update(ctx, task); err != nil {
			return ctrl.Result{}, err
		}
	}

	topic := r.baseTopic + "/control/manager/start"

	env := make(map[string]any, len(task.Spec.Env)+2)
	for k, v := range task.Spec.Env {
		env[k] = v
	}
	env["PROPLET_ID"] = mqttPropletID
	env["TASK_ID"] = string(task.UID)

	// scheduledState matches task.Scheduled (=1) in the main propeller's
	// numeric task.State enum.  Sending "state" aligns with publishStart in
	// the manager so proplets can validate the expected state transition.
	const scheduledState = 1

	// The proplet uses the "name" field as the WASM function name (service.rs:575).
	// Use FunctionName when set; fall back to the task Name so the field is
	// never empty (proplet rejects requests with an empty name).
	funcName := task.Spec.FunctionName
	if funcName == "" {
		funcName = task.Name
	}

	payload := map[string]any{
		"id":                string(task.UID),
		"name":              funcName,
		"state":             scheduledState,
		"kind":              task.Spec.Kind,
		"image_url":         task.Spec.ImageURL,
		"file":              task.Spec.File,
		"inputs":            task.Spec.Inputs,
		"cli_args":          task.Spec.CLIArgs,
		"env":               env,
		"daemon":            task.Spec.Daemon,
		"broadcast":         task.Spec.Broadcast,
		"mode":              task.Spec.Mode,
		"encrypted":         task.Spec.Encrypted,
		"kbs_resource_path": task.Spec.KBSResourcePath,
		"proplet_id":        mqttPropletID,
		"priority":          task.Spec.Priority,
	}

	if task.Spec.Metadata != nil {
		var meta map[string]any
		if err := json.Unmarshal(task.Spec.Metadata.Raw, &meta); err != nil {
			logger.Error(err, "failed to unmarshal task metadata, skipping")
		} else {
			payload["metadata"] = meta
		}
	}

	if task.Spec.MonitoringProfile != nil {
		mp := task.Spec.MonitoringProfile
		// Key must be "monitoring_profile" — commit e735acc changed the Rust proplet's
		// serde rename annotation from "monitoringProfile" back to "monitoring_profile".
		// Interval is serialised as u64 seconds by the proplet's serde_duration module.
		intervalSecs := uint64(10) // matches MonitoringProfile::default().interval in Rust
		if mp.Interval != nil {
			intervalSecs = uint64(mp.Interval.Seconds())
		}
		payload["monitoring_profile"] = map[string]any{
			"enabled":                  mp.Enabled,
			"interval":                 intervalSecs,
			"collect_cpu":              mp.CollectCPU,
			"collect_memory":           mp.CollectMemory,
			"collect_disk_io":          mp.CollectDiskIO,
			"collect_threads":          mp.CollectThreads,
			"collect_file_descriptors": mp.CollectFileDescriptors,
			"export_to_mqtt":           mp.ExportToMQTT,
			"retain_history":           mp.RetainHistory,
			"history_size":             mp.HistorySize,
		}
	}

	if err := r.pubsub.Publish(topic, payload); err != nil {
		logger.Error(err, "failed to publish task start command")
		return ctrl.Result{}, err
	}

	now := metav1.Now()
	task.Status.Phase = propellerapiv1.TaskRunningPhase
	task.Status.AssignedProplet = propletID
	task.Status.StartedAt = &now
	r.updateCondition(task, propellerapiv1.StartedType, metav1.ConditionTrue, "Running", "External task dispatched via MQTT")
	if err := r.Status().Update(ctx, task); err != nil {
		return ctrl.Result{RequeueAfter: time.Second}, nil
	}

	// Completion arrives via MQTT result event (mqttResultHandler → taskEvents).
	// No polling requeue needed.
	return ctrl.Result{}, nil
}

func (r *TaskReconciler) handleJobSucceeded(ctx context.Context, task *propellerapiv1.Task, job *batchv1.Job) (ctrl.Result, error) {
	now := metav1.Now()
	task.Status.Phase = propellerapiv1.TaskCompletedPhase
	task.Status.FinishedAt = &now
	r.extractAndStoreResult(ctx, task, job)
	r.updateCondition(task, propellerapiv1.CompletedType, metav1.ConditionTrue, "Completed", "Task completed successfully")
	if err := r.Status().Update(ctx, task); err != nil {
		return ctrl.Result{RequeueAfter: time.Second}, nil
	}
	return ctrl.Result{}, nil
}

func (r *TaskReconciler) handleJobFailed(ctx context.Context, task *propellerapiv1.Task, job *batchv1.Job) (ctrl.Result, error) {
	now := metav1.Now()
	task.Status.Phase = propellerapiv1.TaskFailedPhase
	task.Status.FinishedAt = &now
	task.Status.Error = r.extractJobFailureMessage(job)
	r.updateCondition(task, propellerapiv1.CompletedType, metav1.ConditionFalse, "Failed", task.Status.Error)
	if err := r.Status().Update(ctx, task); err != nil {
		return ctrl.Result{RequeueAfter: time.Second}, nil
	}
	return ctrl.Result{}, nil
}

func (r *TaskReconciler) extractJobFailureMessage(job *batchv1.Job) string {
	for _, c := range job.Status.Conditions {
		if c.Type == batchv1.JobFailed && c.Message != "" {
			return c.Message
		}
	}
	return "job failed"
}

func (r *TaskReconciler) extractAndStoreResult(ctx context.Context, task *propellerapiv1.Task, job *batchv1.Job) {
	logger := log.FromContext(ctx)
	result, err := ExtractResultFromJob(ctx, r.Client, job)
	if err != nil {
		logger.V(1).Info("no result to extract from job (container image may not produce structured output)", "job", job.Name)
		return
	}
	if result == nil {
		return
	}
	raw, err := json.Marshal(result)
	if err == nil {
		task.Status.Results = &apiextensionsv1.JSON{Raw: raw}
	}
}

// buildResourceRequirements converts PropletResources to a corev1.ResourceRequirements.
// Returns an error for invalid quantity strings rather than panicking.
func (r *TaskReconciler) buildResourceRequirements(req *propellerapiv1.PropletResources) (corev1.ResourceRequirements, error) {
	if req == nil {
		return corev1.ResourceRequirements{}, nil
	}
	reqs := corev1.ResourceRequirements{}
	if req.CPU != "" {
		q, err := resource.ParseQuantity(req.CPU)
		if err != nil {
			return corev1.ResourceRequirements{}, fmt.Errorf("invalid CPU quantity %q: %w", req.CPU, err)
		}
		reqs.Requests = corev1.ResourceList{corev1.ResourceCPU: q}
		reqs.Limits = corev1.ResourceList{corev1.ResourceCPU: q}
	}
	if req.Memory != "" {
		q, err := resource.ParseQuantity(req.Memory)
		if err != nil {
			return corev1.ResourceRequirements{}, fmt.Errorf("invalid Memory quantity %q: %w", req.Memory, err)
		}
		if reqs.Requests == nil {
			reqs.Requests = corev1.ResourceList{}
			reqs.Limits = corev1.ResourceList{}
		}
		reqs.Requests[corev1.ResourceMemory] = q
		reqs.Limits[corev1.ResourceMemory] = q
	}
	return reqs, nil
}

func (r *TaskReconciler) updateCondition(task *propellerapiv1.Task, conditionType propellerapiv1.TaskConditionType, status metav1.ConditionStatus, reason, message string) {
	if task.Status.Conditions == nil {
		task.Status.Conditions = []propellerapiv1.TaskCondition{}
	}
	now := metav1.Now()
	cond := propellerapiv1.TaskCondition{
		Type:               conditionType,
		Status:             status,
		LastTransitionTime: now,
		Reason:             reason,
		Message:            message,
	}
	for i, c := range task.Status.Conditions {
		if c.Type == conditionType {
			task.Status.Conditions[i] = cond
			return
		}
	}
	task.Status.Conditions = append(task.Status.Conditions, cond)
}

// applyPendingMetrics checks pendingMetrics for a snapshot stored by the MQTT
// metrics handler and applies it to the in-memory task object.  The caller is
// responsible for persisting via Status().Update() when needed.
func (r *TaskReconciler) applyPendingMetrics(task *propellerapiv1.Task) {
	raw, ok := r.pendingMetrics.LoadAndDelete(string(task.UID))
	if !ok {
		return
	}
	snap := raw.(propellerapiv1.TaskMetricsSnapshot)
	task.Status.LatestMetrics = &snap
}

// mqttTaskMetricsHandler is called by the MQTT goroutine for messages on
// /control/proplet/task_metrics.  It must not write to the Kubernetes API.
func (r *TaskReconciler) mqttTaskMetricsHandler(ctx context.Context, msg map[string]any) error {
	taskUID, ok := msg["task_id"].(string)
	if !ok || taskUID == "" {
		return nil
	}

	snap := propellerapiv1.TaskMetricsSnapshot{}
	now := metav1.Now()
	snap.Timestamp = &now

	if metricsData, ok := msg["metrics"].(map[string]any); ok {
		if v, ok := metricsData["cpu_percent"].(float64); ok {
			snap.CPUMilliPercent = int64(v * 1000)
		}
		if v, ok := metricsData["memory_bytes"].(float64); ok {
			snap.MemoryBytes = uint64(v)
		}
		if v, ok := metricsData["memory_percent"].(float64); ok {
			snap.MemoryMilliPercent = int64(v * 1000)
		}
	}

	// Find the Task with this UID and enqueue a reconcile.
	var tasks propellerapiv1.TaskList
	if err := r.List(ctx, &tasks, client.InNamespace(r.Namespace)); err != nil {
		return err
	}
	for i := range tasks.Items {
		if string(tasks.Items[i].UID) != taskUID {
			continue
		}
		t := &tasks.Items[i]
		r.pendingMetrics.Store(taskUID, snap)
		select {
		case r.taskEvents <- event.GenericEvent{Object: t}:
		default:
		}
		return nil
	}
	return nil
}

// mqttResultHandler is called by the MQTT goroutine when a proplet publishes
// a result on /control/proplet/results.  It must not write to the Kubernetes
// API.  Instead it stores the update and enqueues a GenericEvent.
//
// The proplet (and main propeller manager) uses "task_id" as the key in the
// result payload — this matches the key used by the manager's updateResultsHandler.
func (r *TaskReconciler) mqttResultHandler(ctx context.Context, msg map[string]any) error {
	taskUID, ok := msg["task_id"].(string)
	if !ok || taskUID == "" {
		return nil
	}

	update := mqttTaskUpdate{phase: propellerapiv1.TaskCompletedPhase}
	if errMsg, ok := msg["error"].(string); ok && errMsg != "" {
		update.phase = propellerapiv1.TaskFailedPhase
		update.errMsg = errMsg
	}
	// The proplet's ResultMessage.results is a plain string (e.g. "30"), not a
	// JSON object.  json.Unmarshal on the MQTT payload produces a Go string here.
	// Marshalling it again would double-encode it ("\"30\""); instead, wrap the
	// string as a JSON string literal directly.
	if results, ok := msg["results"].(string); ok && results != "" {
		if raw, err := json.Marshal(results); err == nil {
			update.result = raw
		}
	}

	return r.enqueueTaskByUID(ctx, taskUID, update)
}

// enqueueTaskByUID finds the Task with the given UID, stores the update in
// pendingResults, and sends a GenericEvent to the taskEvents channel so the
// reconcile loop picks it up.
func (r *TaskReconciler) enqueueTaskByUID(ctx context.Context, taskUID string, update mqttTaskUpdate) error {
	var tasks propellerapiv1.TaskList
	if err := r.List(ctx, &tasks, client.InNamespace(r.Namespace)); err != nil {
		return err
	}

	for i := range tasks.Items {
		if string(tasks.Items[i].UID) != taskUID {
			continue
		}
		t := &tasks.Items[i]
		r.pendingResults.Store(taskUID, update)
		select {
		case r.taskEvents <- event.GenericEvent{Object: t}:
		default:
			// Channel full; periodic reconcile will drain the pending update.
		}
		return nil
	}
	return nil
}

// enqueueDependents is the MapFunc for the Watches(Task, ...) watch.  When a
// Task reaches a terminal phase, it fans out reconcile requests to all pending
// Tasks that list it in their DependsOn.  The field indexer on spec.dependsOn
// makes this lookup O(log n) rather than a full table scan.
func (r *TaskReconciler) enqueueDependents(ctx context.Context, obj client.Object) []reconcile.Request {
	changedTask, ok := obj.(*propellerapiv1.Task)
	if !ok {
		return nil
	}
	if !isTerminalPhase(changedTask.Status.Phase) {
		return nil
	}

	var dependents propellerapiv1.TaskList
	if err := r.List(ctx, &dependents,
		client.InNamespace(changedTask.Namespace),
		client.MatchingFields{indexDependsOn: changedTask.Name},
	); err != nil {
		log.FromContext(ctx).Error(err, "enqueueDependents: failed to list dependents", "task", changedTask.Name)
		return nil
	}

	requests := make([]reconcile.Request, 0, len(dependents.Items))
	for _, t := range dependents.Items {
		if t.Status.Phase == propellerapiv1.TaskPendingPhase {
			requests = append(requests, reconcile.Request{
				NamespacedName: types.NamespacedName{Name: t.Name, Namespace: t.Namespace},
			})
		}
	}
	return requests
}

// SetupWithManager sets up the controller with the Manager.
func (r *TaskReconciler) SetupWithManager(tenantID, channelID string, mgr ctrl.Manager, pubsub mqtt.PubSub, sched scheduler.Scheduler) error {
	r.pubsub = pubsub
	r.sched = sched
	r.tenantID = tenantID
	r.channelID = channelID
	r.baseTopic = fmt.Sprintf(baseTopicFmt, tenantID, channelID)
	r.taskEvents = make(chan event.GenericEvent, 256)

	// Register a field index on spec.dependsOn so enqueueDependents can use
	// MatchingFields for an efficient lookup instead of a full table scan.
	if err := mgr.GetFieldIndexer().IndexField(
		context.Background(),
		&propellerapiv1.Task{},
		indexDependsOn,
		func(obj client.Object) []string {
			return obj.(*propellerapiv1.Task).Spec.DependsOn
		},
	); err != nil {
		return fmt.Errorf("failed to register %s field index: %w", indexDependsOn, err)
	}

	// Subscribe to proplet result and metrics topics; task lifecycle topics are
	// separate from proplet liveness (handled by PropletReconciler).
	if r.pubsub != nil {
		if err := r.pubsub.Subscribe(
			r.baseTopic+"/control/proplet/results",
			func(_ string, msg map[string]any) error {
				return r.mqttResultHandler(context.Background(), msg)
			},
		); err != nil {
			return err
		}
		if err := r.pubsub.Subscribe(
			r.baseTopic+"/control/proplet/task_metrics",
			func(_ string, msg map[string]any) error {
				return r.mqttTaskMetricsHandler(context.Background(), msg)
			},
		); err != nil {
			return err
		}
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&propellerapiv1.Task{}).
		Owns(&batchv1.Job{}).
		Watches(
			&propellerapiv1.Task{},
			handler.EnqueueRequestsFromMapFunc(r.enqueueDependents),
		).
		WatchesRawSource(source.Channel(r.taskEvents, &handler.EnqueueRequestForObject{})).
		Complete(r)
}
