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
	"time"

	propellerapiv1 "github.com/absmach/propeller/api/v1"
	"github.com/absmach/propeller/internal/dag"
	"github.com/absmach/propeller/internal/mqtt"
	"github.com/absmach/propeller/internal/scheduler"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
)

const (
	defaultDepCheckInterval = 15 * time.Second
)

// TaskReconciler reconciles a Task object.
type TaskReconciler struct {
	client.Client

	Scheme    *runtime.Scheme
	sched     scheduler.Scheduler
	pubsub    mqtt.PubSub
	domainID  string
	channelID string
	baseTopic string
}

// +kubebuilder:rbac:groups=propeller.propeller.abstractmachines.fr,resources=tasks,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=propeller.propeller.abstractmachines.fr,resources=tasks/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=propeller.propeller.abstractmachines.fr,resources=tasks/finalizers,verbs=update
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get
// +kubebuilder:rbac:groups=propeller.propeller.abstractmachines.fr,resources=proplets,verbs=get;list

func (r *TaskReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	task := &propellerapiv1.Task{}
	if err := r.Get(ctx, req.NamespacedName, task); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if task.Status.Phase == "" {
		task.Status.Phase = propellerapiv1.TaskPendingPhase
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

// handlePending checks dependencies, selects a proplet, and dispatches the task.
func (r *TaskReconciler) handlePending(ctx context.Context, task *propellerapiv1.Task) (ctrl.Result, error) {
	// Dependency gate: wait until all declared dependencies are terminal.
	if len(task.Spec.DependsOn) > 0 {
		allDone, skip, err := r.evaluateDeps(ctx, task)
		if err != nil {
			return ctrl.Result{}, err
		}
		if skip {
			return r.transitionToSkipped(ctx, task, "run_if condition not met after dependencies completed")
		}
		if !allDone {
			return ctrl.Result{RequeueAfter: defaultDepCheckInterval}, nil
		}
	}

	// Select a proplet.
	propletID, err := r.selectProplet(ctx, task)
	if err != nil {
		// No available proplet — wait and retry.
		log.FromContext(ctx).Info("no available proplet, requeueing", "reason", err.Error())
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}

	// Dispatch based on the proplet backend.
	backend, err := r.determineBackend(ctx, task.Namespace, propletID)
	if err != nil {
		return ctrl.Result{}, err
	}

	switch backend {
	case propellerapiv1.K8sProplet:
		return r.startK8sJob(ctx, task, propletID)
	default:
		return r.startExternalTask(ctx, task, propletID)
	}
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
	shouldSkip := dag.ShouldSkip(task.Spec.DependsOn, task.Spec.RunIf, completed, failed)
	return true, shouldSkip, nil
}

// selectProplet resolves which proplet should execute the task.
// It honours an explicit PropletSelector.PropletID when set, otherwise it
// delegates to the round-robin scheduler across running proplets.
func (r *TaskReconciler) selectProplet(ctx context.Context, task *propellerapiv1.Task) (string, error) {
	if task.Spec.PropletSelector != nil && task.Spec.PropletSelector.PropletID != "" {
		return task.Spec.PropletSelector.PropletID, nil
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
	// Only K8s-Job-backed tasks need active polling; external tasks complete via
	// the MQTT result handler in PropletReconciler.
	jobName := task.Name + "-job"
	job := &batchv1.Job{}
	if err := r.Get(ctx, client.ObjectKey{Name: jobName, Namespace: task.Namespace}, job); err != nil {
		if apierrors.IsNotFound(err) {
			// External task: no job present, wait for MQTT completion.
			return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
		}
		return ctrl.Result{}, err
	}

	if job.Status.Succeeded > 0 {
		return r.handleJobSucceeded(ctx, task, job)
	}
	if job.Status.Failed > 0 {
		return r.handleJobFailed(ctx, task, job)
	}

	return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
}

// handleTerminal handles completed/failed tasks that have IsRecurring set.
// Without a cron parser dependency the controller relies on Status.NextRun
// being set externally or left nil (in which case the task stays terminal).
func (r *TaskReconciler) handleTerminal(ctx context.Context, task *propellerapiv1.Task) (ctrl.Result, error) {
	if !task.Spec.IsRecurring || task.Status.NextRun == nil {
		return ctrl.Result{}, nil
	}

	delay := time.Until(task.Status.NextRun.Time)
	if delay > 0 {
		return ctrl.Result{RequeueAfter: delay}, nil
	}

	// Reset to pending for the next run.
	task.Status.Phase = propellerapiv1.TaskPendingPhase
	task.Status.AssignedProplet = ""
	task.Status.StartedAt = nil
	task.Status.FinishedAt = nil
	task.Status.Error = ""
	task.Status.Results = nil
	task.Status.Conditions = []propellerapiv1.TaskCondition{}
	// Clear NextRun so external scheduling logic can set the next window.
	task.Status.NextRun = nil

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

	configMapName := task.Name + "-config"
	configMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      configMapName,
			Namespace: task.Namespace,
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: task.APIVersion,
					Kind:       task.Kind,
					Name:       task.Name,
					UID:        task.UID,
					Controller: func() *bool { b := true; return &b }(),
				},
			},
		},
		Data: task.Spec.Env,
	}
	if len(task.Spec.File) > 0 {
		if configMap.Data == nil {
			configMap.Data = map[string]string{}
		}
		configMap.Data["wasm_file_provided"] = "true"
	}
	if err := r.Create(ctx, configMap); client.IgnoreAlreadyExists(err) != nil {
		return ctrl.Result{}, err
	}

	jobName := task.Name + "-job"
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobName,
			Namespace: task.Namespace,
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: task.APIVersion,
					Kind:       task.Kind,
					Name:       task.Name,
					UID:        task.UID,
					Controller: func() *bool { b := true; return &b }(),
				},
			},
		},
		Spec: batchv1.JobSpec{
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					RestartPolicy: func() corev1.RestartPolicy {
						if task.Spec.RestartPolicy != "" {
							return task.Spec.RestartPolicy
						}
						if task.Spec.Daemon {
							return corev1.RestartPolicyAlways
						}
						return corev1.RestartPolicyOnFailure
					}(),
					Containers: []corev1.Container{
						{
							Name:  "task",
							Image: task.Spec.ImageURL,
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
							Resources: r.buildResourceRequirements(task.Spec.ResourceRequirements),
						},
					},
				},
			},
		},
	}

	if err := r.Create(ctx, job); err != nil {
		logger.Error(err, "failed to create job", "job", jobName)
		return ctrl.Result{}, err
	}

	now := metav1.Now()
	task.Status.Phase = propellerapiv1.TaskRunningPhase
	task.Status.AssignedProplet = propletID
	task.Status.StartedAt = &now
	r.updateCondition(task, propellerapiv1.StartedType, metav1.ConditionTrue, "Running", "Task is running via K8s Job")
	if err := r.Status().Update(ctx, task); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
}

func (r *TaskReconciler) startExternalTask(ctx context.Context, task *propellerapiv1.Task, propletID string) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	if r.pubsub == nil {
		task.Status.Phase = propellerapiv1.TaskFailedPhase
		now := metav1.Now()
		task.Status.FinishedAt = &now
		task.Status.Error = "MQTT pubsub not configured"
		r.updateCondition(task, propellerapiv1.CompletedType, metav1.ConditionFalse, "MQTTNotConfigured", task.Status.Error)
		_ = r.Status().Update(ctx, task)
		return ctrl.Result{}, nil
	}

	topic := r.baseTopic + "/control/manager/start"

	env := make(map[string]any, len(task.Spec.Env)+2)
	for k, v := range task.Spec.Env {
		env[k] = v
	}
	env["PROPLET_ID"] = propletID
	env["TASK_ID"] = string(task.UID)

	payload := map[string]any{
		"id":                string(task.UID),
		"name":              task.Name,
		"kind":              task.Spec.Kind,
		"image_url":         task.Spec.ImageURL,
		"file":              task.Spec.File,
		"inputs":            task.Spec.Inputs,
		"cli_args":          task.Spec.CLIArgs,
		"env":               env,
		"daemon":            task.Spec.Daemon,
		"mode":              task.Spec.Mode,
		"encrypted":         task.Spec.Encrypted,
		"kbs_resource_path": task.Spec.KBSResourcePath,
		"proplet_id":        propletID,
		"priority":          task.Spec.Priority,
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
			profile["interval"] = mp.Interval.Duration.String()
		}
		payload["monitoring_profile"] = profile
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
		return ctrl.Result{}, err
	}

	return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
}

func (r *TaskReconciler) handleJobSucceeded(ctx context.Context, task *propellerapiv1.Task, job *batchv1.Job) (ctrl.Result, error) {
	now := metav1.Now()
	task.Status.Phase = propellerapiv1.TaskCompletedPhase
	task.Status.FinishedAt = &now
	r.extractAndStoreResult(ctx, task, job)
	r.updateCondition(task, propellerapiv1.CompletedType, metav1.ConditionTrue, "Completed", "Task completed successfully")
	return ctrl.Result{}, r.Status().Update(ctx, task)
}

func (r *TaskReconciler) handleJobFailed(ctx context.Context, task *propellerapiv1.Task, job *batchv1.Job) (ctrl.Result, error) {
	now := metav1.Now()
	task.Status.Phase = propellerapiv1.TaskFailedPhase
	task.Status.FinishedAt = &now
	task.Status.Error = r.extractJobFailureMessage(job)
	r.updateCondition(task, propellerapiv1.CompletedType, metav1.ConditionFalse, "Failed", task.Status.Error)
	return ctrl.Result{}, r.Status().Update(ctx, task)
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
		logger.Error(err, "failed to extract result from job", "job", job.Name)
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

func (r *TaskReconciler) buildResourceRequirements(req *propellerapiv1.PropletResources) corev1.ResourceRequirements {
	if req == nil {
		return corev1.ResourceRequirements{}
	}
	reqs := corev1.ResourceRequirements{}
	if req.CPU != "" {
		reqs.Requests = corev1.ResourceList{corev1.ResourceCPU: resource.MustParse(req.CPU)}
		reqs.Limits = corev1.ResourceList{corev1.ResourceCPU: resource.MustParse(req.CPU)}
	}
	if req.Memory != "" {
		if reqs.Requests == nil {
			reqs.Requests = corev1.ResourceList{}
			reqs.Limits = corev1.ResourceList{}
		}
		reqs.Requests[corev1.ResourceMemory] = resource.MustParse(req.Memory)
		reqs.Limits[corev1.ResourceMemory] = resource.MustParse(req.Memory)
	}
	return reqs
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

// SetupWithManager sets up the controller with the Manager.
func (r *TaskReconciler) SetupWithManager(domainID, channelID string, mgr ctrl.Manager, pubsub mqtt.PubSub, sched scheduler.Scheduler) error {
	r.pubsub = pubsub
	r.sched = sched
	r.domainID = domainID
	r.channelID = channelID
	r.baseTopic = fmt.Sprintf(superMQBaseTopic, domainID, channelID)

	return ctrl.NewControllerManagedBy(mgr).
		For(&propellerapiv1.Task{}).
		Owns(&batchv1.Job{}).
		Complete(r)
}
