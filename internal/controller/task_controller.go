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
	"github.com/absmach/propeller/internal/mqtt"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

const (
	defaultPropletID = "default"
)

type TaskReconciler struct {
	client.Client

	Scheme    *runtime.Scheme
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
// +kubebuilder:rbac:groups=propeller.propeller.abstractmachines.fr,resources=proplets,verbs=get

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

	var (
		result ctrl.Result
		err    error
	)

	switch task.Status.Phase {
	case propellerapiv1.TaskPendingPhase:
		result, err = r.handlePending(ctx, task)
	case propellerapiv1.TaskScheduledPhase, propellerapiv1.TaskRunningPhase:
		result, err = r.handleRunning(ctx, task)
	case propellerapiv1.TaskCompletedPhase, propellerapiv1.TaskFailedPhase:
		return ctrl.Result{}, nil
	default:
		logger.Info("unknown phase", "phase", task.Status.Phase)

		return ctrl.Result{}, nil
	}

	return result, err
}

func (r *TaskReconciler) SetupWithManager(domainID, channelID string, mgr ctrl.Manager, pubsub mqtt.PubSub) error {
	r.pubsub = pubsub
	r.domainID = domainID
	r.channelID = channelID
	r.baseTopic = fmt.Sprintf(superMQBaseTopic, domainID, channelID)

	return ctrl.NewControllerManagedBy(mgr).
		For(&propellerapiv1.Task{}).
		Owns(&batchv1.Job{}).
		Complete(r)
}

func (r *TaskReconciler) handlePending(ctx context.Context, task *propellerapiv1.Task) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	propletID := r.resolvePropletID(task)
	if propletID == "" {
		propletID = defaultPropletID
		logger.Info("no proplet specified, using default", "proplet", propletID)
	}

	// Decide backend based on proplet type.
	backend, err := r.determineBackend(ctx, task.Namespace, propletID)
	if err != nil {
		return ctrl.Result{}, err
	}

	switch backend {
	case propellerapiv1.K8sProplet:
		return r.startK8sJob(ctx, task, propletID)
	case propellerapiv1.ExternalProplet:
		return r.startExternalTask(ctx, task, propletID)
	default:
		logger.Info("unknown proplet backend type, defaulting to external", "proplet", propletID)

		return r.startExternalTask(ctx, task, propletID)
	}
}

func (r *TaskReconciler) handleRunning(ctx context.Context, task *propellerapiv1.Task) (ctrl.Result, error) {
	jobName := task.Name + "-job"
	job := &batchv1.Job{}
	if err := r.Get(ctx, client.ObjectKey{
		Name:      jobName,
		Namespace: task.Namespace,
	}, job); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if job.Status.Succeeded > 0 {
		return r.handleJobSucceeded(ctx, task, job)
	}

	if job.Status.Failed > 0 {
		return r.handleJobFailed(ctx, task, job)
	}

	return ctrl.Result{RequeueAfter: time.Second * 10}, nil
}

func (r *TaskReconciler) determineBackend(ctx context.Context, namespace, propletID string) (propellerapiv1.PropletKind, error) {
	if propletID == "" {
		return propellerapiv1.ExternalProplet, nil
	}

	proplet := &propellerapiv1.Proplet{}
	if err := r.Get(ctx, client.ObjectKey{Name: propletID, Namespace: namespace}, proplet); err != nil {
		// If we can't fetch, fall back to external so we don't assume cluster execution.
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
					Controller: func() *bool {
						b := true

						return &b
					}(),
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

	if err := r.Create(ctx, configMap); err != nil {
		if client.IgnoreAlreadyExists(err) != nil {
			return ctrl.Result{}, err
		}
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
					Controller: func() *bool {
						b := true

						return &b
					}(),
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
										LocalObjectReference: corev1.LocalObjectReference{
											Name: configMapName,
										},
									},
								},
							},
							Env: []corev1.EnvVar{
								{
									Name:  "PROPLET_ID",
									Value: propletID,
								},
								{
									Name:  "TASK_ID",
									Value: task.Name,
								},
							},
							Resources: func() corev1.ResourceRequirements {
								if task.Spec.ResourceRequirements != nil {
									req := corev1.ResourceRequirements{}
									if task.Spec.ResourceRequirements.CPU != "" {
										req.Requests = corev1.ResourceList{
											corev1.ResourceCPU: resource.MustParse(task.Spec.ResourceRequirements.CPU),
										}
										req.Limits = corev1.ResourceList{
											corev1.ResourceCPU: resource.MustParse(task.Spec.ResourceRequirements.CPU),
										}
									}
									if task.Spec.ResourceRequirements.Memory != "" {
										if req.Requests == nil {
											req.Requests = corev1.ResourceList{}
										}
										if req.Limits == nil {
											req.Limits = corev1.ResourceList{}
										}
										req.Requests[corev1.ResourceMemory] = resource.MustParse(task.Spec.ResourceRequirements.Memory)
										req.Limits[corev1.ResourceMemory] = resource.MustParse(task.Spec.ResourceRequirements.Memory)
									}
									return req
								}

								return corev1.ResourceRequirements{}
							}(),
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

	r.updateCondition(task, propellerapiv1.StartedType, metav1.ConditionTrue, "Running", "Task is running")
	if err := r.Status().Update(ctx, task); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{RequeueAfter: time.Second * 5}, nil
}

func (r *TaskReconciler) startExternalTask(ctx context.Context, task *propellerapiv1.Task, propletID string) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	if r.pubsub == nil {
		logger.Error(nil, "mqtt pubsub not configured for external execution")

		now := metav1.Now()
		task.Status.Phase = propellerapiv1.TaskFailedPhase
		task.Status.FinishedAt = &now
		task.Status.Error = "mqtt pubsub not configured for external execution"
		r.updateCondition(task, propellerapiv1.CompletedType, metav1.ConditionFalse, "MQTTNotConfigured", task.Status.Error)
		_ = r.Status().Update(ctx, task)

		return ctrl.Result{}, nil
	}

	topic := r.baseTopic + "/control/manager/start"

	env := map[string]any{}
	for k, v := range task.Spec.Env {
		env[k] = v
	}

	env["PROPLET_ID"] = propletID
	env["TASK_ID"] = string(task.UID)

	payload := map[string]any{
		"id":        string(task.UID),
		"name":      task.Name,
		"image_url": task.Spec.ImageURL,
		"file":      task.Spec.File,
		"inputs":    task.Spec.Inputs,
		"cli_args":  task.Spec.CLIArgs,
		"env":       env,
		"daemon":    task.Spec.Daemon,
		"mode":      task.Spec.Mode,
	}

	if err := r.pubsub.Publish(topic, payload); err != nil {
		logger.Error(err, "failed to publish external task start command")

		return ctrl.Result{}, err
	}

	now := metav1.Now()
	task.Status.Phase = propellerapiv1.TaskRunningPhase
	task.Status.AssignedProplet = propletID
	task.Status.StartedAt = &now

	r.updateCondition(task, propellerapiv1.StartedType, metav1.ConditionTrue, "Running", "External task is running")
	if err := r.Status().Update(ctx, task); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{RequeueAfter: time.Second * 30}, nil
}

func (r *TaskReconciler) resolvePropletID(task *propellerapiv1.Task) string {
	if task.Spec.PropletSelector != nil {
		if task.Spec.PropletSelector.PropletID != "" {
			return task.Spec.PropletSelector.PropletID
		}
	}

	return ""
}

func (r *TaskReconciler) handleJobSucceeded(ctx context.Context, task *propellerapiv1.Task, job *batchv1.Job) (ctrl.Result, error) {
	now := metav1.Now()
	task.Status.Phase = propellerapiv1.TaskCompletedPhase
	task.Status.FinishedAt = &now

	r.extractAndStoreResult(ctx, task, job)

	r.updateCondition(task, propellerapiv1.CompletedType, metav1.ConditionTrue, "Completed", "Task completed successfully")

	if err := r.Status().Update(ctx, task); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

func (r *TaskReconciler) handleJobFailed(ctx context.Context, task *propellerapiv1.Task, job *batchv1.Job) (ctrl.Result, error) {
	now := metav1.Now()
	task.Status.Phase = propellerapiv1.TaskFailedPhase
	task.Status.FinishedAt = &now

	errorMsg := r.extractJobFailureMessage(job)
	task.Status.Error = errorMsg

	r.updateCondition(task, propellerapiv1.CompletedType, metav1.ConditionFalse, "Failed", errorMsg)

	if err := r.Status().Update(ctx, task); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

func (r *TaskReconciler) extractJobFailureMessage(job *batchv1.Job) string {
	for _, condition := range job.Status.Conditions {
		if condition.Type == batchv1.JobFailed && condition.Message != "" {
			return condition.Message
		}
	}

	return "Job failed"
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

	resultJSON, err := json.Marshal(result)
	if err == nil {
		task.Status.Results = &apiextensionsv1.JSON{Raw: resultJSON}
	}
}

func (r *TaskReconciler) updateCondition(task *propellerapiv1.Task, conditionType propellerapiv1.TaskConditionType, status metav1.ConditionStatus, reason, message string) {
	if task.Status.Conditions == nil {
		task.Status.Conditions = []propellerapiv1.TaskCondition{}
	}

	now := metav1.Now()
	condition := propellerapiv1.TaskCondition{
		Type:               conditionType,
		Status:             status,
		LastTransitionTime: now,
		Reason:             reason,
		Message:            message,
	}

	found := false
	for i, c := range task.Status.Conditions {
		if c.Type == conditionType {
			task.Status.Conditions[i] = condition
			found = true

			break
		}
	}
	if !found {
		task.Status.Conditions = append(task.Status.Conditions, condition)
	}
}
