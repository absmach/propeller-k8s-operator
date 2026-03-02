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
	"fmt"
	"time"

	propellerapiv1 "github.com/absmach/propeller/api/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

const (
	defaultRequeueDelay = 10 * time.Second
)

// PropellerJobReconciler reconciles a PropellerJob object
type PropellerJobReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=propeller.propeller.abstractmachines.fr,resources=propellerjobs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=propeller.propeller.abstractmachines.fr,resources=propellerjobs/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=propeller.propeller.abstractmachines.fr,resources=propellerjobs/finalizers,verbs=update
// +kubebuilder:rbac:groups=propeller.propeller.abstractmachines.fr,resources=tasks,verbs=get;list;watch;create;update;patch;delete

// Reconcile handles the reconciliation loop for PropellerJob resources
func (r *PropellerJobReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	job := &propellerapiv1.PropellerJob{}
	if err := r.Get(ctx, req.NamespacedName, job); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Initialize status if needed
	if job.Status.Phase == "" {
		job.Status.Phase = propellerapiv1.JobPhasePending
		job.Status.TaskCount = len(job.Spec.Tasks) + len(job.Spec.TaskRefs)
		if err := r.Status().Update(ctx, job); err != nil {
			return ctrl.Result{}, err
		}
	}

	switch job.Status.Phase {
	case propellerapiv1.JobPhasePending:
		return r.handlePending(ctx, job)
	case propellerapiv1.JobPhaseRunning:
		return r.handleRunning(ctx, job)
	case propellerapiv1.JobPhaseCompleted, propellerapiv1.JobPhaseFailed:
		return ctrl.Result{}, nil
	default:
		logger.Info("unknown job phase", "phase", job.Status.Phase)
		return ctrl.Result{}, nil
	}
}

// handlePending starts the job execution
func (r *PropellerJobReconciler) handlePending(ctx context.Context, job *propellerapiv1.PropellerJob) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	// Create tasks from inline specs
	for i, taskSpec := range job.Spec.Tasks {
		taskName := fmt.Sprintf("%s-task-%d", job.Name, i)
		task := &propellerapiv1.Task{
			ObjectMeta: metav1.ObjectMeta{
				Name:      taskName,
				Namespace: job.Namespace,
				OwnerReferences: []metav1.OwnerReference{
					{
						APIVersion: job.APIVersion,
						Kind:       job.Kind,
						Name:       job.Name,
						UID:        job.UID,
						Controller: func() *bool {
							b := true
							return &b
						}(),
					},
				},
			},
			Spec: taskSpec,
		}

		// Set the JobID reference
		task.Spec.JobID = string(job.UID)

		if err := r.Create(ctx, task); err != nil {
			if client.IgnoreAlreadyExists(err) != nil {
				logger.Error(err, "failed to create task", "task", taskName)
				return ctrl.Result{}, err
			}
		}
	}

	// Update job to Running
	now := metav1.Now()
	job.Status.Phase = propellerapiv1.JobPhaseRunning
	job.Status.StartTime = &now
	if err := r.Status().Update(ctx, job); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{RequeueAfter: defaultRequeueDelay}, nil
}

// handleRunning monitors the tasks and updates job status
func (r *PropellerJobReconciler) handleRunning(ctx context.Context, job *propellerapiv1.PropellerJob) (ctrl.Result, error) {
	// List owned tasks
	taskList := &propellerapiv1.TaskList{}
	if err := r.List(ctx, taskList, client.InNamespace(job.Namespace), client.MatchingFields{
		"spec.jobID": string(job.UID),
	}); err != nil {
		// Fallback to listing all tasks and filtering
		if err := r.List(ctx, taskList, client.InNamespace(job.Namespace)); err != nil {
			return ctrl.Result{}, err
		}
	}

	var completed, failed, skipped int
	for _, task := range taskList.Items {
		if task.Spec.JobID != string(job.UID) {
			continue
		}
		switch task.Status.Phase {
		case propellerapiv1.TaskCompletedPhase:
			completed++
		case propellerapiv1.TaskFailedPhase:
			failed++
		case propellerapiv1.TaskSkippedPhase:
			skipped++
		}
	}

	job.Status.CompletedCount = completed
	job.Status.FailedCount = failed
	job.Status.SkippedCount = skipped

	// Check if all tasks are done
	totalTasks := job.Status.TaskCount
	finishedTasks := completed + failed + skipped

	if finishedTasks >= totalTasks {
		now := metav1.Now()
		job.Status.FinishTime = &now
		if failed > 0 {
			job.Status.Phase = propellerapiv1.JobPhaseFailed
		} else {
			job.Status.Phase = propellerapiv1.JobPhaseCompleted
		}
	}

	if err := r.Status().Update(ctx, job); err != nil {
		return ctrl.Result{}, err
	}

	if job.Status.Phase == propellerapiv1.JobPhaseRunning {
		return ctrl.Result{RequeueAfter: defaultRequeueDelay}, nil
	}

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager
func (r *PropellerJobReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&propellerapiv1.PropellerJob{}).
		Owns(&propellerapiv1.Task{}).
		Complete(r)
}
