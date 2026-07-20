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
	"sort"
	"strconv"
	"strings"
	"time"

	propellerapiv1 "github.com/absmach/propeller/api/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

const (
	defaultRequeueDelay = 10 * time.Second

	// labelJobName tags every Task created by a PropellerJob so they can be
	// listed efficiently without a field indexer.
	labelJobName = "propeller.propeller.absmach.eu/job"

	// annotationSpecIndex records the task's position in PropellerJobSpec.Tasks
	// (0-based).  Used by sequential mode to determine which task to create next.
	annotationSpecIndex = "propeller.propeller.absmach.eu/spec-index"

	// annotationSpecName records the TaskSpec.Name value so DependsOn cross-
	// references can be resolved to Kubernetes resource names.
	annotationSpecName = "propeller.propeller.absmach.eu/spec-name"
)

// PropellerJobReconciler reconciles a PropellerJob object.
type PropellerJobReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=propeller.propeller.absmach.eu,resources=propellerjobs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=propeller.propeller.absmach.eu,resources=propellerjobs/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=propeller.propeller.absmach.eu,resources=propellerjobs/finalizers,verbs=update
// +kubebuilder:rbac:groups=propeller.propeller.absmach.eu,resources=tasks,verbs=get;list;watch;create;update;patch;delete

func (r *PropellerJobReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	job := &propellerapiv1.PropellerJob{}
	if err := r.Get(ctx, req.NamespacedName, job); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if job.Status.Phase == "" {
		now := metav1.Now()
		job.Status.Phase = propellerapiv1.JobPhasePending
		job.Status.CreatedAt = &now
		job.Status.UpdatedAt = &now
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

// handlePending creates the initial set of Task resources and transitions the
// job to Running.
//
//   - parallel:     create all tasks immediately (no ordering)
//   - configurable: create all tasks immediately; DependsOn is resolved to K8s
//     names so the TaskReconciler gates execution automatically
//   - sequential:   create only the first task (index 0); subsequent tasks are
//     created one at a time by handleRunning
func (r *PropellerJobReconciler) handlePending(ctx context.Context, job *propellerapiv1.PropellerJob) (ctrl.Result, error) {
	switch job.Spec.ExecutionMode {
	case propellerapiv1.ExecutionModeSequential:
		if len(job.Spec.Tasks) > 0 {
			if err := r.createTask(ctx, job, job.Spec.Tasks[0], 0); err != nil {
				return ctrl.Result{}, err
			}
		}
	default: // parallel and configurable
		for i, taskSpec := range job.Spec.Tasks {
			if err := r.createTask(ctx, job, taskSpec, i); err != nil {
				return ctrl.Result{}, err
			}
		}
	}

	now := metav1.Now()
	job.Status.Phase = propellerapiv1.JobPhaseRunning
	job.Status.StartTime = &now
	job.Status.UpdatedAt = &now
	if err := r.Status().Update(ctx, job); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{RequeueAfter: defaultRequeueDelay}, nil
}

// handleRunning monitors owned Tasks and, for sequential mode, creates the
// next task once the current one is terminal.
func (r *PropellerJobReconciler) handleRunning(ctx context.Context, job *propellerapiv1.PropellerJob) (ctrl.Result, error) {
	taskList, err := r.listOwnedTasks(ctx, job)
	if err != nil {
		return ctrl.Result{}, err
	}

	var completed, failed, skipped, interrupted int
	for _, t := range taskList.Items {
		switch t.Status.Phase {
		case propellerapiv1.TaskCompletedPhase:
			completed++
		case propellerapiv1.TaskFailedPhase:
			failed++
		case propellerapiv1.TaskSkippedPhase:
			skipped++
		case propellerapiv1.TaskInterruptedPhase:
			interrupted++
		}
	}

	for _, refName := range job.Spec.TaskRefs {
		ref := &propellerapiv1.Task{}
		if err := r.Get(ctx, client.ObjectKey{Name: refName, Namespace: job.Namespace}, ref); err != nil {
			continue // not yet created or not found — still pending
		}
		switch ref.Status.Phase {
		case propellerapiv1.TaskCompletedPhase:
			completed++
		case propellerapiv1.TaskFailedPhase:
			failed++
		case propellerapiv1.TaskSkippedPhase:
			skipped++
		case propellerapiv1.TaskInterruptedPhase:
			interrupted++
		}
	}

	job.Status.CompletedCount = completed
	job.Status.FailedCount = failed
	job.Status.SkippedCount = skipped
	job.Status.InterruptedCount = interrupted
	now := metav1.Now()

	if job.Spec.ExecutionMode == propellerapiv1.ExecutionModeSequential {
		if result, err := r.advanceSequential(ctx, job, taskList); err != nil || result.RequeueAfter > 0 {
			job.Status.UpdatedAt = &now
			if updateErr := r.Status().Update(ctx, job); updateErr != nil {
				return ctrl.Result{}, updateErr
			}
			return result, err
		}
	}

	totalTasks := job.Status.TaskCount
	finishedTasks := completed + failed + skipped + interrupted

	if finishedTasks >= totalTasks {
		job.Status.FinishTime = &now
		job.Status.UpdatedAt = &now
		if failed > 0 || interrupted > 0 {
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

// advanceSequential creates the next task in the spec when all previously
// created tasks have reached a terminal state.
func (r *PropellerJobReconciler) advanceSequential(ctx context.Context, job *propellerapiv1.PropellerJob, taskList *propellerapiv1.TaskList) (ctrl.Result, error) {
	if len(job.Spec.Tasks) == 0 {
		return ctrl.Result{}, nil
	}

	// Find the highest spec-index already created and check if it is terminal.
	highestIndex := -1
	allTerminal := true
	for _, t := range taskList.Items {
		idxStr := t.Annotations[annotationSpecIndex]
		idx, err := strconv.Atoi(idxStr)
		if err != nil {
			continue
		}
		if idx > highestIndex {
			highestIndex = idx
		}
		if !isTerminalPhase(t.Status.Phase) {
			allTerminal = false
		}
	}

	if !allTerminal {
		return ctrl.Result{RequeueAfter: defaultRequeueDelay}, nil
	}

	nextIndex := highestIndex + 1
	if nextIndex >= len(job.Spec.Tasks) {
		return ctrl.Result{}, nil // all tasks have been created
	}

	if err := r.createTask(ctx, job, job.Spec.Tasks[nextIndex], nextIndex); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: defaultRequeueDelay}, nil
}

// createTask creates a single Task CRD owned by the job.
//
// For configurable mode, DependsOn values (which reference other TaskSpec.Name
// fields) are resolved to their Kubernetes resource names: {job-name}-{spec-name}.
// This lets the TaskReconciler gate execution using only Kubernetes object names.
func (r *PropellerJobReconciler) createTask(ctx context.Context, job *propellerapiv1.PropellerJob, spec propellerapiv1.TaskSpec, specIndex int) error {
	taskName := taskKubeName(job.Name, spec.Name)

	// Resolve DependsOn for configurable mode.
	resolvedDeps := spec.DependsOn
	if job.Spec.ExecutionMode == propellerapiv1.ExecutionModeConfigurable && len(spec.DependsOn) > 0 {
		resolvedDeps = make([]string, len(spec.DependsOn))
		for i, dep := range spec.DependsOn {
			resolvedDeps[i] = taskKubeName(job.Name, dep)
		}
	}

	resolvedSpec := spec
	resolvedSpec.DependsOn = resolvedDeps
	resolvedSpec.JobID = string(job.UID)

	task := &propellerapiv1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      taskName,
			Namespace: job.Namespace,
			Labels: map[string]string{
				labelJobName: job.Name,
			},
			Annotations: map[string]string{
				annotationSpecIndex: strconv.Itoa(specIndex),
				annotationSpecName:  spec.Name,
			},
		},
		Spec: resolvedSpec,
	}

	if err := controllerutil.SetControllerReference(job, task, r.Scheme); err != nil {
		return err
	}

	if err := r.Create(ctx, task); client.IgnoreAlreadyExists(err) != nil {
		log.FromContext(ctx).Error(err, "failed to create task", "task", taskName)
		return err
	}
	return nil
}

// listOwnedTasks returns all Tasks carrying the job's ownership label, sorted
// by their spec-index annotation so sequential logic sees a deterministic order.
func (r *PropellerJobReconciler) listOwnedTasks(ctx context.Context, job *propellerapiv1.PropellerJob) (*propellerapiv1.TaskList, error) {
	taskList := &propellerapiv1.TaskList{}
	if err := r.List(ctx, taskList,
		client.InNamespace(job.Namespace),
		client.MatchingLabels{labelJobName: job.Name},
	); err != nil {
		return nil, err
	}

	sort.Slice(taskList.Items, func(i, j int) bool {
		a, _ := strconv.Atoi(taskList.Items[i].Annotations[annotationSpecIndex])
		b, _ := strconv.Atoi(taskList.Items[j].Annotations[annotationSpecIndex])
		return a < b
	})
	return taskList, nil
}

// taskKubeName converts a job name and a spec-level task name to a Kubernetes
// resource name that is safe to use in DependsOn references.
// Kubernetes names must be lowercase alphanumeric with hyphens.
func taskKubeName(jobName, specName string) string {
	// Replace underscores/spaces with hyphens and lowercase to satisfy
	// Kubernetes RFC 1123 subdomain naming rules.
	safe := strings.NewReplacer("_", "-", " ", "-").Replace(specName)
	return strings.ToLower(fmt.Sprintf("%s-%s", jobName, safe))
}

func isTerminalPhase(phase propellerapiv1.TaskPhase) bool {
	switch phase {
	case propellerapiv1.TaskCompletedPhase,
		propellerapiv1.TaskFailedPhase,
		propellerapiv1.TaskSkippedPhase,
		propellerapiv1.TaskInterruptedPhase:
		return true
	}
	return false
}

// SetupWithManager sets up the controller with the Manager.
func (r *PropellerJobReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&propellerapiv1.PropellerJob{}).
		Owns(&propellerapiv1.Task{}).
		Complete(r)
}
