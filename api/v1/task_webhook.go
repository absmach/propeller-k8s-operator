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
	"context"
	"errors"

	propellercron "github.com/absmach/propeller/internal/cron"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation/field"
	ctrl "sigs.k8s.io/controller-runtime"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

var tasklog = logf.Log.WithName("task-resource")

// SetupTaskWebhookWithManager registers the webhook for Task in the manager.
func SetupTaskWebhookWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr).
		For(&Task{}).
		WithDefaulter(&TaskCustomDefaulter{}).
		WithValidator(&TaskCustomValidator{}).
		Complete()
}

// +kubebuilder:webhook:path=/mutate-propeller-propeller-abstractmachines-fr-v1-task,mutating=true,failurePolicy=fail,sideEffects=None,groups=propeller.propeller.abstractmachines.fr,resources=tasks,verbs=create;update,versions=v1,name=mtask-v1.kb.io,admissionReviewVersions=v1

// TaskCustomDefaulter applies defaults to Task resources.
type TaskCustomDefaulter struct{}

var _ webhook.CustomDefaulter = &TaskCustomDefaulter{}

// Default sets default values on a Task.
func (d *TaskCustomDefaulter) Default(_ context.Context, obj runtime.Object) error {
	task, ok := obj.(*Task)
	if !ok {
		return errors.New("expected a Task object")
	}
	tasklog.Info("defaulting", "name", task.Name)

	if task.Spec.Kind == "" {
		task.Spec.Kind = TaskKindStandard
	}
	if task.Spec.Priority == 0 {
		task.Spec.Priority = 50
	}
	if task.Spec.Timezone == "" && task.Spec.Schedule != "" {
		task.Spec.Timezone = "UTC"
	}
	return nil
}

// +kubebuilder:webhook:path=/validate-propeller-propeller-abstractmachines-fr-v1-task,mutating=false,failurePolicy=fail,sideEffects=None,groups=propeller.propeller.abstractmachines.fr,resources=tasks,verbs=create;update,versions=v1,name=vtask-v1.kb.io,admissionReviewVersions=v1

// TaskCustomValidator validates Task resources.
type TaskCustomValidator struct{}

var _ webhook.CustomValidator = &TaskCustomValidator{}

// ValidateCreate validates a new Task.
func (v *TaskCustomValidator) ValidateCreate(_ context.Context, obj runtime.Object) (admission.Warnings, error) {
	task, ok := obj.(*Task)
	if !ok {
		return nil, errors.New("expected a Task object")
	}
	tasklog.Info("validate create", "name", task.Name)
	return nil, v.validateTask(task)
}

// ValidateUpdate validates an updated Task.
func (v *TaskCustomValidator) ValidateUpdate(_ context.Context, _, newObj runtime.Object) (admission.Warnings, error) {
	task, ok := newObj.(*Task)
	if !ok {
		return nil, errors.New("expected a Task object")
	}
	tasklog.Info("validate update", "name", task.Name)
	return nil, v.validateTask(task)
}

// ValidateDelete validates Task deletion (no-op).
func (v *TaskCustomValidator) ValidateDelete(_ context.Context, _ runtime.Object) (admission.Warnings, error) {
	return nil, nil
}

func (v *TaskCustomValidator) validateTask(task *Task) error {
	var allErrs field.ErrorList

	// Broadcast and explicit PropletID are mutually exclusive.
	if task.Spec.Broadcast &&
		task.Spec.PropletSelector != nil &&
		task.Spec.PropletSelector.PropletID != "" {
		allErrs = append(allErrs, field.Invalid(
			field.NewPath("spec", "broadcast"),
			task.Spec.Broadcast,
			"broadcast must not be set when propletSelector.propletId is specified",
		))
	}

	// DependsOn requires a grouping identifier (WorkflowID or JobID).
	if len(task.Spec.DependsOn) > 0 && task.Spec.WorkflowID == "" && task.Spec.JobID == "" {
		allErrs = append(allErrs, field.Required(
			field.NewPath("spec", "workflowId"),
			"workflowId or jobId is required when dependsOn is specified",
		))
	}

	// Validate cron expression when set.
	if task.Spec.Schedule != "" {
		if _, err := propellercron.ParseCronExpression(task.Spec.Schedule); err != nil {
			allErrs = append(allErrs, field.Invalid(
				field.NewPath("spec", "schedule"),
				task.Spec.Schedule,
				"invalid cron expression: must be a valid 5-field cron expression",
			))
		}
	}

	// IsRecurring requires a Schedule.
	if task.Spec.IsRecurring && task.Spec.Schedule == "" {
		allErrs = append(allErrs, field.Required(
			field.NewPath("spec", "schedule"),
			"schedule is required when isRecurring is true",
		))
	}

	if len(allErrs) == 0 {
		return nil
	}

	return apierrors.NewInvalid(
		schema.GroupKind{Group: "propeller.propeller.abstractmachines.fr", Kind: "Task"},
		task.Name,
		allErrs,
	)
}
