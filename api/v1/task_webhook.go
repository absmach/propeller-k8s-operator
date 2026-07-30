package v1

import (
	"context"

	propellercron "github.com/absmach/propeller/internal/cron"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation/field"
	ctrl "sigs.k8s.io/controller-runtime"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

var tasklog = logf.Log.WithName("task-resource")

// SetupTaskWebhookWithManager registers the webhook for Task in the manager.
func SetupTaskWebhookWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr, &Task{}).
		WithDefaulter(&TaskCustomDefaulter{}).
		WithValidator(&TaskCustomValidator{}).
		Complete()
}

// +kubebuilder:webhook:path=/mutate-propeller-propeller-abstractmachines-fr-v1-task,mutating=true,failurePolicy=fail,sideEffects=None,groups=propeller.propeller.absmach.eu,resources=tasks,verbs=create;update,versions=v1,name=mtask-v1.kb.io,admissionReviewVersions=v1

// TaskCustomDefaulter applies defaults to Task resources.
type TaskCustomDefaulter struct{}

// Default sets default values on a Task.
func (d *TaskCustomDefaulter) Default(_ context.Context, task *Task) error {
	tasklog.Info("defaulting", "name", task.Name)

	if task.Spec.Kind == "" {
		task.Spec.Kind = TaskKindStandard
	}
	if task.Spec.Priority == 0 {
		task.Spec.Priority = DefaultPriority
	}
	if task.Spec.Timezone == "" && task.Spec.Schedule != "" {
		task.Spec.Timezone = DefaultTimezone
	}
	return nil
}

// +kubebuilder:webhook:path=/validate-propeller-propeller-abstractmachines-fr-v1-task,mutating=false,failurePolicy=fail,sideEffects=None,groups=propeller.propeller.absmach.eu,resources=tasks,verbs=create;update,versions=v1,name=vtask-v1.kb.io,admissionReviewVersions=v1

// TaskCustomValidator validates Task resources.
type TaskCustomValidator struct{}

// ValidateCreate validates a new Task.
func (v *TaskCustomValidator) ValidateCreate(_ context.Context, task *Task) (admission.Warnings, error) {
	tasklog.Info("validate create", "name", task.Name)
	return nil, v.validateTask(task)
}

// ValidateUpdate validates an updated Task.
func (v *TaskCustomValidator) ValidateUpdate(_ context.Context, _, newObj *Task) (admission.Warnings, error) {
	tasklog.Info("validate update", "name", newObj.Name)
	return nil, v.validateTask(newObj)
}

// ValidateDelete validates Task deletion (no-op).
func (v *TaskCustomValidator) ValidateDelete(_ context.Context, _ *Task) (admission.Warnings, error) {
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
		schema.GroupKind{Group: "propeller.propeller.absmach.eu", Kind: "Task"},
		task.Name,
		allErrs,
	)
}
