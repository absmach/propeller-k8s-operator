package v1

import (
	"context"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation/field"
	ctrl "sigs.k8s.io/controller-runtime"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

var propletlog = logf.Log.WithName("proplet-resource")

// SetupPropletWebhookWithManager registers the webhook for Proplet in the manager.
func SetupPropletWebhookWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr, &Proplet{}).
		WithValidator(&PropletCustomValidator{}).
		Complete()
}

// +kubebuilder:webhook:path=/validate-propeller-propeller-abstractmachines-fr-v1-proplet,mutating=false,failurePolicy=fail,sideEffects=None,groups=propeller.propeller.absmach.eu,resources=proplets,verbs=create;update,versions=v1,name=vproplet-v1.kb.io,admissionReviewVersions=v1

// PropletCustomValidator validates Proplet resources.
type PropletCustomValidator struct{}

// ValidateCreate validates a new Proplet.
func (v *PropletCustomValidator) ValidateCreate(_ context.Context, proplet *Proplet) (admission.Warnings, error) {
	propletlog.Info("validate create", "name", proplet.Name)
	return nil, v.validateProplet(proplet)
}

// ValidateUpdate validates an updated Proplet.
func (v *PropletCustomValidator) ValidateUpdate(_ context.Context, _, newObj *Proplet) (admission.Warnings, error) {
	propletlog.Info("validate update", "name", newObj.Name)
	return nil, v.validateProplet(newObj)
}

// ValidateDelete validates Proplet deletion (no-op).
func (v *PropletCustomValidator) ValidateDelete(_ context.Context, _ *Proplet) (admission.Warnings, error) {
	return nil, nil
}

func (v *PropletCustomValidator) validateProplet(proplet *Proplet) error {
	var allErrs field.ErrorList

	cfg := proplet.Spec.ConnectionConfig
	hasPlain := cfg.APIKey != ""
	hasRef := cfg.APIKeySecretRef != nil

	if !hasPlain && !hasRef {
		allErrs = append(allErrs, field.Required(
			field.NewPath("spec", "connectionConfig", "apiKey"),
			"exactly one of apiKey or apiKeySecretRef must be set",
		))
	}
	if hasPlain && hasRef {
		allErrs = append(allErrs, field.Invalid(
			field.NewPath("spec", "connectionConfig", "apiKey"),
			cfg.APIKey,
			"apiKey and apiKeySecretRef are mutually exclusive; set exactly one",
		))
	}

	if len(allErrs) == 0 {
		return nil
	}
	return apierrors.NewInvalid(
		schema.GroupKind{Group: "propeller.propeller.absmach.eu", Kind: "Proplet"},
		proplet.Name,
		allErrs,
	)
}
