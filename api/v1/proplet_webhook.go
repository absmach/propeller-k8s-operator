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

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation/field"
	ctrl "sigs.k8s.io/controller-runtime"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

var propletlog = logf.Log.WithName("proplet-resource")

// SetupPropletWebhookWithManager registers the webhook for Proplet in the manager.
func SetupPropletWebhookWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr).
		For(&Proplet{}).
		WithValidator(&PropletCustomValidator{}).
		Complete()
}

// +kubebuilder:webhook:path=/validate-propeller-propeller-abstractmachines-fr-v1-proplet,mutating=false,failurePolicy=fail,sideEffects=None,groups=propeller.propeller.abstractmachines.fr,resources=proplets,verbs=create;update,versions=v1,name=vproplet-v1.kb.io,admissionReviewVersions=v1

// PropletCustomValidator validates Proplet resources.
type PropletCustomValidator struct{}

var _ webhook.CustomValidator = &PropletCustomValidator{}

// ValidateCreate validates a new Proplet.
func (v *PropletCustomValidator) ValidateCreate(_ context.Context, obj runtime.Object) (admission.Warnings, error) {
	proplet, ok := obj.(*Proplet)
	if !ok {
		return nil, errors.New("expected a Proplet object")
	}
	propletlog.Info("validate create", "name", proplet.Name)
	return nil, v.validateProplet(proplet)
}

// ValidateUpdate validates an updated Proplet.
func (v *PropletCustomValidator) ValidateUpdate(_ context.Context, _, newObj runtime.Object) (admission.Warnings, error) {
	proplet, ok := newObj.(*Proplet)
	if !ok {
		return nil, errors.New("expected a Proplet object")
	}
	propletlog.Info("validate update", "name", proplet.Name)
	return nil, v.validateProplet(proplet)
}

// ValidateDelete validates Proplet deletion (no-op).
func (v *PropletCustomValidator) ValidateDelete(_ context.Context, _ runtime.Object) (admission.Warnings, error) {
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
		schema.GroupKind{Group: "propeller.propeller.abstractmachines.fr", Kind: "Proplet"},
		proplet.Name,
		allErrs,
	)
}
