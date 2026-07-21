package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	propellerv1 "github.com/absmach/propeller/api/v1"
	"github.com/absmach/propeller/internal/mqtt"
	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

type FederatedJobReconciler struct {
	client.Client

	Scheme    *runtime.Scheme
	pubsub    mqtt.PubSub
	baseTopic string
	namespace string
}

const federatedJobConditionReady = "Ready"

// +kubebuilder:rbac:groups=propeller.propeller.absmach.eu,resources=federatedjobs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=propeller.propeller.absmach.eu,resources=federatedjobs/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=propeller.propeller.absmach.eu,resources=trainingrounds,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=propeller.propeller.absmach.eu,resources=trainingrounds/status,verbs=get;update;patch

func (r *FederatedJobReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	namespace := req.Namespace

	_ = namespace

	federatedJob := &propellerv1.FederatedJob{}
	if err := r.Get(ctx, req.NamespacedName, federatedJob); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if federatedJob.Status.Phase == "" {
		federatedJob.Status.Phase = phasePending
		if err := r.Status().Update(ctx, federatedJob); err != nil {
			return statusUpdateError(err)
		}
	}

	var (
		result ctrl.Result
		err    error
	)

	switch federatedJob.Status.Phase {
	case phasePending:
		result, err = r.handlePending(ctx, federatedJob)
	case phaseRunning:
		result, err = r.handleRunning(ctx, federatedJob)
	case phaseCompleted, phaseFailed:
		return ctrl.Result{}, nil
	default:
		logger.Info("unknown phase", "phase", federatedJob.Status.Phase)

		return ctrl.Result{}, nil
	}

	return result, err
}

func (r *FederatedJobReconciler) SetupWithManager(mgr ctrl.Manager, pubsub mqtt.PubSub, baseTopic, namespace string) error {
	r.pubsub = pubsub
	r.baseTopic = baseTopic
	r.namespace = namespace

	if r.pubsub != nil {
		// Subscribe to FL round start messages from the FL coordinator.
		flRoundStartTopic := r.baseTopic + "/fl/rounds/start"
		if err := r.pubsub.Subscribe(
			flRoundStartTopic,
			func(_ string, msg map[string]any) error {
				return r.mqttFLRoundStartHandler(context.Background(), msg)
			},
		); err != nil {
			return err
		}

		// Subscribe to FL update submissions from proplets.
		flUpdateTopic := r.baseTopic + "/fl/rounds/+/updates/+"
		if err := r.pubsub.Subscribe(
			flUpdateTopic,
			func(_ string, msg map[string]any) error {
				return r.mqttFLUpdateHandler(context.Background(), msg)
			},
		); err != nil {
			return err
		}
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&propellerv1.FederatedJob{}).
		Owns(&propellerv1.TrainingRound{}).
		Complete(r)
}

// mqttFLRoundStartHandler handles FL round start messages from the FL
// coordinator.  It creates or updates the corresponding FederatedJob and
// TrainingRound resources.
func (r *FederatedJobReconciler) mqttFLRoundStartHandler(ctx context.Context, msg map[string]any) error {
	logger := log.FromContext(ctx)

	roundID, _ := msg["round_id"].(string)
	modelURI, _ := msg["model_uri"].(string)
	taskWasmImage, _ := msg["task_wasm_image"].(string)
	kOfN, _ := msg["k_of_n"].(float64)
	timeoutS, _ := msg["timeout_seconds"].(float64)

	participantsRaw, _ := msg["participants"].([]any)
	participants := make([]string, 0, len(participantsRaw))
	for _, p := range participantsRaw {
		if s, ok := p.(string); ok {
			participants = append(participants, s)
		}
	}

	if roundID == "" || len(participants) == 0 {
		logger.Info("invalid FL round start message", "msg", msg)
		return nil
	}

	experimentID, _ := msg["experiment_id"].(string)
	if experimentID == "" {
		experimentID = roundID
	}

	hyperparamsRaw, _ := msg["hyperparams"].(map[string]any)
	var hyperparams *apiextensionsv1.JSON
	if len(hyperparamsRaw) > 0 {
		raw, err := json.Marshal(hyperparamsRaw)
		if err == nil {
			hyperparams = &apiextensionsv1.JSON{Raw: raw}
		}
	}

	federatedJob := &propellerv1.FederatedJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "fl-" + experimentID,
			Namespace: r.namespace,
		},
		Spec: propellerv1.FederatedJobSpec{
			ExperimentID:   experimentID,
			ModelRef:       modelURI,
			TaskWasmImage:  taskWasmImage,
			Participants:   makeParticipantSpecs(participants),
			KOfN:           int(kOfN),
			TimeoutSeconds: int(timeoutS),
			Rounds: propellerv1.RoundConfig{
				Total:    1,
				Strategy: "sequential",
			},
			Aggregator: propellerv1.AggregatorConfig{
				Algorithm: "fedavg",
			},
			Hyperparams: hyperparams,
		},
	}

	if err := r.Create(ctx, federatedJob); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			logger.Error(err, "failed to create FederatedJob from FL round start")
		}
	}

	logger.Info("FederatedJob created from FL round start",
		"experimentID", experimentID,
		"roundID", roundID,
		"participants", len(participants))
	return nil
}

// mqttFLUpdateHandler handles FL update messages from proplets.
func (r *FederatedJobReconciler) mqttFLUpdateHandler(ctx context.Context, msg map[string]any) error {
	return nil
}

func makeParticipantSpecs(ids []string) []propellerv1.ParticipantSpec {
	specs := make([]propellerv1.ParticipantSpec, len(ids))
	for i, id := range ids {
		specs[i] = propellerv1.ParticipantSpec{PropletID: id}
	}
	return specs
}

//nolint:unparam // ctrl.Result is required by the interface signature
func (r *FederatedJobReconciler) handlePending(ctx context.Context, job *propellerv1.FederatedJob) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	if err := r.validateSpec(job); err != nil {
		logger.Error(err, "invalid spec")
		job.Status.Phase = phaseFailed
		r.updateCondition(job, "False", "InvalidSpec", err.Error())
		if err := r.Status().Update(ctx, job); err != nil {
			return statusUpdateError(err)
		}

		return ctrl.Result{}, nil
	}

	roundName := fmt.Sprintf("round-%d-%s", 1, job.Name)
	round := &propellerv1.TrainingRound{
		ObjectMeta: metav1.ObjectMeta{
			Name:      roundName,
			Namespace: job.Namespace,
		},
		Spec: propellerv1.TrainingRoundSpec{
			RoundID: fmt.Sprintf("round-%d", 1),
			FederatedJobRef: corev1.LocalObjectReference{
				Name: job.Name,
			},
			ModelRef:       job.Spec.ModelRef,
			TaskWasmImage:  job.Spec.TaskWasmImage,
			Participants:   r.getParticipantIDs(job.Spec.Participants),
			Hyperparams:    job.Spec.Hyperparams,
			KOfN:           job.Spec.KOfN,
			TimeoutSeconds: job.Spec.TimeoutSeconds,
		},
	}
	// Use SetControllerReference so GVK is resolved from the scheme — job.TypeMeta
	// is empty for objects returned by client.Get and cannot be used directly.
	if err := controllerutil.SetControllerReference(job, round, r.Scheme); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.Create(ctx, round); client.IgnoreAlreadyExists(err) != nil {
		return ctrl.Result{}, fmt.Errorf("failed to create training round: %w", err)
	}

	job.Status.Phase = phaseRunning
	job.Status.CurrentRound = 1
	r.updateCondition(job, "True", "Running", "Job is running")
	if err := r.Status().Update(ctx, job); err != nil {
		return ctrl.Result{RequeueAfter: time.Second}, nil
	}

	return ctrl.Result{}, nil
}

func (r *FederatedJobReconciler) handleRunning(ctx context.Context, job *propellerv1.FederatedJob) (ctrl.Result, error) {
	roundName := fmt.Sprintf("round-%d-%s", job.Status.CurrentRound, job.Name)
	round := &propellerv1.TrainingRound{}
	if err := r.Get(ctx, client.ObjectKey{Name: roundName, Namespace: job.Namespace}, round); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	switch round.Status.Phase {
	case phaseCompleted:
		job.Status.CompletedRounds++
		if round.Status.AggregatedModelRef != "" {
			job.Status.AggregatedModelRef = round.Status.AggregatedModelRef
		}

		if job.Status.CompletedRounds >= job.Spec.Rounds.Total {
			job.Status.Phase = phaseCompleted
			r.updateCondition(job, "True", "Completed", "All rounds completed")
		} else {
			nextRoundNum := job.Status.CurrentRound + 1
			nextRoundName := fmt.Sprintf("round-%d-%s", nextRoundNum, job.Name)
			modelRef := round.Status.AggregatedModelRef
			if modelRef == "" {
				modelRef = job.Spec.ModelRef
			}

			nextRoundAnnotations := make(map[string]string)
			if aggregatedUpdateJSON, ok := round.Annotations["propeller.propeller.absmach.eu/aggregated-update"]; ok {
				nextRoundAnnotations["propeller.propeller.absmach.eu/aggregated-update"] = aggregatedUpdateJSON
			}

			nextRound := &propellerv1.TrainingRound{
				ObjectMeta: metav1.ObjectMeta{
					Name:        nextRoundName,
					Namespace:   job.Namespace,
					Annotations: nextRoundAnnotations,
				},
				Spec: propellerv1.TrainingRoundSpec{
					RoundID: fmt.Sprintf("round-%d", nextRoundNum),
					FederatedJobRef: corev1.LocalObjectReference{
						Name: job.Name,
					},
					ModelRef:       modelRef,
					TaskWasmImage:  job.Spec.TaskWasmImage,
					Participants:   r.getParticipantIDs(job.Spec.Participants),
					Hyperparams:    job.Spec.Hyperparams,
					KOfN:           job.Spec.KOfN,
					TimeoutSeconds: job.Spec.TimeoutSeconds,
				},
			}
			if err := controllerutil.SetControllerReference(job, nextRound, r.Scheme); err != nil {
				return ctrl.Result{}, err
			}
			if err := r.Create(ctx, nextRound); client.IgnoreAlreadyExists(err) != nil {
				return ctrl.Result{}, fmt.Errorf("failed to create next training round: %w", err)
			}

			job.Status.CurrentRound = nextRoundNum
		}

		if err := r.Status().Update(ctx, job); err != nil {
			return statusUpdateError(err)
		}

		return ctrl.Result{RequeueAfter: time.Second * 5}, nil

	case phaseFailed:
		job.Status.Phase = phaseFailed
		r.updateCondition(job, "False", "RoundFailed", "Training round failed")
		if err := r.Status().Update(ctx, job); err != nil {
			return statusUpdateError(err)
		}

		return ctrl.Result{}, nil

	default:
		return ctrl.Result{RequeueAfter: time.Second * 10}, nil
	}
}

func (r *FederatedJobReconciler) validateSpec(job *propellerv1.FederatedJob) error {
	if job.Spec.ExperimentID == "" {
		return errors.New("experimentId is required")
	}
	if job.Spec.ModelRef == "" {
		return errors.New("modelRef is required")
	}
	if job.Spec.TaskWasmImage == "" {
		return errors.New("taskWasmImage is required")
	}
	if len(job.Spec.Participants) == 0 {
		return errors.New("at least one participant is required")
	}
	if job.Spec.KOfN <= 0 {
		return errors.New("kOfN must be greater than 0")
	}
	if job.Spec.KOfN > len(job.Spec.Participants) {
		return errors.New("kOfN cannot be greater than number of participants")
	}

	return nil
}

func (r *FederatedJobReconciler) getParticipantIDs(participants []propellerv1.ParticipantSpec) []string {
	ids := make([]string, len(participants))
	for i, p := range participants {
		ids[i] = p.PropletID
	}

	return ids
}

func (r *FederatedJobReconciler) updateCondition(job *propellerv1.FederatedJob, status, reason, message string) {
	now := time.Now()
	condition := &metav1.Condition{
		Type:               federatedJobConditionReady,
		Status:             metav1.ConditionStatus(status),
		LastTransitionTime: metav1.NewTime(now),
		Reason:             reason,
		Message:            message,
	}

	found := false
	for i, c := range job.Status.Conditions {
		if c.Type == federatedJobConditionReady {
			job.Status.Conditions[i] = *condition
			found = true

			break
		}
	}
	if !found {
		job.Status.Conditions = append(job.Status.Conditions, *condition)
	}
}
