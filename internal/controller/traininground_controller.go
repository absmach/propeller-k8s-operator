package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	propellerapiv1 "github.com/absmach/propeller/api/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

const (
	phasePending   = "Pending"
	phaseRunning   = "Running"
	phaseCompleted = "Completed"
	phaseFailed    = "Failed"
)

type TrainingRoundReconciler struct {
	client.Client

	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=propeller.propeller.absmach.eu,resources=trainingrounds,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=propeller.propeller.absmach.eu,resources=trainingrounds/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=propeller.propeller.absmach.eu,resources=tasks,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=propeller.propeller.absmach.eu,resources=tasks/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=propeller.propeller.absmach.eu,resources=federatedjobs,verbs=get

func (r *TrainingRoundReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	namespace := req.Namespace

	_ = namespace

	round := &propellerapiv1.TrainingRound{}
	if err := r.Get(ctx, req.NamespacedName, round); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	federatedJobName := ""
	if round.Spec.FederatedJobRef.Name != "" {
		federatedJobName = round.Spec.FederatedJobRef.Name
	}
	_ = federatedJobName

	if round.Status.Phase == "" {
		now := metav1.Now()
		round.Status.Phase = phasePending
		round.Status.StartTime = &now
		round.Status.UpdatesRequired = round.Spec.KOfN
		if err := r.Status().Update(ctx, round); err != nil {
			return ctrl.Result{}, err
		}
	}

	var (
		result ctrl.Result
		err    error
	)

	switch round.Status.Phase {
	case phasePending:
		result, err = r.handlePending(ctx, round)
	case phaseRunning:
		result, err = r.handleRunning(ctx, round)
	case "Aggregating":
		result, err = r.handleAggregating(ctx, round)
	case phaseCompleted, phaseFailed:
		return ctrl.Result{}, nil
	default:
		logger.Info("unknown phase", "phase", round.Status.Phase)

		return ctrl.Result{}, nil
	}

	return result, err
}

func (r *TrainingRoundReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&propellerapiv1.TrainingRound{}).
		Owns(&propellerapiv1.Task{}).
		Complete(r)
}

func (r *TrainingRoundReconciler) handlePending(ctx context.Context, round *propellerapiv1.TrainingRound) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	if len(round.Status.Participants) == 0 {
		round.Status.Participants = make([]propellerapiv1.RoundParticipantStatus, len(round.Spec.Participants))
		for i, propletID := range round.Spec.Participants {
			round.Status.Participants[i] = propellerapiv1.RoundParticipantStatus{
				PropletID:      propletID,
				Status:         "Pending",
				UpdateReceived: false,
			}
		}
	}

	for i, participantID := range round.Spec.Participants {
		taskName := fmt.Sprintf("%s-%s", round.Name, participantID)

		existingTask := &propellerapiv1.Task{}
		if err := r.Get(ctx, client.ObjectKey{Name: taskName, Namespace: round.Namespace}, existingTask); err == nil {
			round.Status.Participants[i].TaskRef = &corev1.ObjectReference{
				APIVersion: "propeller.propeller.absmach.eu/v1",
				Kind:       "Task",
				Name:       taskName,
				Namespace:  round.Namespace,
			}

			continue
		}

		env := make(map[string]string)
		env["ROUND_ID"] = round.Spec.RoundID
		env["MODEL_URI"] = round.Spec.ModelRef
		env["PROPLET_ID"] = participantID

		if aggregatedUpdateJSON, ok := round.Annotations["propeller.propeller.absmach.eu/aggregated-update"]; ok {
			var aggEnv UpdateEnvelope
			if err := json.Unmarshal([]byte(aggregatedUpdateJSON), &aggEnv); err == nil {
				env["FL_GLOBAL_VERSION"] = aggEnv.GlobalVersion
				env["FL_GLOBAL_UPDATE_B64"] = aggEnv.UpdateB64
				if aggEnv.Format != "" {
					env["FL_GLOBAL_UPDATE_FORMAT"] = aggEnv.Format
				}
			}
		}

		if round.Spec.FederatedJobRef.Name != "" {
			federatedJob := &propellerapiv1.FederatedJob{}
			if err := r.Get(ctx, client.ObjectKey{Name: round.Spec.FederatedJobRef.Name, Namespace: round.Namespace}, federatedJob); err == nil {
				env["FL_JOB_ID"] = federatedJob.Spec.ExperimentID
			}
		}

		if round.Spec.Hyperparams != nil {
			env["HYPERPARAMS"] = string(round.Spec.Hyperparams.Raw)
		}

		task := &propellerapiv1.Task{
			ObjectMeta: metav1.ObjectMeta{
				Name:      taskName,
				Namespace: round.Namespace,
				Labels: map[string]string{
					"training-round": round.Name,
					"round-id":       round.Spec.RoundID,
					"participant":    participantID,
				},
			},
			Spec: propellerapiv1.TaskSpec{
				Name:     taskName,
				ImageURL: round.Spec.TaskWasmImage,
				Env:      env,
				Mode:     "train",
				Daemon:   false,
				CLIArgs:  []string{},
				PropletSelector: &propellerapiv1.PropletSelector{
					PropletID: participantID,
				},
			},
		}
		// SetControllerReference resolves GVK from scheme; round.TypeMeta is
		// empty for objects returned by client.Get.
		if err := controllerutil.SetControllerReference(round, task, r.Scheme); err != nil {
			return ctrl.Result{}, err
		}
		if err := r.Create(ctx, task); client.IgnoreAlreadyExists(err) != nil {
			logger.Error(err, "failed to create task", "task", taskName)
			return ctrl.Result{}, err
		}

		round.Status.Participants[i].TaskRef = &corev1.ObjectReference{
			APIVersion: "propeller.propeller.absmach.eu/v1",
			Kind:       "Task",
			Name:       taskName,
			Namespace:  round.Namespace,
		}
		round.Status.Participants[i].Status = phaseRunning
	}

	round.Status.Phase = phaseRunning
	r.updateCondition(round, "True", "Running", "Round is running")
	if err := r.Status().Update(ctx, round); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{RequeueAfter: time.Second * 5}, nil
}

func (r *TrainingRoundReconciler) handleRunning(ctx context.Context, round *propellerapiv1.TrainingRound) (ctrl.Result, error) {
	updatesReceived, collectedUpdates := r.processParticipants(ctx, round)
	round.Status.UpdatesReceived = updatesReceived

	if len(collectedUpdates) == 0 {
		return r.updateStatusAndRequeue(ctx, round, time.Second*10)
	}

	r.storeCollectedUpdates(round, collectedUpdates)

	if updatesReceived >= round.Spec.KOfN {
		return r.transitionToAggregating(ctx, round, updatesReceived)
	}

	// If all participants are in a terminal state (including skipped/interrupted)
	// and KOfN is unachievable, fail the round immediately rather than waiting
	// for a timeout that may never arrive.
	allTerminal := true
	for _, p := range round.Status.Participants {
		if p.Status != phaseCompleted && p.Status != phaseFailed {
			allTerminal = false
			break
		}
	}
	if allTerminal && len(round.Status.Participants) > 0 {
		round.Status.Phase = phaseFailed
		r.updateCondition(round, "False", "InsufficientUpdates",
			fmt.Sprintf("All participants terminal but only %d/%d updates collected", updatesReceived, round.Spec.KOfN))
		return ctrl.Result{}, r.Status().Update(ctx, round)
	}

	if timedOut, err := r.checkTimeout(ctx, round, updatesReceived); timedOut {
		return ctrl.Result{}, err
	}

	return r.updateStatusAndRequeue(ctx, round, time.Second*10)
}

//nolint:unparam // ctrl.Result is required by the interface signature
func (r *TrainingRoundReconciler) handleAggregating(ctx context.Context, round *propellerapiv1.TrainingRound) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	var collectedUpdates []UpdateEnvelope
	if updatesJSON, ok := round.Annotations["propeller.propeller.absmach.eu/collected-updates"]; ok {
		if err := json.Unmarshal([]byte(updatesJSON), &collectedUpdates); err != nil {
			logger.Error(err, "failed to unmarshal collected updates")
		}
	}

	algorithm := "fedavg"
	if round.Spec.FederatedJobRef.Name != "" {
		federatedJob := &propellerapiv1.FederatedJob{}
		if err := r.Get(ctx, client.ObjectKey{Name: round.Spec.FederatedJobRef.Name, Namespace: round.Namespace}, federatedJob); err == nil {
			if federatedJob.Spec.Aggregator.Algorithm != "" {
				algorithm = federatedJob.Spec.Aggregator.Algorithm
			}
		}
	}

	aggregatedModelRef := r.aggregateUpdates(ctx, round, collectedUpdates, algorithm)
	if aggregatedModelRef == "" {
		return ctrl.Result{}, nil
	}

	now := metav1.Now()
	round.Status.EndTime = &now
	round.Status.Phase = phaseCompleted
	round.Status.AggregatedModelRef = aggregatedModelRef

	r.updateCondition(round, "True", "Completed", fmt.Sprintf("Round completed and aggregated %d updates", len(collectedUpdates)))

	if err := r.Status().Update(ctx, round); err != nil {
		return ctrl.Result{}, err
	}

	logger.Info("round completed", "round", round.Name, "aggregatedModel", round.Status.AggregatedModelRef, "updates", len(collectedUpdates))

	return ctrl.Result{}, nil
}

func (r *TrainingRoundReconciler) processParticipants(ctx context.Context, round *propellerapiv1.TrainingRound) (int, []UpdateEnvelope) {
	logger := log.FromContext(ctx)
	updatesReceived := 0
	collectedUpdates := make([]UpdateEnvelope, 0)

	for i, participant := range round.Status.Participants {
		if participant.TaskRef == nil {
			continue
		}

		task := &propellerapiv1.Task{}
		if err := r.Get(ctx, client.ObjectKey{
			Name:      participant.TaskRef.Name,
			Namespace: participant.TaskRef.Namespace,
		}, task); err != nil {
			logger.Error(err, "failed to get task", "task", participant.TaskRef.Name)

			continue
		}

		switch task.Status.Phase {
		case propellerapiv1.TaskCompletedPhase:
			if participant.UpdateReceived {
				break
			}
			updatesReceived++
			round.Status.Participants[i].UpdateReceived = true
			round.Status.Participants[i].Status = phaseCompleted

			env, ok := processCompletedParticipant(&round.Status.Participants[i], task, round)
			if ok {
				collectedUpdates = append(collectedUpdates, env)
				logger.Info("collected FL update", "proplet", participant.PropletID, "round", round.Spec.RoundID)
			} else {
				r.logMissingUpdate(ctx, task)
			}
		case propellerapiv1.TaskFailedPhase:
			round.Status.Participants[i].Status = phaseFailed
			logger.Info("participant task failed", "participant", participant.PropletID, "task", participant.TaskRef.Name)
		case propellerapiv1.TaskSkippedPhase, propellerapiv1.TaskInterruptedPhase:
			// Skipped/interrupted tasks will never deliver an update; mark them
			// so the round does not wait for them indefinitely.
			round.Status.Participants[i].Status = phaseFailed
			logger.Info("participant task skipped or interrupted", "participant", participant.PropletID, "task", participant.TaskRef.Name, "phase", task.Status.Phase)
		case propellerapiv1.TaskRunningPhase, propellerapiv1.TaskPendingPhase:
		}
	}

	return updatesReceived, collectedUpdates
}

func (r *TrainingRoundReconciler) logMissingUpdate(ctx context.Context, task *propellerapiv1.Task) {
	logger := log.FromContext(ctx)
	if task.Status.Results == nil {
		logger.Info("task completed but no results found", "task", task.Name)
	} else {
		logger.Info("task completed but no FL update found", "task", task.Name)
	}
}

func (r *TrainingRoundReconciler) storeCollectedUpdates(round *propellerapiv1.TrainingRound, collectedUpdates []UpdateEnvelope) {
	updatesJSON, err := json.Marshal(collectedUpdates)
	if err == nil {
		if round.Annotations == nil {
			round.Annotations = make(map[string]string)
		}
		round.Annotations["propeller.propeller.absmach.eu/collected-updates"] = string(updatesJSON)
	}
}

func (r *TrainingRoundReconciler) transitionToAggregating(ctx context.Context, round *propellerapiv1.TrainingRound, updatesReceived int) (ctrl.Result, error) {
	round.Status.Phase = "Aggregating"
	r.updateCondition(round, "True", "Aggregating", fmt.Sprintf("Collected %d updates, starting aggregation", updatesReceived))
	if err := r.Status().Update(ctx, round); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{RequeueAfter: time.Second * 2}, nil
}

// checkTimeout returns (timedOut, err). When timedOut is true the caller must
// return immediately; err holds any Status().Update failure so it is not
// silently discarded.
func (r *TrainingRoundReconciler) checkTimeout(ctx context.Context, round *propellerapiv1.TrainingRound, updatesReceived int) (bool, error) {
	if round.Spec.TimeoutSeconds <= 0 || round.Status.StartTime == nil {
		return false, nil
	}
	elapsed := time.Since(round.Status.StartTime.Time)
	if elapsed <= time.Duration(round.Spec.TimeoutSeconds)*time.Second {
		return false, nil
	}
	round.Status.Phase = phaseFailed
	r.updateCondition(round, "False", "Timeout", fmt.Sprintf("Round timed out: only %d/%d updates received", updatesReceived, round.Spec.KOfN))
	return true, r.Status().Update(ctx, round)
}

func (r *TrainingRoundReconciler) aggregateUpdates(ctx context.Context, round *propellerapiv1.TrainingRound, collectedUpdates []UpdateEnvelope, algorithm string) string {
	startTime := time.Now()
	logger := log.FromContext(ctx)

	if len(collectedUpdates) == 0 {
		aggregatedModelRef := fmt.Sprintf("oci://registry/model:%s-fallback", round.Spec.RoundID)
		logger.Info("no updates collected, using fallback model reference")

		return aggregatedModelRef
	}

	aggEnv, err := AggregateFLUpdates(collectedUpdates, algorithm)
	if err != nil {
		logger.Error(err, "failed to aggregate updates")
		round.Status.Phase = phaseFailed
		r.updateCondition(round, "False", "AggregationFailed", fmt.Sprintf("Failed to aggregate: %v", err))
		if err := r.Status().Update(ctx, round); err != nil {
			return ""
		}

		return ""
	}

	r.storeAggregatedUpdate(round, aggEnv)
	aggregatedModelRef := fmt.Sprintf("oci://registry/model:%s-aggregated", round.Spec.RoundID)
	logger.Info("aggregation completed", "updates", len(collectedUpdates), "algorithm", algorithm)

	_ = time.Since(startTime)

	return aggregatedModelRef
}

func (r *TrainingRoundReconciler) storeAggregatedUpdate(round *propellerapiv1.TrainingRound, aggEnv UpdateEnvelope) {
	aggJSON, err := json.Marshal(aggEnv)
	if err == nil {
		if round.Annotations == nil {
			round.Annotations = make(map[string]string)
		}
		round.Annotations["propeller.propeller.absmach.eu/aggregated-update"] = string(aggJSON)
	}
}

func (r *TrainingRoundReconciler) updateStatusAndRequeue(ctx context.Context, round *propellerapiv1.TrainingRound, requeueAfter time.Duration) (ctrl.Result, error) {
	if err := r.Status().Update(ctx, round); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{RequeueAfter: requeueAfter}, nil
}

func (r *TrainingRoundReconciler) updateCondition(round *propellerapiv1.TrainingRound, status, reason, message string) {
	now := metav1.Now()
	condition := metav1.Condition{
		Type:               "Ready",
		Status:             metav1.ConditionStatus(status),
		LastTransitionTime: now,
		Reason:             reason,
		Message:            message,
	}

	found := false
	for i, c := range round.Status.Conditions {
		if c.Type == "Ready" {
			round.Status.Conditions[i] = condition
			found = true

			break
		}
	}
	if !found {
		round.Status.Conditions = append(round.Status.Conditions, condition)
	}
}
