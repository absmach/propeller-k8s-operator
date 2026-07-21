package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var ErrNoResult = errors.New("no result found")

func ExtractResultFromJob(ctx context.Context, c client.Client, job *batchv1.Job) (map[string]any, error) {
	startTime := time.Now()
	_ = startTime

	if resultJSON, ok := job.Annotations["propeller.propeller.absmach.eu/result"]; ok {
		var result map[string]any
		if err := json.Unmarshal([]byte(resultJSON), &result); err == nil {
			return result, nil
		}
	}

	podList := &corev1.PodList{}
	labels := job.Spec.Selector.MatchLabels
	if err := c.List(ctx, podList, client.MatchingLabels(labels), client.InNamespace(job.Namespace)); err != nil {
		return nil, fmt.Errorf("failed to list pods: %w", err)
	}

	var succeededPod *corev1.Pod
	for i := range podList.Items {
		pod := &podList.Items[i]
		if pod.Status.Phase == corev1.PodSucceeded {
			succeededPod = &podList.Items[i]

			break
		}
	}

	if succeededPod == nil {
		return nil, fmt.Errorf("%w: no succeeded pod found", ErrNoResult)
	}

	if len(succeededPod.Status.ContainerStatuses) > 0 {
		cs := succeededPod.Status.ContainerStatuses[0]
		if cs.State.Terminated != nil && cs.State.Terminated.Message != "" {
			var result map[string]any
			if err := json.Unmarshal([]byte(cs.State.Terminated.Message), &result); err == nil {
				return result, nil
			}
		}
	}

	configMapName := job.Name + "-result"
	configMap := &corev1.ConfigMap{}
	if err := c.Get(ctx, types.NamespacedName{Name: configMapName, Namespace: job.Namespace}, configMap); err == nil {
		if resultJSON, ok := configMap.Data["result"]; ok {
			var result map[string]any
			if err := json.Unmarshal([]byte(resultJSON), &result); err == nil {
				return result, nil
			}
		}
	}

	secretName := job.Name + "-result"
	secret := &corev1.Secret{}
	if err := c.Get(ctx, types.NamespacedName{Name: secretName, Namespace: job.Namespace}, secret); err == nil {
		if resultData, ok := secret.Data["result"]; ok {
			var result map[string]any
			if err := json.Unmarshal(resultData, &result); err == nil {
				return result, nil
			}
		}
	}

	if resultJSON, ok := succeededPod.Annotations["propeller.propeller.absmach.eu/result"]; ok {
		var result map[string]any
		if err := json.Unmarshal([]byte(resultJSON), &result); err == nil {
			return result, nil
		}
	}

	return nil, fmt.Errorf("%w: tried all extraction methods", ErrNoResult)
}

func ExtractFLUpdateFromResult(result map[string]any) (UpdateEnvelope, error) {
	if env, ok := result["update_envelope"].(map[string]any); ok {
		return parseUpdateEnvelope(env)
	}

	if results, ok := result["results"].(map[string]any); ok {
		if env, ok := results["update_envelope"].(map[string]any); ok {
			return parseUpdateEnvelope(env)
		}
	}

	return parseUpdateEnvelope(result)
}

func parseUpdateEnvelope(data map[string]any) (UpdateEnvelope, error) {
	var env UpdateEnvelope

	switch v := data["round_id"].(type) {
	case string:
		var roundIDNum uint64
		if _, err := fmt.Sscanf(v, "%d", &roundIDNum); err == nil {
			env.RoundID = roundIDNum
		}
	case float64:
		env.RoundID = uint64(v)
	case uint64:
		env.RoundID = v
	}

	if jobID, ok := data["job_id"].(string); ok {
		env.JobID = jobID
	}
	if propletID, ok := data["proplet_id"].(string); ok {
		env.PropletID = propletID
	}
	if globalVersion, ok := data["global_version"].(string); ok {
		env.GlobalVersion = globalVersion
	}
	if updateB64, ok := data["update_b64"].(string); ok {
		env.UpdateB64 = updateB64
	}
	if format, ok := data["format"].(string); ok {
		env.Format = format
	}

	switch v := data["num_samples"].(type) {
	case float64:
		env.NumSamples = uint64(v)
	case uint64:
		env.NumSamples = v
	}

	if metricsData, ok := data["metrics"].(map[string]any); ok {
		env.Metrics = metricsData
	}

	if env.JobID == "" || env.RoundID == 0 || env.PropletID == "" {
		return UpdateEnvelope{}, errors.New("missing required fields in update envelope")
	}

	return env, nil
}
