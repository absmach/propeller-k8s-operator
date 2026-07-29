package controller

import (
	"errors"
	"fmt"
)

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
