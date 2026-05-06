package controller

import (
	"encoding/json"
	"fmt"

	propellerapiv1 "github.com/absmach/propeller/api/v1"
)

type UpdateEnvelope struct {
	TaskID        string         `json:"task_id,omitempty"`
	JobID         string         `json:"job_id"`
	RoundID       uint64         `json:"round_id"`
	GlobalVersion string         `json:"global_version"`
	PropletID     string         `json:"proplet_id"`
	NumSamples    uint64         `json:"num_samples"`
	UpdateB64     string         `json:"update_b64"`
	Metrics       map[string]any `json:"metrics,omitempty"`
	Format        string         `json:"format,omitempty"`
}

func extractUpdateFromTask(task *propellerapiv1.Task, roundIDStr string) (UpdateEnvelope, bool) {
	if task.Status.Results == nil {
		return UpdateEnvelope{}, false
	}

	var resultData map[string]any
	if err := json.Unmarshal(task.Status.Results.Raw, &resultData); err != nil {
		return UpdateEnvelope{}, false
	}

	env, err := ExtractFLUpdateFromResult(resultData)
	if err != nil {
		return UpdateEnvelope{}, false
	}

	if env.RoundID == 0 {
		return env, true
	}

	var roundIDNum uint64
	if _, err := fmt.Sscanf(roundIDStr, "round-%d", &roundIDNum); err != nil {
		if _, err2 := fmt.Sscanf(roundIDStr, "%d", &roundIDNum); err2 != nil {
			return env, true
		}
	}

	if env.RoundID == roundIDNum || roundIDNum == 0 {
		return env, true
	}

	return UpdateEnvelope{}, false
}

func processCompletedParticipant(
	participant *propellerapiv1.RoundParticipantStatus,
	task *propellerapiv1.Task,
	round *propellerapiv1.TrainingRound,
) (UpdateEnvelope, bool) {
	if participant.UpdateReceived {
		return UpdateEnvelope{}, false
	}

	env, ok := extractUpdateFromTask(task, round.Spec.RoundID)
	if !ok {
		return UpdateEnvelope{}, false
	}

	return env, true
}
