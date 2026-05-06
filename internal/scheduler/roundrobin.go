package scheduler

import propellerv1 "github.com/absmach/propeller/api/v1"

type roundRobin struct {
	LastProplet int
}

func NewRoundRobin() Scheduler {
	return &roundRobin{
		LastProplet: 0,
	}
}

// SelectProplet orchestrates the 3-phase scheduling process
func (r *roundRobin) SelectProplet(t propellerv1.Task, proplets []propellerv1.Proplet) (propellerv1.Proplet, error) {
	if len(proplets) == 0 {
		return propellerv1.Proplet{}, ErrNoProplet
	}

	// Phase 1: Filter candidates
	candidates := r.SelectCandidateProplets(t, proplets)
	if len(candidates) == 0 {
		return propellerv1.Proplet{}, ErrNoCandidates
	}

	// Phase 2: Score candidates
	scores := r.Score(t, candidates)

	// Phase 3: Pick best candidate
	selected := r.Pick(scores, candidates)

	return selected, nil
}

// SelectCandidateProplets filters proplets that are alive and match task requirements
func (r *roundRobin) SelectCandidateProplets(t propellerv1.Task, proplets []propellerv1.Proplet) []propellerv1.Proplet {
	candidates := make([]propellerv1.Proplet, 0, len(proplets))

	for i := range proplets {
		p := proplets[i]

		// Must be running
		if p.Status.Phase != propellerv1.PropletRunningPhase {
			continue
		}

		// Check proplet type preference
		if t.Spec.PreferredPropletType != "" && t.Spec.PreferredPropletType != propellerv1.AnyProplet {
			if p.Spec.Type != t.Spec.PreferredPropletType {
				continue
			}
		}

		// Check proplet selector if specified
		if t.Spec.PropletSelector != nil {
			if t.Spec.PropletSelector.PropletID != "" && p.Name != t.Spec.PropletSelector.PropletID {
				continue
			}

			// Check label matching
			if len(t.Spec.PropletSelector.MatchLabels) > 0 {
				if !matchLabels(p.Labels, t.Spec.PropletSelector.MatchLabels) {
					continue
				}
			}

			// Check device type matching (for external proplets)
			if len(t.Spec.PropletSelector.MatchDeviceTypes) > 0 && p.Spec.External != nil {
				if !containsAny(t.Spec.PropletSelector.MatchDeviceTypes, p.Spec.External.DeviceType) {
					continue
				}
			}

			// Check capability matching
			if len(t.Spec.PropletSelector.MatchCapabilities) > 0 && p.Spec.External != nil {
				if !containsAll(p.Spec.External.Capabilities, t.Spec.PropletSelector.MatchCapabilities) {
					continue
				}
			}
		}

		candidates = append(candidates, p)
	}

	return candidates
}

// Score assigns scores to proplets using round-robin logic
// Next proplet in rotation gets lowest score (0.1), others get 1.0
func (r *roundRobin) Score(t propellerv1.Task, proplets []propellerv1.Proplet) map[string]float64 {
	scores := make(map[string]float64)

	if len(proplets) == 0 {
		return scores
	}

	// Determine next worker in round-robin rotation
	newProplet := (r.LastProplet + 1) % len(proplets)
	r.LastProplet = newProplet

	for idx, p := range proplets {
		if idx == newProplet {
			scores[p.Name] = 0.1 // Lowest score = highest priority
		} else {
			scores[p.Name] = 1.0
		}
	}

	return scores
}

// Pick selects the proplet with the lowest score
func (r *roundRobin) Pick(scores map[string]float64, candidates []propellerv1.Proplet) propellerv1.Proplet {
	if len(candidates) == 0 {
		return propellerv1.Proplet{}
	}

	var bestProplet propellerv1.Proplet
	lowestScore := -1.0

	for _, p := range candidates {
		score, ok := scores[p.Name]
		if !ok {
			continue
		}

		if lowestScore < 0 || score < lowestScore {
			lowestScore = score
			bestProplet = p
		}
	}

	return bestProplet
}

// matchLabels checks if proplet labels contain all required labels
func matchLabels(propletLabels, requiredLabels map[string]string) bool {
	for k, v := range requiredLabels {
		if propletLabels[k] != v {
			return false
		}
	}
	return true
}

// containsAny checks if the value matches any of the allowed values
func containsAny(allowed []string, value string) bool {
	for _, a := range allowed {
		if a == value {
			return true
		}
	}
	return false
}

// containsAll checks if proplet capabilities include all required capabilities
func containsAll(have, need []string) bool {
	haveSet := make(map[string]bool)
	for _, h := range have {
		haveSet[h] = true
	}
	for _, n := range need {
		if !haveSet[n] {
			return false
		}
	}
	return true
}
