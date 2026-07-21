package scheduler

import (
	"sort"

	propellerv1 "github.com/absmach/propeller/api/v1"
)

const defaultPriority = 50

type priorityScheduler struct {
	base Scheduler
}

func NewPriority() Scheduler {
	return &priorityScheduler{
		base: NewRoundRobin(),
	}
}

func (p *priorityScheduler) SelectProplet(t propellerv1.Task, proplets []propellerv1.Proplet) (propellerv1.Proplet, error) {
	candidates := p.SelectCandidateProplets(t, proplets)
	if len(candidates) == 0 {
		return propellerv1.Proplet{}, ErrNoCandidates
	}

	scores := p.Score(t, candidates)
	return p.Pick(scores, candidates), nil
}

func (p *priorityScheduler) SelectCandidateProplets(t propellerv1.Task, proplets []propellerv1.Proplet) []propellerv1.Proplet {
	return p.base.SelectCandidateProplets(t, proplets)
}

func (p *priorityScheduler) Score(t propellerv1.Task, proplets []propellerv1.Proplet) map[string]float64 {
	scores := make(map[string]float64)

	prio := t.Spec.Priority
	if prio == 0 {
		prio = defaultPriority
	}

	// Higher priority = lower score (more likely to be picked)
	for _, pr := range proplets {
		scores[pr.Name] = 1.0 - (float64(prio) / 100.0)
	}

	return scores
}

func (p *priorityScheduler) Pick(scores map[string]float64, candidates []propellerv1.Proplet) propellerv1.Proplet {
	if len(candidates) == 0 {
		return propellerv1.Proplet{}
	}

	sorted := make([]propellerv1.Proplet, len(candidates))
	copy(sorted, candidates)

	sort.Slice(sorted, func(i, j int) bool {
		si := scores[sorted[i].Name]
		sj := scores[sorted[j].Name]
		if si != sj {
			return si < sj
		}
		return sorted[i].Name < sorted[j].Name
	})

	return sorted[0]
}
