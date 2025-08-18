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

func (r *roundRobin) SelectProplet(t propellerv1.Task, proplets []propellerv1.Proplet) (propellerv1.Proplet, error) {
	if len(proplets) == 0 {
		return propellerv1.Proplet{}, ErrNoProplet
	}

	alive := 0
	for i := range proplets {
		if proplets[i].Status.Phase == propellerv1.PropletRunningPhase {
			alive += 1
		}
	}
	if alive == 0 {
		return propellerv1.Proplet{}, ErrDeadProplers
	}

	if len(proplets) == 1 {
		return proplets[0], nil
	}

	r.LastProplet = (r.LastProplet + 1) % len(proplets)

	p := proplets[r.LastProplet]
	if !(p.Status.Phase == propellerv1.PropletRunningPhase) {
		return r.SelectProplet(t, proplets)
	}
	p.Status.TaskCount += 1

	return p, nil
}
