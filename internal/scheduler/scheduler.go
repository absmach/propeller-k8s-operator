package scheduler

import (
	"errors"

	propellerv1 "github.com/absmach/propeller/api/v1"
)

var (
	ErrNoProplet          = errors.New("no proplet was provided")
	ErrDeadProplers       = errors.New("all proplets are dead")
	ErrNoCandidates       = errors.New("no candidate proplets match requirements")
	ErrInsufficientResources = errors.New("no proplet has sufficient resources")
)

// Scheduler defines the interface for task scheduling with a 3-phase approach:
// 1. SelectCandidateProplets - filters proplets based on task requirements
// 2. Score - assigns scores to candidate proplets
// 3. Pick - selects the best proplet based on scores
type Scheduler interface {
	// SelectProplet is the main entry point that orchestrates the 3-phase scheduling
	SelectProplet(t propellerv1.Task, proplets []propellerv1.Proplet) (propellerv1.Proplet, error)

	// SelectCandidateProplets filters proplets based on task requirements (phase 1)
	// Returns only proplets that can potentially run the task
	SelectCandidateProplets(t propellerv1.Task, proplets []propellerv1.Proplet) []propellerv1.Proplet

	// Score assigns a score to each candidate proplet (phase 2)
	// Lower scores are better (like cost-based scheduling)
	Score(t propellerv1.Task, proplets []propellerv1.Proplet) map[string]float64

	// Pick selects the best proplet from scored candidates (phase 3)
	Pick(scores map[string]float64, candidates []propellerv1.Proplet) propellerv1.Proplet
}
