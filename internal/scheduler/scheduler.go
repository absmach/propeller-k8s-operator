package scheduler

import (
	"errors"

	propellerv1 "github.com/absmach/propeller/api/v1"
)

var (
	ErrNoProplet    = errors.New("no proplet was provided")
	ErrDeadProplers = errors.New("all proplets are dead")
)

type Scheduler interface {
	SelectProplet(t propellerv1.Task, proplets []propellerv1.Proplet) (propellerv1.Proplet, error)
}
