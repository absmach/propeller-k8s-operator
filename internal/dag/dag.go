/*
Copyright 2025.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package dag provides helpers for evaluating directed-acyclic-graph (DAG)
// task dependencies used by both the Task and PropellerJob controllers.
package dag

import (
	"errors"
	"fmt"
)

const (
	RunIfSuccess = "success"
	RunIfFailure = "failure"
)

var (
	ErrCircularDependency = errors.New("circular dependency detected")
	ErrMissingDependency  = errors.New("dependency task not found")
)

// AllDepsTerminal returns true when every dependency name in dependsOn is
// present in either completed or failed.  An empty dependsOn list returns true.
func AllDepsTerminal(dependsOn []string, completed, failed map[string]bool) bool {
	for _, dep := range dependsOn {
		if !completed[dep] && !failed[dep] {
			return false
		}
	}
	return true
}

// ShouldSkip returns true when all deps are terminal and the RunIf condition
// is not satisfied — meaning the task must transition to Skipped instead of
// being scheduled.  Returns false (never skip) when deps are not yet terminal.
//
// runIf == "" or "success" → skip when any dep failed.
// runIf == "failure"       → skip when no dep failed.
func ShouldSkip(dependsOn []string, runIf string, completed, failed map[string]bool) bool {
	if len(dependsOn) == 0 {
		return false
	}
	if !AllDepsTerminal(dependsOn, completed, failed) {
		return false
	}

	anyFailed := false
	for _, dep := range dependsOn {
		if failed[dep] {
			anyFailed = true
			break
		}
	}

	switch runIf {
	case RunIfFailure:
		return !anyFailed
	default: // "success" or empty
		return anyFailed
	}
}

// ValidateDAG checks a set of tasks for circular dependencies and missing
// dependency references.  tasks is a map from task name to its dependency list.
// Returns ErrCircularDependency if a cycle is found.
func ValidateDAG(tasks map[string][]string) error {
	for name, deps := range tasks {
		for _, dep := range deps {
			if dep == name {
				return fmt.Errorf("%w: task %s depends on itself", ErrCircularDependency, name)
			}
		}
	}

	visited := make(map[string]bool)
	recStack := make(map[string]bool)

	var dfs func(name string) error
	dfs = func(name string) error {
		visited[name] = true
		recStack[name] = true

		for _, dep := range tasks[name] {
			if _, exists := tasks[dep]; !exists {
				return fmt.Errorf("%w: dependency %q of task %q does not exist", ErrMissingDependency, dep, name)
			}
			if !visited[dep] {
				if err := dfs(dep); err != nil {
					return err
				}
			} else if recStack[dep] {
				return fmt.Errorf("%w: cycle detected involving tasks %q and %q", ErrCircularDependency, name, dep)
			}
		}

		recStack[name] = false
		return nil
	}

	for name := range tasks {
		if !visited[name] {
			if err := dfs(name); err != nil {
				return err
			}
		}
	}
	return nil
}

// TopologicalSort returns the task names in dependency order (dependencies
// before dependents).  tasks is a map from task name to its dependency list.
// Returns ErrCircularDependency if a cycle prevents a valid ordering.
func TopologicalSort(tasks map[string][]string) ([]string, error) {
	if err := ValidateDAG(tasks); err != nil {
		return nil, err
	}

	inDegree := make(map[string]int)
	for name, deps := range tasks {
		inDegree[name] = len(deps)
	}

	queue := make([]string, 0)
	for name, degree := range inDegree {
		if degree == 0 {
			queue = append(queue, name)
		}
	}

	result := make([]string, 0, len(tasks))
	visited := make(map[string]bool)

	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		if visited[current] {
			continue
		}
		visited[current] = true
		result = append(result, current)

		for name, deps := range tasks {
			if visited[name] {
				continue
			}
			for _, dep := range deps {
				if dep == current {
					inDegree[name]--
					break
				}
			}
			if inDegree[name] == 0 && !visited[name] {
				queue = append(queue, name)
			}
		}
	}

	if len(result) != len(tasks) {
		return nil, errors.New("topological sort failed: not all tasks processed")
	}

	return result, nil
}
