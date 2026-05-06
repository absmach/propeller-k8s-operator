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

const (
	RunIfSuccess = "success"
	RunIfFailure = "failure"
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
