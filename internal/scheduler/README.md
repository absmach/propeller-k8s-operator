# Scheduler Package

## Status: Unused

This package (`internal/scheduler/`) contains a round-robin scheduler implementation for selecting Proplets to execute Tasks.

**Current Status**: This scheduler is **not currently used** by any controllers in the codebase.

### Implementation

- `scheduler.go`: Defines the `Scheduler` interface with `SelectProplet` method
- `roundrobin.go`: Implements round-robin scheduling algorithm

### Decision

The scheduler package is kept in the codebase for potential future use but is not actively used. Controllers currently use direct proplet selection via `propletSelector.propletId` in Task specs.

### Future Use

If proplet selection logic needs to be centralized or made configurable, this scheduler can be integrated into the Task controller.
