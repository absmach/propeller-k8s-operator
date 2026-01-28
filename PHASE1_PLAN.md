# PHASE 1: PLAN - Kubebuilder Re-scaffold & Re-implementation

## Executive Summary

This plan documents a **complete re-scaffold** of the operator using Kubebuilder as the build system and scaffolding source of truth. This is **not** an incremental cleanup—it is a full rebuild from Kubebuilder scaffolding with existing behavior ported exactly.

**Key Principle**: Zero behavior changes unless explicitly documented and justified. All changes must be validated via byte-for-byte CRD diffs and golden fixture comparisons.

---

## Current Operator Summary

### Architecture & Flow

**Entrypoint**: `cmd/main.go`
- Uses `controller-runtime` Manager
- Initializes 4 controllers: PropletReconciler, TaskReconciler, FederatedJobReconciler, TrainingRoundReconciler
- Optional MQTT integration for external proplet communication
- Health/ready probes, metrics server, leader election configured
- Certificate watchers for metrics and webhook TLS

**Controller Loop**:
- All controllers use standard `Reconcile(ctx, req)` pattern
- Controllers watch their primary resources via `For()` builder
- Some controllers watch owned resources via `Owns()` builder
- No custom predicates or event filters currently used

**Current Watches** (exact code analysis):
- **PropletReconciler**: `For(&Proplet{})` only. **No `Owns()` watch for Deployment**. Deployment is managed manually via Get/Create/Update.
- **TaskReconciler**: `For(&Task{})`, `Owns(&Job{})`
- **FederatedJobReconciler**: `For(&FederatedJob{})`, `Owns(&TrainingRound{})`
- **TrainingRoundReconciler**: `For(&TrainingRound{})`, `Owns(&Task{})`

**Client Usage**:
- Standard `client.Client` from controller-runtime
- Direct Get/List/Create/Update/Delete operations
- Status updates via `Status().Update()`
- No custom client wrappers or caching layers

**Leader Election**:
- Configurable via `--leader-elect` flag (default: false)
- LeaderElectionID: `"fa27fa49.propeller.abstractmachines.fr"`
- LeaderElectionReleaseOnCancel: **NOT set** (commented out in code)

**Metrics/Health Endpoints**:
- Metrics: configurable address (default: "0" = disabled), supports HTTPS with cert watcher
- Health probe: `:8081` (configurable via `--health-probe-bind-address`)
- Ready probe: `:8081` (configurable via `--health-probe-bind-address`)
- Metrics secure: default `true` (HTTPS)

**Logging**:
- Uses `zap` logger via controller-runtime
- Development mode enabled by default
- Structured logging with context values

**Config/Env Flags**:
- `--metrics-bind-address`: metrics endpoint (default: "0")
- `--health-probe-bind-address`: health/ready probes (default: ":8081")
- `--leader-elect`: enable leader election (default: false)
- `--metrics-secure`: HTTPS for metrics (default: true)
- `--webhook-cert-path`, `--webhook-cert-name`, `--webhook-cert-key`: webhook TLS
- `--metrics-cert-path`, `--metrics-cert-name`, `--metrics-cert-key`: metrics TLS
- `--enable-http2`: enable HTTP/2 (default: false)
- `--liveliness-interval`: proplet liveliness check interval (default: 10s)
- `--last-seen-threshold`: external proplet offline threshold (default: 30s)
- `--mqtt-address`, `--mqtt-qos`, `--mqtt-timeout`: MQTT broker config
- `--domain-id`, `--channel-id`, `--client-id`, `--client-key`: MQTT auth

**Namespaces Watched**:
- All controllers are namespace-scoped (no cluster-scoped resources)
- No explicit namespace filtering in controllers
- Cache scope: **all namespaces** (default controller-runtime behavior)

**Reconciliation Triggers**:
- Primary resource changes (Create/Update/Delete)
- Owned resource changes (Job status changes trigger Task reconciliation, TrainingRound changes trigger FederatedJob reconciliation)
- Periodic requeue based on status (liveliness checks, timeout checks)
- MQTT messages (external proplet liveness, task results) - **these do NOT trigger reconcile directly**, they update status via background handlers

**Manager Configuration** (current):
- No explicit `MaxConcurrentReconciles` set (uses default: 1 per controller)
- No explicit rate limiter configuration (uses default exponential backoff)
- No explicit cache namespace filtering
- Scheme includes: clientgoscheme, propellerv1, propellerv1alpha1

### Custom Resources (CRDs)

#### Group: `propeller.propeller.abstractmachines.fr/v1`

**1. Proplet** (`api/v1/proplet_types.go`)
- **Kind**: `Proplet`
- **Scope**: Namespaced
- **Spec Fields**: (see full spec in code)
- **Status Fields**: (see full spec in code)
- **Defaulting**: None (all defaults via kubebuilder markers)
- **Validation**: OpenAPI v3 schema validation via kubebuilder markers
- **Webhooks**: None currently
- **Finalizer**: `"propeller.propeller.abstractmachines.fr/finalizer"`
  - **Behavior**: Added to ALL proplets (both k8s and external) on first reconcile
  - **Removal**: Only removed after cleanup completes (for k8s: after Deployment deleted; for external: immediately, no cleanup needed)

#### Group: `propeller.absmach.io/v1alpha1`

**2. FederatedJob** (`api/v1alpha1/federatedjob_types.go`)
- **Kind**: `FederatedJob`
- **Scope**: Namespaced
- **Finalizer**: None

**3. TrainingRound** (`api/v1alpha1/traininground_types.go`)
- **Kind**: `TrainingRound`
- **Scope**: Namespaced
- **Finalizer**: None

**4. Task** (`api/v1/task_types.go`)
- **Kind**: `Task`
- **Scope**: Namespaced
- **Finalizer**: None (but RBAC exists for it)

### Secondary Resources Managed

#### PropletReconciler
- **Deployments** (apps/v1)
  - Name: `{proplet-name}-proplet`
  - Ownership: Controller reference set (garbage collected on Proplet delete)
  - **Watch behavior**: Currently **NOT watched** via `Owns()`. Deployment changes do NOT trigger Proplet reconciliation. Only Proplet changes trigger reconciliation, which then manually checks Deployment status.

#### TaskReconciler
- **Jobs** (batch/v1)
  - Name: `{task-name}-job`
  - Ownership: Controller reference set
  - **Watch behavior**: Watched via `Owns()`. Job status changes trigger Task reconciliation.
- **ConfigMaps** (core/v1)
  - Name: `{task-name}-config`
  - Ownership: Controller reference set
  - **Watch behavior**: NOT watched. Only created/updated on Task reconciliation.

#### FederatedJobReconciler
- **TrainingRounds** (propeller.absmach.io/v1alpha1)
  - Name: `round-{round-number}-{federatedjob-name}`
  - Ownership: Controller reference set
  - **Watch behavior**: Watched via `Owns()`. TrainingRound changes trigger FederatedJob reconciliation.

#### TrainingRoundReconciler
- **Tasks** (propeller.propeller.abstractmachines.fr/v1)
  - Name: `{traininground-name}-{participant-id}`
  - Ownership: Controller reference set
  - **Watch behavior**: Watched via `Owns()`. Task changes trigger TrainingRound reconciliation.

### Idempotency Strategy

**PropletReconciler**:
- K8s Proplet: Manual comparison via `deploymentNeedsUpdate()` checks: replicas, image, env, resources, labels. Updates only if differences detected.
- External Proplet: Status-only reconciliation (no resource creation).

**TaskReconciler**:
- Creates Job and ConfigMap if not exist (idempotent via Create with IgnoreAlreadyExists).
- No update logic for Job/ConfigMap (recreate if needed).

**FederatedJobReconciler**:
- Creates TrainingRound for current round if not exist.
- Creates next TrainingRound when current completes.

**TrainingRoundReconciler**:
- Creates Task for each participant if not exist.
- Updates status based on Task status.

**Drift Correction**:
- PropletReconciler: Manual comparison function `deploymentNeedsUpdate()`.
- Other controllers: No drift correction (rely on ownership/garbage collection).

**Delete Handling**:
- PropletReconciler: Finalizer ensures Deployment deletion before Proplet deletion (for k8s type only; external has no cleanup).
- Other controllers: Rely on owner references for cascade deletion.

---

## Behavior Contract

### Proplet Behavior

**K8s Proplet**:
1. On Create/Update: Create or update Deployment named `{name}-proplet` with controller reference.
2. Deployment spec derived from Proplet spec (image, replicas, env, resources).
3. Status updated from Deployment status (ready replicas, available replicas, phase, conditions).
4. Phase transitions: Initializing → Running (when replicas ready).
5. Conditions: Ready (based on deployment readiness), Connected (always true for k8s), Healthy (based on deployment conditions).
6. Task count: Counts Tasks with `status.assignedProplet == proplet.name`.
7. On Delete: Finalizer ensures Deployment deletion before Proplet deletion.
8. **Finalizer**: Added to ALL proplets (including external) on first reconcile. Only removed after cleanup (k8s: after Deployment deleted; external: immediately, no cleanup).

**External Proplet**:
1. No Deployment created.
2. Status updated based on MQTT liveness messages (LastSeen timestamp).
3. Phase transitions: Initializing → Running (when LastSeen < threshold) → Offline (when LastSeen > threshold).
4. Conditions: Ready (true when online), Connected (true when online, false when offline).
5. Task count: Same as K8s Proplet.
6. On Delete: Finalizer removed immediately (no cleanup needed).

**MQTT Integration**:
- Subscribes to `m/{domainId}/c/{channelId}/messages/#` on startup (if MQTT configured).
- Handles `control/proplet/alive` messages: updates LastSeen, status for matching proplet (by clientId).
- Handles `control/proplet/results` messages: updates Task status with results.
- **Important**: MQTT handlers run in background context, NOT in reconcile context. They directly update status via client.

**MQTT Status Write Semantics** (Parity Requirements):
- **Client**: MQTT handlers use `r.Client` (same client as reconciler, but with background context `context.Background()`).
- **Conflict Handling**: Status updates use `r.updatePropletStatus()` which has retry logic (3 attempts with 100ms backoff per attempt).
- **Concurrency**: MQTT handlers can race with reconciler status updates. Current code handles this via:
  - Status update retry on conflict (in `updatePropletStatus()`)
  - Fresh Get before Update on conflict (re-reads proplet, applies status, updates)
- **Topic Subscription**: QoS from flag (`--mqtt-qos`), timeout from flag (`--mqtt-timeout`, default 30s).
- **Reconnect Behavior**: MQTT client has auto-reconnect enabled (`SetAutoReconnect(true)`), max reconnect interval 1 minute.
- **Parity Requirement**: Preserve exact client usage (`r.Client` with `context.Background()`), retry logic (3 attempts), and conflict handling (fresh Get on conflict).

### Task Behavior

**Pending Phase**:
1. Resolve proplet ID from spec.propletSelector.propletId (or use "default").
2. Determine backend (k8s vs external) by fetching Proplet resource.
3. If k8s: Create Job + ConfigMap, set phase to Running.
4. If external: Publish MQTT message to `m/{domainId}/c/{channelId}/messages/control/manager/start`, set phase to Running.
5. If MQTT not configured and external: Set phase to Failed with error.

**Running Phase**:
1. For k8s: Watch Job status (via `Owns()` watch).
2. If Job succeeded: Extract results, set phase to Completed.
3. If Job failed: Extract error message, set phase to Failed.
4. For external: Results come via MQTT (handled by PropletReconciler.mqttResultHandler).

**Completed/Failed Phase**:
- No further reconciliation.

**Result Extraction** (for k8s Jobs):
1. Check Job annotations: `propeller.absmach.io/result`.
2. Check succeeded Pod's container terminated message.
3. Check ConfigMap: `{job-name}-result`.
4. Check Secret: `{job-name}-result`.
5. Check Pod annotations: `propeller.absmach.io/result`.

### FederatedJob Behavior

**Pending Phase**:
1. Validate spec (experimentId, modelRef, taskWasmImage, participants, kOfN).
2. If invalid: Set phase to Failed with condition.
3. If valid: Create first TrainingRound (`round-1-{name}`), set phase to Running, currentRound = 1.

**Running Phase**:
1. Watch current TrainingRound status (via `Owns()` watch).
2. If TrainingRound completed: Increment completedRounds, update aggregatedModelRef.
3. If completedRounds >= rounds.total: Set phase to Completed.
4. Else: Create next TrainingRound (`round-{next}-{name}`) with aggregated model ref, increment currentRound.
5. If TrainingRound failed: Set phase to Failed.

### TrainingRound Behavior

**Pending Phase**:
1. Initialize participants status array.
2. Create Task for each participant (`{round-name}-{participant-id}`).
3. Set phase to Running.

**Running Phase**:
1. Watch participant Tasks (via `Owns()` watch).
2. Collect updates from completed Tasks (extract FL update envelope from results).
3. Store collected updates in annotation: `propeller.absmach.io/collected-updates`.
4. If updatesReceived >= kOfN: Transition to Aggregating.
5. If timeout exceeded: Set phase to Failed.

**Aggregating Phase**:
1. Aggregate collected updates using algorithm (fedavg or concat).
2. Store aggregated update in annotation: `propeller.absmach.io/aggregated-update`.
3. Set aggregatedModelRef (format: `oci://registry/model:{roundId}-aggregated`).
4. Set phase to Completed.

**Completed/Failed Phase**:
- No further reconciliation.

---

## Kubebuilder Re-scaffold Strategy

### Version Pinning

**Kubebuilder CLI Version**: `4.7.1` (matches current PROJECT file)
**Plugin**: `go.kubebuilder.io/v4` (matches current PROJECT file)
**controller-runtime Version**: `v0.21.0` (pinned in go.mod)
**Kubernetes API Version**: `v0.33.0` (pinned in go.mod)

**Rationale**: These versions are already in use. We will maintain compatibility and pin explicitly.

### Multi-Group Scaffolding

**Current State**: Two API groups:
- `propeller.propeller.abstractmachines.fr/v1` (Proplet, Task)
- `propeller.absmach.io/v1alpha1` (FederatedJob, TrainingRound)

**Kubebuilder Group Naming**:
- Kubebuilder constructs full API group as `{group}.{domain}`
- With `--domain propeller.abstractmachines.fr`:
  - `--group propeller` → `propeller.propeller.abstractmachines.fr` ✅ (matches existing)
  - `--group absmach` → `absmach.propeller.abstractmachines.fr` ❌ (does NOT match `propeller.absmach.io`)

**Strategy for Second Group**:
Since the second group uses a different domain (`propeller.absmach.io`), we have two options:

**Option A: Use kubebuilder create api, then rewrite group** (chosen)
1. Run `kubebuilder create api --group absmach --version v1alpha1 --kind FederatedJob --namespaced`
2. Run `kubebuilder create api --group absmach --version v1alpha1 --kind TrainingRound --namespaced`
3. **Immediately rewrite** `api/v1alpha1/groupversion_info.go`:
   - Change `GroupVersion.Group` from `absmach.propeller.abstractmachines.fr` to `propeller.absmach.io`
4. **Update RBAC markers** in controllers to use `propeller.absmach.io`
5. Run `make generate && make manifests`
6. **Verify** generated CRDs have `spec.group: propeller.absmach.io`

**Option B: Manual API package creation** (alternative)
- Create `api/v1alpha1/` directory manually
- Create `groupversion_info.go` with correct group from start
- Create type files manually
- Skip `kubebuilder create api` for this group

**Chosen: Option A** (use kubebuilder, then fix group)

**Scaffolding Steps**:
1. `kubebuilder init --domain propeller.abstractmachines.fr --repo github.com/absmach/propeller --project-name propeller-k8s-operator`
2. `kubebuilder create api --group propeller --version v1 --kind Proplet --namespaced`
3. `kubebuilder create api --group propeller --version v1 --kind Task --namespaced`
4. `kubebuilder create api --group absmach --version v1alpha1 --kind FederatedJob --namespaced`
5. `kubebuilder create api --group absmach --version v1alpha1 --kind TrainingRound --namespaced`
6. **Rewrite** `api/v1alpha1/groupversion_info.go`: Change `GroupVersion.Group` to `propeller.absmach.io`
7. **Update RBAC markers** in `federatedjob_controller.go` and `traininground_controller.go` to use `propeller.absmach.io`
8. Run `make generate && make manifests`
9. **Gate**: Verify `config/crd/bases/propeller.absmach.io_*.yaml` has `spec.group: propeller.absmach.io`
10. **Gate**: Verify `api/v1alpha1/groupversion_info.go` has `GroupVersion.Group == "propeller.absmach.io"`
11. **Gate**: Verify scheme registration in `cmd/main.go` uses correct group strings

**Note**: Kubebuilder expects consistent domain. The second group uses a different domain (`propeller.absmach.io` vs `propeller.abstractmachines.fr`). This will require manual adjustment of `groupversion_info.go` after scaffolding.

### Project Layout (Post-Scaffold)

```
api/
  v1/              # Group: propeller.propeller.abstractmachines.fr
    proplet_types.go
    task_types.go
    groupversion_info.go
    zz_generated.deepcopy.go
  v1alpha1/        # Group: propeller.absmach.io
    federatedjob_types.go
    traininground_types.go
    groupversion_info.go
    zz_generated.deepcopy.go
internal/
  controller/      # All controllers (migrated from old location)
  mqtt/           # MQTT client (unchanged)
cmd/
  main.go         # Manager setup (migrated/regenerated)
config/           # Kustomize manifests (regenerated)
PROJECT           # Kubebuilder project config (regenerated)
Makefile          # Kubebuilder Makefile (regenerated)
```

### CRD Generation & Parity

**Critical Requirement**: Generated CRDs must match existing CRDs **semantically** (not byte-for-byte, as formatting/ordering may differ).

**Semantic Comparison Targets**:
- `.spec.group` (must match exactly)
- `.spec.names` (kind, plural, singular, shortNames)
- `.spec.scope` (Namespaced vs Cluster)
- `.spec.versions[].name` (version string)
- `.spec.versions[].served` (boolean)
- `.spec.versions[].storage` (boolean)
- `.spec.versions[].subresources.status` (presence/absence)
- `.spec.versions[].additionalPrinterColumns` (columns, types, JSONPaths)
- `.spec.versions[].schema.openAPIV3Schema` (types, required fields, enums, validation rules, defaults)
- `.spec.preserveUnknownFields` (boolean, placement)

**Validation Process**:
1. Generate CRDs from new scaffold: `make manifests`
2. Extract CRDs from `config/crd/bases/`
3. **Canonicalize** both old and new CRDs:
   - Strip metadata (resourceVersion, uid, managedFields, timestamps, generation)
   - Sort keys recursively (for deterministic comparison)
   - Normalize whitespace
   - Extract only semantic fields listed above
4. Compare using `scripts/validate-crd-parity.sh`:
   ```bash
   # Extract semantic schema only
   yq eval '.spec' old-crd.yaml | yq eval 'sort_keys(..)' > old-semantic.yaml
   yq eval '.spec' new-crd.yaml | yq eval 'sort_keys(..)' > new-semantic.yaml
   diff -u old-semantic.yaml new-semantic.yaml
   ```

**Acceptable Differences**:
- Metadata (timestamps, resourceVersion, uid, managedFields)
- Field ordering (if semantically equivalent)
- Whitespace/formatting (if semantically equivalent)
- Description text ordering (if validation rules match)

**Unacceptable Differences**:
- Schema changes (types, required fields, enums, validation rules)
- Printer column changes (names, types, JSONPaths)
- Subresource presence/absence
- preserveUnknownFields value or placement
- Default value changes
- Group/version/kind mismatches

**If Differences Found**:
1. Document exact semantic differences (not formatting).
2. Determine if marker/type changes are needed to match.
3. If differences are intentional (e.g., fixing a bug), explicitly document and justify.
4. If differences are unintentional, fix markers/types to match.
5. **Gate**: Do not proceed until semantic parity achieved.

### RBAC Mapping (Derived from Actual API Calls)

**PropletReconciler**:
- `proplets`: get, list, watch, create, update, patch, delete (primary resource)
- `proplets/status`: get, update, patch (status updates)
- `proplets/finalizers`: update (finalizer management)
- `deployments`: get, list, watch, create, update, patch, delete (Deployment management)
- **Note**: No pods/secrets permissions needed (doesn't read them directly)

**TaskReconciler**:
- `tasks`: get, list, watch, create, update, patch, delete (primary resource)
- `tasks/status`: get, update, patch (status updates)
- `tasks/finalizers`: update (RBAC exists but not used)
- `jobs`: get, list, watch, create, update, patch, delete (Job management, also watched via Owns)
- `configmaps`: get, list, create, update, patch, delete (ConfigMap creation, result extraction)
- `pods`: get, list (result extraction from Job pods)
- `secrets`: get (result extraction from Secret)
- `proplets`: get (to determine backend type)

**FederatedJobReconciler**:
- `federatedjobs`: get, list, watch, create, update, patch, delete (primary resource)
- `federatedjobs/status`: get, update, patch (status updates)
- `trainingrounds`: get, list, watch, create, update, patch, delete (TrainingRound management, also watched via Owns)
- `trainingrounds/status`: get, update, patch (reading TrainingRound status)

**TrainingRoundReconciler**:
- `trainingrounds`: get, list, watch, create, update, patch, delete (primary resource)
- `trainingrounds/status`: get, update, patch (status updates)
- `tasks`: get, list, watch, create, update, patch, delete (Task management, also watched via Owns)
- `tasks/status`: get, update, patch (reading Task status for results)
- `federatedjobs`: get (reading FederatedJob spec for aggregator config)

**Events RBAC** (if Events are added):
- `events`: create, patch (for EventRecorder)

**RBAC Mapping Derived from Controller Code**:

**PropletReconciler**:
- `proplets`: get, list, watch, create, update, patch, delete (primary resource)
- `proplets/status`: get, update, patch (status updates)
- `proplets/finalizers`: update (finalizer management)
- `deployments`: get, list, watch, create, update, patch, delete (Deployment management)
- **Note**: No pods/secrets permissions needed (doesn't read them directly)

**TaskReconciler**:
- `tasks`: get, list, watch, create, update, patch, delete (primary resource)
- `tasks/status`: get, update, patch (status updates)
- `tasks/finalizers`: update (RBAC exists but not used - keep for consistency)
- `jobs`: get, list, watch, create, update, patch, delete (Job management, also watched via Owns)
- `configmaps`: get, list, create, update, patch, delete (ConfigMap creation, result extraction)
- `pods`: get, list (result extraction from Job pods - must include both get and list)
- `secrets`: get (result extraction from Secret)
- `proplets`: get (to determine backend type)

**FederatedJobReconciler**:
- `federatedjobs`: get, list, watch, create, update, patch, delete (primary resource)
- `federatedjobs/status`: get, update, patch (status updates)
- `trainingrounds`: get, list, watch, create, update, patch, delete (TrainingRound management, also watched via Owns)
- `trainingrounds/status`: get, update, patch (reading TrainingRound status)

**TrainingRoundReconciler**:
- `trainingrounds`: get, list, watch, create, update, patch, delete (primary resource)
- `trainingrounds/status`: get, update, patch (status updates)
- `tasks`: get, list, watch, create, update, patch, delete (Task management, also watched via Owns)
- `tasks/status`: get, update, patch (reading Task status for results)
- `federatedjobs`: get (reading FederatedJob spec for aggregator config)

**RBAC Gate Derived from SetupWithManager**:
- For every `Owns(&X{})` watch, ensure RBAC includes: `get`, `list`, `watch` on X
- For every `Create()`, ensure RBAC includes: `create` on resource
- For every `Update()`, ensure RBAC includes: `update` on resource
- For every `Status().Update()`, ensure RBAC includes: `update`, `patch` on `*/status`
- For every `Status().Patch()`, ensure RBAC includes: `patch` on `*/status`
- For finalizer add/remove, ensure RBAC includes: `update` on `*/finalizers`

**Current RBAC Gaps**:
- FederatedJobReconciler: **NO RBAC markers** (missing entirely)
- TrainingRoundReconciler: **NO RBAC markers** (missing entirely)
- TaskReconciler: Missing pods (get, list), secrets (get), configmaps, proplets (get) permissions

### Manager Configuration Parity

**Current Settings** (from code analysis):
- Scheme: clientgoscheme + propellerv1 + propellerv1alpha1
- Metrics: Secure by default (HTTPS), address "0" (disabled by default)
- Health: `:8081` for both healthz and readyz
- Leader Election: ID `"fa27fa49.propeller.abstractmachines.fr`, release on cancel **NOT set**
- MaxConcurrentReconciles: **Not set** (default: 1 per controller)
- Rate Limiter: **Not set** (default exponential backoff)
- Cache: **Not set** (default: all namespaces)

**Parity Requirements**:
- All above settings must match exactly.
- No new controller options unless explicitly documented as change.

### Webhooks

**Current State**: No webhooks implemented.

**Decision**: 
- **Defaulting Webhooks**: Not needed (defaults via kubebuilder markers are sufficient).
- **Validation Webhooks**: Not added initially (preserve current validation in controller).
- **Conversion Webhooks**: Not needed (no version migration).

**Action**: No webhooks in re-implementation. Document validation webhook as future enhancement.

### Watches & Owned Resources

**Current Watches** (exact):
- PropletReconciler: `For(&Proplet{})` only. **NO `Owns(&Deployment{})`**.
- TaskReconciler: `For(&Task{})`, `Owns(&Job{})`.
- FederatedJobReconciler: `For(&FederatedJob{})`, `Owns(&TrainingRound{})`.
- TrainingRoundReconciler: `For(&TrainingRound{})`, `Owns(&Task{})`.

**Decision**: 
- **Preserve exact watch behavior**. Do NOT add `Owns(&Deployment{})` to PropletReconciler unless explicitly documented as behavior change.
- **Rationale**: Adding `Owns()` would change reconcile triggers (Deployment changes would trigger Proplet reconciliation), which is a behavior change.

### Finalizers

**Current Behavior**:
- **Proplet**: Finalizer `propeller.propeller.abstractmachines.fr/finalizer` added to **ALL proplets** (both k8s and external) on first reconcile.
- **K8s Proplet**: Finalizer removed only after Deployment is deleted.
- **External Proplet**: Finalizer removed immediately on delete (no cleanup needed).
- **Task**: No finalizer (but RBAC exists).
- **FederatedJob**: No finalizer.
- **TrainingRound**: No finalizer.

**Parity Requirement**: Preserve exact finalizer behavior.

### Events

**Current State**: No explicit event emission found.

**Decision**: **DEFER Events to post-parity phase**.

**Rationale**:
- Events require RBAC (`events: create, patch`).
- Events are additive but could fail if RBAC missing (violates "no silent changes").
- Events are observability improvement, not core behavior.

**Action**: Do NOT add Events in initial re-implementation. Document as follow-up enhancement.

### CreateOrUpdate Pattern

**Current Pattern**: Manual comparison and update (e.g., `deploymentNeedsUpdate()`).

**Decision**: **DO NOT adopt `controllerutil.CreateOrUpdate`** for strict parity.

**Rationale**:
- `CreateOrUpdate` can mutate fields unintentionally.
- `CreateOrUpdate` changes update semantics (always patches vs conditional update).
- Current manual comparison is explicit and matches exact behavior.

**Action**: Preserve existing manual comparison/update logic.

---

## Step-by-Step Implementation Plan

### Step 0: Kubebuilder Re-scaffold (NEW - Critical)

**0.1 Backup Current State**
- Commit current codebase to git branch `pre-kubebuilder-scaffold`.
- Export current CRDs: `kubectl get crd -o yaml > current-crds.yaml`.
- Capture golden fixtures (see Step 0.2).

**0.2 Create Golden Fixtures**
- Create `test/fixtures/` directory.
- For each CRD type, create sample CR YAMLs:
  - `test/fixtures/proplet-k8s.yaml`
  - `test/fixtures/proplet-external.yaml`
  - `test/fixtures/task-k8s.yaml`
  - `test/fixtures/task-external.yaml`
  - `test/fixtures/federatedjob.yaml`
  - `test/fixtures/traininground.yaml`
- For each sample CR, capture expected created resources:
  - `test/fixtures/expected/deployment-{proplet-name}.yaml`
  - `test/fixtures/expected/job-{task-name}.yaml`
  - `test/fixtures/expected/configmap-{task-name}.yaml`
  - `test/fixtures/expected/task-{traininground-name}-{participant}.yaml`
  - `test/fixtures/expected/traininground-round-{n}-{job-name}.yaml`
- Capture expected status fields after reconciliation.

**0.2.1 Fixture Normalization Rules**
- **Strip volatile metadata** from expected outputs:
  - `metadata.resourceVersion`
  - `metadata.uid`
  - `metadata.managedFields`
  - `metadata.creationTimestamp` (or compare with tolerance)
  - `metadata.generation`
- **Normalize status timestamps**:
  - Strip `status.*At` timestamps OR compare with time tolerance (±5s)
  - Strip `status.conditions[].lastTransitionTime` OR compare with tolerance
- **Compare only owned spec fields**:
  - For Deployment: compare `spec.replicas`, `spec.template.spec.containers[]`, `spec.selector`, `metadata.labels`
  - For Job: compare `spec.template.spec`, `metadata.labels`, `metadata.ownerReferences`
  - For ConfigMap: compare `data`, `metadata.labels`, `metadata.ownerReferences`
- **Normalization script**: `scripts/normalize-fixture.sh` will:
  - Remove volatile metadata
  - Sort keys recursively
  - Extract only owned spec fields
  - Compare status with timestamp tolerances

**0.3 Initialize Kubebuilder Project**
```bash
# In clean directory or branch
kubebuilder init --domain propeller.abstractmachines.fr --repo github.com/absmach/propeller --project-name propeller-k8s-operator
```
- Verify `PROJECT` file matches expected structure.
- Verify `Makefile` is generated.
- Verify `config/` directory structure.

**0.4 Create API Groups and Kinds**

**Deterministic Sequence**:

```bash
# First group: propeller.propeller.abstractmachines.fr/v1
kubebuilder create api --group propeller --version v1 --kind Proplet --namespaced
kubebuilder create api --group propeller --version v1 --kind Task --namespaced

# Second group: propeller.absmach.io/v1alpha1
# Step 1: Create APIs (will generate with wrong group initially)
kubebuilder create api --group absmach --version v1alpha1 --kind FederatedJob --namespaced
kubebuilder create api --group absmach --version v1alpha1 --kind TrainingRound --namespaced

# Step 2: Immediately rewrite group in groupversion_info.go
# Edit api/v1alpha1/groupversion_info.go:
#   Change: GroupVersion.Group = "absmach.propeller.abstractmachines.fr"
#   To:     GroupVersion.Group = "propeller.absmach.io"

# Step 3: Update RBAC markers in controllers
# Edit internal/controller/federatedjob_controller.go:
#   Change: // +kubebuilder:rbac:groups=absmach.propeller.abstractmachines.fr
#   To:     // +kubebuilder:rbac:groups=propeller.absmach.io
# Same for traininground_controller.go

# Step 4: Regenerate
make generate
make manifests

# Step 5: Verify group correctness (GATE)
# Check config/crd/bases/propeller.absmach.io_*.yaml has spec.group: propeller.absmach.io
# Check api/v1alpha1/groupversion_info.go has GroupVersion.Group == "propeller.absmach.io"
# Check cmd/main.go scheme registration uses "propeller.absmach.io"
```

**Gate: API Group String Correctness**
After Step 0.4, verify:
1. `api/v1/groupversion_info.go`: `GroupVersion.Group == "propeller.propeller.abstractmachines.fr"`
2. `api/v1alpha1/groupversion_info.go`: `GroupVersion.Group == "propeller.absmach.io"`
3. `config/crd/bases/propeller.propeller.abstractmachines.fr_*.yaml`: `spec.group == "propeller.propeller.abstractmachines.fr"`
4. `config/crd/bases/propeller.absmach.io_*.yaml`: `spec.group == "propeller.absmach.io"`
5. `cmd/main.go`: Scheme registration uses both correct group strings
6. RBAC markers in controllers use correct group strings
7. **Exit 1 if any mismatch**

**0.5 Migrate Type Definitions**
- Copy type definitions from old `api/v1/` to new scaffolded `api/v1/`.
- Copy type definitions from old `api/v1alpha1/` to new scaffolded `api/v1alpha1/`.
- Ensure all kubebuilder markers are preserved.
- Run `make generate` to regenerate deepcopy.

**0.6 Generate CRDs and Compare**
- Run `make manifests` to generate CRDs.
- Compare generated CRDs against current CRDs:
  ```bash
  # Extract schemas only (ignore metadata)
  yq eval '.spec.versions[0].schema' config/crd/bases/*.yaml > new-schemas.yaml
  yq eval '.spec.versions[0].schema' <current-crd-path> > old-schemas.yaml
  diff -u old-schemas.yaml new-schemas.yaml
  ```
- If differences found, document and fix markers/types.
- **Gate**: Do not proceed until CRDs match (or differences are documented and justified).

### Step 1: Migrate Controllers

**1.1 Create Controller Scaffolds**
- Controllers are already scaffolded by `kubebuilder create api`.
- Move existing controller logic into scaffolded controllers.
- Preserve exact reconcile logic.

**1.2 Add RBAC Markers**
- Add RBAC markers to all controllers based on RBAC mapping above.
- Run `make manifests` to regenerate `config/rbac/role.yaml`.
- Verify generated RBAC matches requirements.

**1.3 Preserve Watch Behavior**
- **Do NOT add `Owns(&Deployment{})` to PropletReconciler** (preserve current behavior).
- Preserve all other watches exactly.

**1.4 Preserve Update Logic**
- **Do NOT adopt `CreateOrUpdate`** (preserve manual comparison).
- Preserve exact comparison and update logic.

### Step 2: Migrate Manager Setup

**2.1 Migrate main.go**
- Copy manager setup from old `cmd/main.go`.
- Ensure scheme registration matches (clientgoscheme + both API groups).
- Ensure all flags match.
- Ensure metrics/health/leader election config matches.

**2.2 Preserve Manager Options**
- MaxConcurrentReconciles: Not set (default).
- Rate Limiter: Not set (default).
- Cache: Not set (default: all namespaces).
- LeaderElectionReleaseOnCancel: Not set (preserve current).

**2.3 MQTT Integration**
- Preserve MQTT setup logic.
- Preserve MQTT handler registration.
- Ensure MQTT is optional (graceful handling when nil).

### Step 3: Migrate Supporting Code

**3.1 Internal Packages**
- Copy `internal/mqtt/` unchanged.
- Copy `internal/controller/aggregation.go` unchanged.
- Copy `internal/controller/result_extractor.go` unchanged.
- Copy `internal/controller/traininground_helpers.go` unchanged.
- **Decision on `internal/scheduler/`**: Document as unused, remove or keep with comment.

### Step 4: Update Generated Files

**4.1 Regenerate All**
- Run `make generate` (deepcopy).
- Run `make manifests` (CRDs, RBAC).
- Verify no unexpected changes.

**4.2 CRD Parity Check**
- Re-run CRD diff (from Step 0.6).
- **Gate**: CRDs must match or differences documented.

### Step 5: Golden Fixture Validation

**5.1 Apply Fixtures**
- Deploy new operator to test cluster.
- Apply golden fixture CRs.
- Wait for reconciliation.

**5.2 Compare Outputs**
- Extract created resources: `kubectl get -o yaml deployment/job/configmap/task/traininground`.
- Compare against expected fixtures:
  ```bash
  diff -u test/fixtures/expected/*.yaml <extracted-resources>/*.yaml
  ```
- Compare status fields.
- **Gate**: All resources must match (or differences documented).

### Step 6: Integration Testing

**6.1 End-to-End Scenarios**
- Test K8s Proplet lifecycle (create, update, delete).
- Test External Proplet lifecycle (create, MQTT liveness, delete).
- Test Task execution (k8s and external backends).
- Test FederatedJob → TrainingRound → Task flow.
- Test result extraction paths.
- Test finalizer behavior.

**6.2 Edge Cases**
- Status update conflicts.
- MQTT message handling.
- Timeout scenarios.
- Error recovery.

### Step 7: Documentation

**7.1 Behavior Changes**
- Document any intentional changes (currently: none planned).
- Document deferred improvements (Events, CreateOrUpdate, webhooks).

**7.2 Migration Guide**
- Document how to migrate from old operator to new.
- Document any breaking changes (currently: none).

---

## Risk List

### High Risk

**1. CRD Schema Differences**
- **Risk**: Kubebuilder regeneration may change schema in unexpected ways.
- **Mitigation**: Byte-for-byte diff gate, fix markers/types to match.
- **Validation**: Automated CRD diff in CI.

**2. Watch Behavior Changes**
- **Risk**: Adding `Owns()` to PropletReconciler would change reconcile triggers.
- **Mitigation**: Explicitly preserve current watch behavior (no `Owns(&Deployment{})`).
- **Validation**: Test that Deployment changes do NOT trigger Proplet reconciliation.

**3. Finalizer Behavior**
- **Risk**: Finalizer logic for external proplets might be misunderstood.
- **Mitigation**: Document exact behavior, preserve code logic.
- **Validation**: Test finalizer add/remove for both proplet types.

### Medium Risk

**4. Multi-Group Domain Mismatch**
- **Risk**: Second group uses different domain, requires manual adjustment.
- **Mitigation**: Document manual steps, verify groupversion_info.go matches.
- **Validation**: Verify API group strings in generated CRDs.

**5. Manager Configuration Drift**
- **Risk**: Default controller-runtime settings might differ.
- **Mitigation**: Explicitly set all manager options to match current.
- **Validation**: Compare manager config in code.

**6. RBAC Gaps**
- **Risk**: Missing RBAC might cause runtime failures.
- **Mitigation**: Map all API calls to RBAC, add all required permissions.
- **Validation**: Test with minimal RBAC, verify no permission errors.

### Low Risk

**7. Code Organization**
- **Risk**: File locations might differ slightly.
- **Mitigation**: Preserve logical structure, document any moves.
- **Validation**: Verify imports resolve correctly.

**8. Makefile Differences**
- **Risk**: Generated Makefile might have different targets.
- **Mitigation**: Preserve custom targets if any, document differences.
- **Validation**: Test all Makefile targets.

---

## Parity Traps Checklist

**Common Silent Behavior Changes** (must verify each):

- [ ] **Defaulted fields**: Verify `replicas` default (1), `imagePullPolicy` (Always for proplet), etc.
- [ ] **Selector labels**: Verify Deployment selector matches pod labels exactly (no rollout break).
- [ ] **OwnerReferences**: Verify `controller=true`, `blockOwnerDeletion` settings match.
- [ ] **Status condition transitions**: Verify `lastTransitionTime` updates only on status change (not on every reconcile).
- [ ] **Leader election release on cancel**: Verify NOT set (preserve current behavior).
- [ ] **Reconcile frequency**: Verify `Owns()` watches don't change reconcile triggers (Deployment changes don't trigger Proplet reconcile).
- [ ] **Finalizer timing**: Verify finalizer added on first reconcile, removed after cleanup.
- [ ] **Status update conflicts**: Verify retry logic (3 attempts with backoff) preserved.
- [ ] **MQTT handler context**: Verify background context (not reconcile context) preserved.
- [ ] **Result extraction order**: Verify order preserved (Job annotation → Pod message → ConfigMap → Secret → Pod annotation).
- [ ] **Container env vars**: Verify exact env var names and values match (PROPLET_*, TASK_ID, etc.).
- [ ] **Resource naming**: Verify all resource names match exactly (`{name}-proplet`, `{name}-job`, etc.).
- [ ] **Label keys/values**: Verify all labels match exactly (app.kubernetes.io/*, propeller.absmach.fr/*).
- [ ] **Annotation keys**: Verify annotation keys match exactly (`propeller.absmach.io/*`).

**Validation Method**:
- Each trap verified via golden fixtures OR targeted unit test
- Document any intentional changes
- **Gate**: All traps verified before proceeding to Phase 2

---

## Files to Create/Modify

### Files to Create (New Scaffold)

1. `PROJECT` (regenerated by kubebuilder init)
2. `Makefile` (regenerated by kubebuilder init)
3. `api/v1/proplet_types.go` (migrated from old)
4. `api/v1/task_types.go` (migrated from old)
5. `api/v1alpha1/federatedjob_types.go` (migrated from old)
6. `api/v1alpha1/traininground_types.go` (migrated from old)
7. `api/v1/groupversion_info.go` (regenerated, may need adjustment)
8. `api/v1alpha1/groupversion_info.go` (regenerated, **must adjust domain**)
9. `api/v1/zz_generated.deepcopy.go` (regenerated)
10. `api/v1alpha1/zz_generated.deepcopy.go` (regenerated)
11. `internal/controller/proplet_controller.go` (migrated from old)
12. `internal/controller/task_controller.go` (migrated from old)
13. `internal/controller/federatedjob_controller.go` (migrated from old)
14. `internal/controller/traininground_controller.go` (migrated from old)
15. `cmd/main.go` (migrated from old, may need scheme adjustments)
16. `config/crd/bases/*.yaml` (regenerated, must match existing)
17. `config/rbac/role.yaml` (regenerated, must include all required permissions)
18. `test/fixtures/*.yaml` (new, golden fixtures)

### Files to Modify (Existing)

1. `api/v1/groupversion_info.go` (verify domain matches)
2. `api/v1alpha1/groupversion_info.go` (adjust domain to `propeller.absmach.io`)
3. All controller files (add RBAC markers, preserve logic)
4. `cmd/main.go` (verify scheme registration, manager config)

### Files to Remove

1. `internal/scheduler/` (if confirmed unused, or document as future feature)

---

## Deployment/Manifests Parity

**Current Deployment Manifest** (from `config/manager/manager.yaml`):
- Deployment name: `propeller-k8s-operator-controller-manager`
- ServiceAccount: `propeller-k8s-operator-controller-manager`
- Container args: (from main.go flags)
- Container env: (if any)
- Probes: healthz on `:8081`, readyz on `:8081`
- Metrics port: (if enabled)
- Leader election: ID `fa27fa49.propeller.abstractmachines.fr`
- Security context: (if any)

**Parity Requirements**:
- Deployment name must match (or document change)
- ServiceAccount name must match (or document change)
- Container args must match (all flags preserved)
- Probe endpoints must match (`:8081` for both)
- Leader election ID must match
- Metrics/health configuration must match

**Validation**:
- Compare `config/manager/manager.yaml` (generated) against current
- Compare `config/rbac/service_account.yaml` name
- Compare `config/rbac/role_binding.yaml` subjects and roleRef
- **Gate**: Manifest diff must show only acceptable differences (metadata, formatting)

## Validation Plan

### Automated Validation

**1. CRD Semantic Diff Gate**
```bash
# In CI or pre-commit
make manifests
./scripts/validate-crd-parity.sh  # Semantic comparison (not byte-for-byte)
# Exit 1 if semantic differences found (unless documented)
```

**2. Golden Fixture Tests (Normalized)**
```bash
# Apply fixtures, extract outputs, normalize, compare
kubectl apply -f test/fixtures/
# Wait for reconciliation
kubectl get -o yaml deployment/job/configmap/task/traininground > actual-outputs.yaml
./scripts/normalize-fixture.sh actual-outputs.yaml > normalized-actual.yaml
./scripts/normalize-fixture.sh test/fixtures/expected/*.yaml > normalized-expected.yaml
diff -u normalized-expected.yaml normalized-actual.yaml
# Exit 1 if differences found
```

**3. RBAC Validation**
```bash
# Verify all required permissions are present
./scripts/validate-rbac.sh  # Checks role.yaml against RBAC mapping
# Exit 1 if gaps found
```

**4. API Group String Gate**
```bash
# Verify group strings are correct
./scripts/validate-api-groups.sh  # Checks groupversion_info.go and CRDs
# Exit 1 if mismatches found
```

**5. Deployment Manifest Parity**
```bash
# Compare generated manifests against current
./scripts/validate-manifests-parity.sh  # Compares manager.yaml, RBAC bindings
# Exit 1 if differences found (unless documented)
```

### Manual Parity Verification

1. **Deploy current operator** to test cluster.
2. **Apply sample CRs** (from golden fixtures).
3. **Record created resources** (names, labels, specs, status).
4. **Deploy new operator** to same cluster (different namespace or replace).
5. **Apply same CRs**.
6. **Compare**:
   - Resource names match.
   - Resource specs match (labels, annotations, env vars, etc.).
   - Status updates match (phases, conditions, timestamps).
   - MQTT messages processed (if MQTT configured).
7. **Test edge cases**:
   - Proplet deletion (verify finalizer behavior).
   - Task completion (verify results extracted).
   - TrainingRound aggregation (verify aggregated model ref).
   - Status update conflicts (verify retry works).

---

## Summary

This plan documents a **complete Kubebuilder re-scaffold** of the operator. Key principles:

1. **Full re-scaffold**: Use `kubebuilder init` and `kubebuilder create api` as source of truth.
2. **Version pinning**: Explicit Kubebuilder, plugin, and controller-runtime versions.
3. **Byte-for-byte CRD parity**: Generated CRDs must match existing (with diff gate).
4. **Multi-group handling**: Explicit steps for two API groups with different domains.
5. **Complete RBAC mapping**: Derived from actual API calls, not assumptions.
6. **Preserve watch behavior**: Do NOT add `Owns(&Deployment{})` unless documented as change.
7. **No CreateOrUpdate**: Preserve manual comparison logic.
8. **Defer Events**: Do not add in initial implementation.
9. **Golden fixtures**: Automated validation of resource creation.
10. **Explicit finalizer behavior**: Document exact add/remove logic.

**Estimated Changes**:
- **Behavior changes**: None intended. The following items are high-risk for accidental behavior change:
  - CRD regeneration (mitigated by diff gate)
  - Watch behavior (mitigated by explicit preservation)
  - Manager config (mitigated by explicit settings)
- **Code changes**: ~1000 lines migrated (types, controllers, main), ~100 lines added (RBAC markers, fixtures)
- **Will only proceed if**: CRD diffs are clean, golden fixtures match, RBAC is complete

This plan matches the "brutally direct, no shortcuts" specification.
