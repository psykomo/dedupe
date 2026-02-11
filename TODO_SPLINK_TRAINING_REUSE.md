# TODO: Splink Training Reuse and Retraining Strategy

## Goal
Implement stable, reusable Splink model parameters so bootstrap/incremental runs are faster and decisions are consistent across batches.

## Current State
- Training runs every time Splink is invoked.
- Trained parameters are not persisted across runs.
- Incremental/bootstrap small batches can produce unstable EM estimates.

## Scope
- In scope: parameter persistence, train trigger policy, runtime selection logic, observability, tests.
- Out of scope (for this phase): UI for review queue, major schema redesign of canonical entities.

## Phase 1: Config and Contracts
- [ ] Add config option `run.train_mode` with values: `always`, `bootstrap_only`, `periodic`.
- [ ] Add config option `run.training_artifact_path` (path for persisted model artifacts).
- [ ] Add config option `run.training_min_records` (minimum rows required to retrain).
- [ ] Add config option `run.periodic_retrain_every_runs` (integer, used when `periodic`).
- [ ] Add config option `run.periodic_retrain_max_age_hours` (integer, used when `periodic`).
- [ ] Add config option `run.force_retrain` (boolean override for a single run).
- [ ] Define backward-compatible defaults so existing configs continue working.

Acceptance Criteria:
- Existing `config.dummy.yaml` works without changes.
- New fields are validated with clear errors.

## Phase 2: Persisted Training Metadata
- [ ] Add DuckDB table `model_registry`:
  - `model_id` (PK)
  - `model_version`
  - `created_at`
  - `source_table`
  - `trained_on_run_id`
  - `trained_rows`
  - `training_window_start`
  - `training_window_end`
  - `settings_json`
  - `parameters_json`
  - `metrics_json`
  - `is_active`
- [ ] Add index for quick lookup of active model by `source_table`.
- [ ] Add store methods:
  - `get_active_model(source_table)`
  - `save_model_artifact(...)`
  - `activate_model(model_id)`
  - `list_models(source_table, limit)`

Acceptance Criteria:
- Migration can run on existing DuckDB safely.
- At most one active model per source table.

## Phase 3: Splink Runner Refactor
- [ ] Refactor runner into explicit steps:
  - build settings
  - train (optional)
  - predict
  - export trained params
  - import trained params
- [ ] Ensure `run_full_dedupe` and `run_incremental_link` can accept pre-trained parameters.
- [ ] Keep current behavior as fallback when no artifact is available.

Acceptance Criteria:
- Same function signatures remain supported or have a compatibility shim.
- Prediction works with and without retraining.

## Phase 4: Training Trigger Logic in Pipeline
- [ ] Add `should_retrain(...)` decision function using:
  - `train_mode`
  - existing active model availability
  - model age
  - run count since last retrain
  - `training_min_records`
  - `force_retrain`
- [ ] Apply policy per run type:
  - `full`: retrain unless explicitly disabled.
  - `bootstrap`: retrain only when policy says so.
  - `incremental`: default reuse active model.
- [ ] Write trigger reason to `dedupe_run.metadata_json`.

Acceptance Criteria:
- Decision path is deterministic and logged in run metadata.
- Incremental runs usually reuse, not retrain.

## Phase 5: Failure and Recovery Rules
- [ ] If retrain fails, fallback to active model (if exists) and continue run.
- [ ] If no active model exists and retrain fails, fail run with clear message.
- [ ] Validate artifact integrity before use.
- [ ] Guard against partial writes by writing artifact then atomically activating model row.

Acceptance Criteria:
- Simulated failure paths produce predictable status and error messages.

## Phase 6: CLI and Operations
- [ ] Add CLI `train-model` command:
  - trains using configured source
  - persists artifact
  - optionally activates model
- [ ] Add CLI `model-status` command:
  - shows active model, age, trained rows, last usage
- [ ] Add CLI `model-activate --model-id ...` command.
- [ ] Update README with:
  - how to run first training
  - how periodic retraining works
  - how to force retrain
  - rollback to older model

Acceptance Criteria:
- Ops can inspect and switch models without code changes.

## Phase 7: Testing Plan
- [ ] Unit tests:
  - config validation for train options
  - trigger decision logic
  - store methods for model registry
- [ ] Integration tests:
  - bootstrap run with retrain then reuse
  - incremental run reuses active model
  - retrain failure fallback path
  - no active model + retrain failure -> run fails
- [ ] Performance checks on dummy data:
  - compare run time `always` vs `reuse`
  - compare match distribution stability across batches
- [ ] Data quality checks:
  - stable auto-match rate range
  - review queue rate range
  - entity growth trend sanity

Acceptance Criteria:
- Tests pass in CI/local.
- Measured runtime improvement in reuse mode.

## Phase 8: Rollout Plan
- [ ] Step 1: ship schema + passive metadata writes (no behavior change).
- [ ] Step 2: enable reuse in non-production environment.
- [ ] Step 3: run side-by-side validation (`always` vs `reuse`) for N runs.
- [ ] Step 4: enable `bootstrap_only` in production.
- [ ] Step 5: move to `periodic` after stability threshold is met.

Exit Criteria:
- No increase in bad merges from manual review.
- Runtime reduced for incremental runs.
- Model management SOP documented.

## Open Questions to Resolve Before Implementation
- [ ] Should model artifacts be stored only in DuckDB (`parameters_json`) or also as external files?
- [ ] What is acceptable model staleness window (hours/days)?
- [ ] Who approves model activation in production?
- [ ] Do we need separate active models per `id_upt` or one global per source table?
- [ ] Should review outcomes feed supervised calibration later?

## Suggested Default Decisions (Can Be Changed)
- `train_mode = bootstrap_only`
- `training_min_records = 50000`
- `periodic_retrain_every_runs = 20`
- `periodic_retrain_max_age_hours = 168` (7 days)
- fallback behavior: reuse last active model on retrain failure
