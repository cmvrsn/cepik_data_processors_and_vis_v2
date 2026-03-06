# MotoIntel CEPIK – Architectural & Technical Review

## 1) High-level architecture assessment

### What this application is
The repository contains a full data platform slice for CEPIK vehicle data:
- **Ingestion** from CEPIK API using an ECS/Fargate worker (`worker.py`) writing raw partitioned parquet snapshots to S3 (`snapshots/archive/snapshot_date=...`).
- **Orchestration** via Step Functions (`orchestrator.json`) coordinating planning, ingestion, publication, transformation, QA, archive repair, historical trend build, and notification.
- **Serving layer** through Athena-backed API functions (`dashboard_api_endpoint`) and a Streamlit dashboard (`dashboard_prototype/dashboard.py`).
- **Analytical model** centered around `motobi_prod_latest` (fact-like aggregate) and historical `motobi_prod_snapshot_trend`, with DIM tables used for filters and canonicalization.

### Is it coherent / production-like?
**Partially yes.** The pipeline has a clear staged design and separation of concerns (plan → ingest → publish → build → validate → notify), but several implementation choices reduce production safety:
- Heavy reliance on mutable singleton targets (`snapshots/latest`, `prod-data/latest`, single table names) instead of versioned immutable outputs + promoted pointers.
- Minimal concurrency controls and no run-level lock, making overlapping runs risky.
- Limited explicit idempotency in key stages (except snapshot trend month check).
- Some operational hardcoding (AWS ARNs, subnets, SGs in ASL) that weakens portability.

## 2) Orchestration and data flow quality

### Current flow (as implemented)
1. `Plan Snapshot` returns run metadata (years, months, cluster/task ARN, bucket/prefix).
2. `Run Single Full Snapshot Worker` starts one ECS task over full date scope.
3. `Publish RAW` deletes `snapshots/latest` and copies selected archive snapshot there.
4. `Build PROD` drops/rebuilds `motobi_prod_latest` using CTAS from `motobi_raw_latest`.
5. `QA Validate` compares raw row count vs sum(total_count) from prod.
6. `Repair RAW Archive` runs `MSCK REPAIR TABLE raw_archive` in history DB.
7. `Build Snapshot Trend` inserts one month slice to historical trend table if missing.
8. `Send Notification` tries sending XLSX report via SES.

### Architectural fit
- **Strong points:** explicit pipeline order, batch-oriented data model, historical trend append step, quality gate telemetry.
- **Weak points:** destructive writes and swaps are not atomic from consumer perspective; no compensating transaction strategy if a mid-pipeline step fails after deleting/publishing.

## 3) Risks and anti-patterns

### Idempotency risks
- `Publish RAW` always deletes `snapshots/latest` then copies, with no version pointer / two-phase switch.
- `Build PROD` drops and recreates `motobi_prod_latest` + empties S3 prefix before CTAS; reruns during failure windows can leave missing dataset.
- `Build Snapshot Trend` checks only `snapshot_month`, so rerun with corrected data in same month cannot replace stale month without manual delete.

### Data consistency risks
- QA compares `COUNT(*)` raw vs `SUM(total_count)` prod, but transformations include filters/grouping; ratio drift may be expected and not strict integrity proof.
- `MSCK REPAIR` is used as repair mechanism; on large partition sets it can be slow and eventually inconsistent during active writes.
- Dashboard filters combine raw string interpolations in SQL; unexpected values can cause query errors and inconsistent user experience.

### Hidden state
- Mutable S3 prefixes (`latest`) and mutable Athena table names create hidden current-state coupling.
- Streamlit in prototype imports Athena client directly (bypassing API boundary), creating dual access modes.

### Coupling concerns
- ASL references concrete Lambda ARNs, ECS task defs, subnet/SG IDs directly; infra and app logic are tightly coupled.
- Dashboard API and Streamlit share same Athena client module, so query-shape changes can break both planes simultaneously.

### Error handling and race conditions
- Most Athena polling loops have no timeout/circuit breaker (except API `run_athena_query` timeout), risking long-running waits and Lambda timeout.
- No distributed lock in Step Functions to prevent simultaneous pipeline executions racing on same `latest` targets.
- `repair_raw_archive` likely times out on large partition cardinality due to full-table `MSCK REPAIR` and unbounded wait loop.

## 4) DIM tables used by visualizations

### DIM tables identified
- `motobi_cepik.dim_region`
- `motobi_cepik.dim_brand`
- `motobi_cepik.dim_metadata`
- `motobi_cepik.dim_vehicle_subtype`
- `motobi_cepik.dim_powiat_mapping` (map canonicalization)
- plus reference `motobi_cepik.population` used for normalization/indexing

### How they are fed today
No DIM build job exists in orchestration code. Dashboard/API assume DIM tables already exist and are query-ready. This implies DIM refresh is external/manual or managed outside this repo.

### How they are refreshed currently
From code perspective: **not refreshed in pipeline**. Reads are direct from Athena tables in every request or Streamlit cache window.

### Safe DIM refresh strategy (recommended)
1. Build DIMs in a dedicated `Build Dimensions` stage after successful `Build PROD`.
2. Use **versioned outputs** (`dim_*_vYYYYMMDDHHMM`) or partitioned `snapshot_date` in DIM tables.
3. Validate DIM row counts/null ratios/uniqueness constraints.
4. Promote via view or pointer table (`dim_*_current`) only after validation.
5. Keep fact and DIM promotion in same run transaction semantics:
   - fact published version id = `run_id`
   - dims published version id = same `run_id`
   - dashboard reads only where `version_id = current_published_version`.

## 5) `repair_raw_archive` timeout analysis and migration recommendation

### Likely timeout causes
- The Lambda executes `MSCK REPAIR TABLE raw_archive` on historical archive table (`motobi_cepik_hist.raw_archive`). On large archive partitions this is notoriously long.
- Polling loop has no local timeout and waits until Athena terminal state; Lambda hard timeout can be reached first.
- Step runs after multiple prior tasks, so overall schedule pressure can amplify retries/timeouts.

### Should this be ECS instead of Lambda?
**Yes, for robust production scale.** `MSCK REPAIR` on growing historical data is a long-running metadata maintenance task better suited for ECS/Fargate or EMR-style batch with larger timeout envelope.

### Clean migration path (no hacks)
1. Create dedicated ECS task (`raw-archive-maintainer`) that can:
   - run `MSCK REPAIR` OR (preferred) incremental `ALTER TABLE ADD PARTITION` from discovered S3 partitions.
2. Add new Step Functions task using `ecs:runTask.sync` after QA.
3. Keep Lambda as fallback initially behind feature flag; cut over after soak period.
4. Add explicit max runtime and CloudWatch metric alarms.
5. Long-term: stop relying on full `MSCK`; emit partition registration incrementally as data is written.

## 6) Visualization layer data access and dependencies

### Data fetching model
- Streamlit prototype imports and invokes Athena client directly, using `st.cache_data` wrappers for dimensions and selected trend queries.
- API Lambda exposes similar endpoints, but prototype currently bypasses API for many paths.

### Dependency mapping (high level)
- Filters:
  - voivodeship/county from `dim_region`
  - brands/models from `dim_brand` + fact table constrained queries
  - origin/alt fuel from `dim_metadata`
  - subtype from `dim_vehicle_subtype`
- Visual metrics:
  - KPI/fuel/origin/top brands/top models/trends from `motobi_prod_latest`
  - snapshot trend from `motobi_cepik_hist.motobi_prod_snapshot_trend`
  - map metrics combine fact + `population` + `dim_powiat_mapping`

### Performance bottlenecks
- Frequent Athena query-per-widget model; many charts call separate scans for same filter set.
- Dynamic SQL string composition with large IN clauses (brands/models/counties) may degrade planning and scan efficiency.
- Lack of pre-aggregated serving tables for common dashboard grains (monthly x type x region).
- `MSCK REPAIR`-driven partition discovery can delay data visibility and dashboard freshness.

## 7) Prioritized improvement roadmap

### P0 (stability & data correctness)
- Introduce run lock / singleton execution guard in Step Functions.
- Replace destructive `latest` rewrites with versioned snapshots + atomic pointer switch (view or metadata pointer).
- Add explicit timeout handling and retry policies per Athena/Lambda task.
- Migrate `repair_raw_archive` to ECS and/or incremental partition registration.

### P1 (consistency & observability)
- Add dedicated DIM build-and-promote stage tied to same `run_id` as fact publish.
- Add data quality checks beyond raw/prod count ratio (null spikes, distinct cardinalities, partition completeness).
- Emit structured run manifest (run_id, snapshot_date, source ranges, table versions).

### P2 (performance)
- Consolidate dashboard queries into fewer pre-aggregated tables/materialized views.
- Introduce API-side cache for common filter combinations.
- Separate Streamlit prototype from direct Athena access in production; use API contract only.

## 8) Executive recommendation for `repair_raw_archive`

Move the task to ECS now, and redesign toward incremental partition registration (event-driven) so full-table `MSCK REPAIR` is no longer required in normal runs. Keep Lambda only for lightweight metadata tasks, not for unbounded archive maintenance.
