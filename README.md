# Self-Healing Data Pipeline

An ETL pipeline that catches its own data-quality failures, diagnoses the root
cause, applies a remediation playbook and re-runs - escalating to a human only
when a circuit breaker trips instead of looping forever.

Built in Python with pandas and SQLite so the whole thing runs locally in
under a minute. The `airflow/`, `spark/` and `dbt/` directories show how the
same detect -> diagnose -> heal loop maps onto a production stack
(Airflow orchestration, PySpark/Delta Lake on Databricks, dbt warehouse tests).

## Why

Data pipelines rarely fail with a clean error. They fail with a renamed
column, a null spike from a broken upstream join, a retried extract that
doubles your keys, or a partial load at 3am. The standard response is a pager,
a human reading logs, and a hand-rolled fix. This project encodes that loop:

1. **Detect** - fail-fast quality gates (schema, types, nulls, duplicates, volume)
   run before anything is loaded.
2. **Diagnose** - a rule-based classifier maps the failing checks to a root
   cause with a confidence score. An optional LLM advisor (any
   OpenAI-compatible endpoint) classifies failures the rules cannot.
3. **Heal** - a playbook registry maps each diagnosis to a remediation:
   re-map drifted columns, strip corrupted types, quarantine bad rows,
   de-duplicate on the primary key, or re-extract from source.
4. **Escalate** - a circuit breaker caps healing attempts. When it opens, the
   incident log has everything a human needs: failing checks, diagnosis,
   actions tried.

## Quickstart

```bash
pip install -r requirements.txt
PYTHONPATH=src python -m healer.demo     # narrated demo, ~1 second
pip install -r requirements-dev.txt
pytest -q                                # 19 tests
```

The demo generates 500 synthetic orders, runs a clean baseline load, then
injects five classic upstream failures one at a time. The pipeline heals all
five automatically:

```
--- Scenario 1: schema drift (upstream renamed 'amount' to 'order_amount') ---
  attempt 1: FAILED
      MISSING_COLUMN: required column 'amount' is missing from the extract
  diagnose: schema_drift (confidence 0.95) - required column(s) ['amount'] missing; upstream likely renamed or dropped fields
  heal: applying playbook 'schema_drift'
  attempt 2: SUCCESS - loaded 500 rows

--- Scenario 3: null spike (a broken upstream join dropped customer_id on 20% of rows) ---
  attempt 1: FAILED
      NULL_SPIKE: column 'customer_id' null rate 20.0% exceeds the 1.0% threshold
  diagnose: null_spike (confidence 0.85) - null rate above threshold in ['customer_id']; likely a broken upstream join or feed gap
  heal: applying playbook 'null_spike'
  attempt 2: SUCCESS - loaded 400 rows

=== Summary ===
5/5 injected failures healed automatically
warehouse rows: 500 | quarantined: 100
```

Every failure, diagnosis and action is appended to
`demo_out/reports/incidents.jsonl`, and a run summary lands in
`demo_out/reports/last_run.json`.

## What heals what

| Failure injected        | Check that fires   | Diagnosis         | Playbook                                        |
|-------------------------|--------------------|-------------------|-------------------------------------------------|
| Column renamed upstream | `MISSING_COLUMN`   | `schema_drift`    | re-map columns by normalized name               |
| `"USD 12.30"` strings   | `TYPE_MISMATCH`    | `type_corruption` | strip non-numerics, re-coerce                   |
| 20% nulls in a key col  | `NULL_SPIKE`       | `null_spike`      | quarantine bad rows, load the valid remainder   |
| Retried extract         | `DUPLICATE_KEYS`   | `duplicate_keys`  | de-dupe on primary key, keep latest             |
| Partial extract (10%)   | `VOLUME_DROP`      | `volume_drop`     | re-extract the full batch from source           |
| Source also lost data   | `VOLUME_DROP` x N  | `volume_drop`     | circuit breaker opens after 3 heals -> escalate |

The load step is an idempotent upsert keyed on `order_id`, so re-runs after a
heal never double-load.

## Optional LLM advisor

Diagnosis is deterministic by default. If you set:

```bash
export HEALER_LLM_API_KEY=...            # any OpenAI-compatible endpoint
export HEALER_LLM_BASE_URL=...           # optional
export HEALER_LLM_MODEL=...              # optional
```

the healing loop asks the model to classify failures the rules cannot, and
falls back to escalation on any error. Nothing calls the network without the
env var, so the demo and CI stay fully offline.

## Layout

```
src/healer/
  config.py     thresholds, paths, circuit-breaker limit
  dataset.py    deterministic synthetic source data
  faults.py     the five failure injectors
  quality.py    the quality gates (schema / types / nulls / dupes / volume)
  pipeline.py   validate -> transform -> idempotent load (+ quarantine table)
  diagnose.py   rule-based root-cause classifier
  heal.py       playbooks + the self-healing loop with circuit breaker
  llm.py        optional OpenAI-compatible advisor
  incident.py   JSONL incident log
  demo.py       the narrated demo
tests/          19 pytest tests, one per check, diagnosis and playbook
airflow/dags/   production-style Airflow DAG wrapping the runner
spark/          PySpark + Delta Lake (MERGE) variant for Databricks
dbt/            staging + mart models with dbt tests for the warehouse layer
docs/           architecture notes
```

## Production mapping

- `airflow/dags/self_healing_orders_dag.py` runs the same `SelfHealingRunner`
  on a daily schedule and pushes the healing outcome to XCom for alerting.
- `spark/databricks_job.py` is the PySpark/Delta version: quality gates as
  DataFrame checks, quarantine as a Delta table, idempotent load as
  `MERGE INTO`, designed to run as a Databricks job.
- `dbt/` models the warehouse layer (`stg_orders` excludes quarantined rows,
  `fct_daily_revenue` aggregates) with `unique` / `not_null` /
  `accepted_values` tests, so dbt acts as the always-on safety net under the
  healing loop.

## Honest limitations

- The "detection" here is rule-based thresholds, not ML anomaly detection;
  baselines are configured, not learned.
- Playbooks fix the five failure classes listed above. Anything else
  escalates, which is deliberate: silent wrong data is worse than a page.
- The demo warehouse is SQLite for zero-setup; the Spark module shows the
  Delta Lake version of the same pattern.
- The LLM advisor is advisory-only: it can classify, it cannot apply fixes.

## Inspiration and credits

The pattern is assembled from well-known public engineering work:

- [Apache Airflow](https://airflow.apache.org) for orchestration and retry semantics
- [Great Expectations](https://greatexpectations.io) for declarative data-quality checks
- [Martin Fowler's CircuitBreaker](https://martinfowler.com/bliki/CircuitBreaker.html) for the escalation cap
- [Google's SRE book](https://sre.google/sre-book/table-of-contents/) for the "automate the toil, page a human when automation fails" philosophy
- Public engineering blogs from [Uber](https://www.uber.com/blog/engineering/), [Netflix](https://netflixtechblog.com), [Airbnb](https://medium.com/airbnb-engineering) and [Databricks](https://www.databricks.com/blog) on data quality at scale and Delta Lake patterns

## How this was built

Designed and written by Rahul Rathore with AI pair-programming assistance
(Instinct). Every claim in this README is backed by code in this repo: the
demo output above is real, and `pytest` reproduces every row of the healing
table. Nothing here was copied from the credited sources; they inspired the
architecture, not the code.

## License

MIT - see [LICENSE](LICENSE).
