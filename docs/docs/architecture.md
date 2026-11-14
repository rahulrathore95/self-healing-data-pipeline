# Architecture

```
            +--------------------+
            |  source extract    |
            +---------+----------+
                      v
            +--------------------+
            |  quality gates     |   schema / types / nulls / dupes / volume
            |  (fail-fast)       |
            +---------+----------+
                 ok   |   issues
            +---------+----------+
            |                   v
            v           +--------------------+
     transform +        |  diagnose          |   rules first, optional LLM advisor
     idempotent load    +---------+----------+
                                  v
                        +--------------------+
                        |  playbook registry |   remap / strip / quarantine /
                        +---------+----------+   dedupe / re-extract
                                  v
                          re-run the pipeline
                                  |
                     attempts > max? ---- yes ----> ESCALATE
                     (circuit breaker)              (incident log -> human)
```

## Design decisions

- **Fail-fast validation before load.** Bad data never reaches the warehouse,
  which keeps the healing loop's signal clean and the warehouse trustworthy.
- **Playbooks, not general-purpose auto-fix.** Each remediation encodes one
  deliberate decision (quarantine vs drop, dedupe keep-latest, re-extract vs
  impute). Unknown failures escalate. Silent wrong data is worse than a page.
- **Idempotent load.** The upsert keyed on `order_id` means a healed re-run is
  always safe, which is what makes automatic retry acceptable.
- **Circuit breaker.** `max_heal_attempts` caps the loop so a genuinely broken
  source (which re-extraction cannot fix) escalates instead of looping.
- **Deterministic by default.** The LLM advisor only classifies failures the
  rules miss, and only when an API key is configured; it can never apply fixes.
- **Incident log as the audit trail.** Every failure, diagnosis and action is
  JSONL, so post-mortems and dashboards read the same file the loop writes.
