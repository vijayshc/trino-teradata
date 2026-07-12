# Stress testing suite

Standalone tools for concurrent load and bottleneck analysis against a **live**
Trino cluster with the Teradata Direct connector installed.

These are **not** unit tests and are not run by `./mvnw verify` by default.

## Contents

| Artifact | Role |
|----------|------|
| `StressTestRunner` | Sustained concurrent Trino queries, latency percentiles, stuck-query detection |
| `BottleneckAnalyzer` | Connection pool, thread saturation, data volume, escalation |
| `teradata_stress_test.sh` | Optional BTEQ baseline on Teradata (requires `bteq` + credentials) |

## Prerequisites

- Deployed connector and catalog (e.g. `tdexport`)
- Fixture tables from `testing/setup_*.bteq` (for default `StressTestRunner` queries)
- JDK matching the project (see root `pom.xml`)
- `TRINO_JDBC_URL` / `TRINO_USER` (see `dev/env.example`)

## Run (recommended)

From the repository root:

```bash
cp dev/env.example dev/local.env   # set TRINO_JDBC_URL, TRINO_USER

./scripts/run_stress.sh quick
./scripts/run_stress.sh test --concurrency 50 --duration 120
./scripts/run_stress.sh analyze
./scripts/run_stress.sh analyze connection
./scripts/run_stress.sh escalate
./scripts/run_stress.sh heavy

# Optional Teradata-native baseline
export TD_HOST=... TD_LOGON_USER=... TD_LOGON_PASSWORD=...
./scripts/run_stress.sh bteq 10 60
```

## Build only

```bash
./mvnw -pl testing/trino-teradata-stress -am package -DskipTests
```

Output:

```text
testing/trino-teradata-stress/target/trino-teradata-stress-*.jar
testing/trino-teradata-stress/target/lib/   # runtime deps (trino-jdbc)
```

## StressTestRunner options

| Flag | Default | Meaning |
|------|---------|---------|
| `--concurrency N` | 10 | Worker threads |
| `--duration N` | 60 | Seconds |
| `--ramp-up N` | 5 | Gradual start |
| `--query-type` | mixed | `small` \| `medium` \| `large` \| `mixed` |
| `--url` | env / localhost | JDBC URL |
| `--user` | env / `trino` | Trino user |

Default queries hit `trinoexport.test_join_dim` (5 rows). Edit the runner or
pass a custom environment with matching fixtures for other workloads.

## Interpreting results

- **P99 ≫ P50** — queueing or thread pool contention on bridge / Trino
- **High failure rate** — timeouts, pool exhaustion, or data-plane errors
- **BTEQ fast, Trino stress slow** — connector / bridge path
- **Both slow** — Teradata or network capacity

Do not commit secrets; use `dev/local.env` (gitignored).
