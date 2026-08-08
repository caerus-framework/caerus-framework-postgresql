# caerus-framework-postgresql

[![CI](https://github.com/caerus-framework/caerus-framework-postgresql/actions/workflows/ci.yml/badge.svg)](https://github.com/caerus-framework/caerus-framework-postgresql/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/caerus-framework/caerus-framework-postgresql/graph/badge.svg)](https://codecov.io/gh/caerus-framework/caerus-framework-postgresql)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)


Caerus Framework PostgreSQL Component. Wraps a [pgx](https://github.com/jackc/pgx)
connection pool (`pgxpool`), verifies connectivity at `Init` (fail-fast), and
closes it at `Shutdown`. Requires the `data` stage to be registered.

## Not an ORM — the ops chassis for Postgres

This module owns the **connection & ops chassis**, not your queries. Write SQL
with [sqlc](https://sqlc.dev) or pgx against `Pool()` (and `WithinTx` for
transactions); this component owns everything around it:

- pool lifecycle: `Init` fail-fast ping, `Shutdown`, `Health` (`/readyz`)
- live reload / reconnect from a configuration source (`WithConfigSource`)
- migrate Job + CLI (`WithMigrations` / `Migrate` / framework job flag)
- day-2 ops: pool metrics, `WithQueryTracer` hooks, TLS file rotation, timeouts

Non-goals: ActiveRecord, auto-migrate from structs, `Repository[T]`, hiding
`Pool()`.

### Service checklist

For every new service using this component:

- [ ] `Health` wired — observability `/readyz` auto-discovers `HealthProvider`
- [ ] Schema via `WithMigrations` + `--postgresql.job=migrate` (or `WithEmbeddedMigrations`
      for `//go:embed` FS) as a K8s Job **before** the Deployment; serving pods omit `WithMigrateOnInit`
- [ ] `WithConfigSource("…")` for file-based config (External Secrets rotation)
- [ ] `WithName` + `GetByName` when a process needs primary + replica
- [ ] Confirm `postgresql_pool_*` on `/metrics` (pool saturation visible)

## Wiring

```go
package main

import (
	"context"
	"log/slog"
	"os"

	cf "github.com/caerus-framework/caerus-framework"
	cf_logs "github.com/caerus-framework/caerus-framework-logs"
	cf_postgres "github.com/caerus-framework/caerus-framework-postgresql"
)

func main() {
	fw := cf.New()

	logs := cf_logs.New(cf_logs.WithWriter(os.Stdout))
	if err := fw.AddComponent(logs); err != nil { // "logs" is a required dependency
		slog.Error("register logs", "err", err)
		os.Exit(1)
	}

	postgres := cf_postgres.New(
		cf_postgres.WithHost("127.0.0.1"),
		cf_postgres.WithPort(5432),
		cf_postgres.WithUser("svc"),
		cf_postgres.WithPassword("secret"),
		cf_postgres.WithDatabase("mydb"),
		cf_postgres.WithSSLMode("disable"),
	)
	app := NewMyApp(postgres) // any component with GetDependencies() -> []string{cf_postgres.ComponentName}
	if err := fw.AddComponent(postgres); err != nil {
		slog.Error("register postgres", "err", err)
		os.Exit(1)
	}
	if err := fw.AddComponent(app); err != nil {
		slog.Error("register app", "err", err)
		os.Exit(1)
	}

	if err := fw.Run(context.Background()); err != nil {
		slog.Error("startup failed", "err", err)
		os.Exit(1)
	}
}
```

## Usage

After `fw.Run` (or in any component whose stage runs after `data`), get the
pool and issue queries through the pgx API:

```go
pool := cf.MustGet[*cf_postgres.CFPostgres](fw).Pool()

var note string
err := pool.QueryRow(ctx, "SELECT note FROM notes WHERE id = $1", 42).Scan(&note)
```

The pool provides connection reuse, health-checked idle connections, and safe
concurrency.

### Transactions

Use `WithinTx` for multi-statement atomic operations — commit on `nil`,
rollback on error or a canceled context:

```go
pg := cf.MustGet[*cf_postgres.CFPostgres](fw)
err := pg.WithinTx(ctx, func(tx pgx.Tx) error {
    if _, err := tx.Exec(ctx, "INSERT INTO a (id) VALUES ($1)", 1); err != nil {
        return err
    }
    _, err := tx.Exec(ctx, "INSERT INTO b (id) VALUES ($1)", 1)
    return err
})
```

For more control (`pgx.TxOptions`, savepoints) use `pool.Begin(ctx)` and
`pgx.Tx` directly — `WithinTx` is the common case, not a transaction API.

## Options

| Option | Description |
| --- | --- |
| `WithConfig(PostgresConfig)` | connection config loaded from the configuration component; non-zero fields override option-set defaults |
| `WithPoolConfig(*pgxpool.Config)` | full pgxpool.Config (deep-copied); call before convenience setters you want overridden |
| `WithConnString(dsn)` | full connection string (DSN or URL), parsed via `pgxpool.ParseConfig`; bad DSN fails at `Init` |
| `WithHost(h)` | server host (default `127.0.0.1`) |
| `WithPort(p)` | server port (default `5432`) |
| `WithUser(u)` | role to connect as (default: current OS user) |
| `WithPassword(p)` | authentication password |
| `WithDatabase(d)` | database name (default: the user name) |
| `WithSSLMode(m)` | `disable`, `prefer`, `require`, `verify-ca`, `verify-full` (default `prefer`); unknown mode fails at `Init` |
| `WithApplicationName(n)` | sets the `application_name` runtime parameter |
| `WithStatementTimeout(d)` | per-connection `statement_timeout` runtime parameter (ms); 0 = unset |
| `WithLockTimeout(d)` | per-connection `lock_timeout` runtime parameter (ms); 0 = unset |
| `WithTLSRootCAFile(path)` | PEM CA bundle to verify the server (mTLS/private CA); re-read on every connect/reload |
| `WithTLSClientCertFile(cert, key)` | PEM client cert + key for mTLS; re-read on every connect/reload |
| `WithMaxConns(n)` | pool max connections (default `max(4, GOMAXPROCS)`) |
| `WithMinConns(n)` | pool min connections (default `0`) |
| `WithMaxConnLifetime(d)` | max connection age (default `1h`) |
| `WithMaxConnIdleTime(d)` | max idle time (default `30m`) |
| `WithHealthCheckPeriod(d)` | idle health-check interval (default `1m`) |
| `WithConnectTimeout(d)` | per-attempt connect timeout (default `0` = none, libpq default) |
| `WithPingTimeout(d)` | Init connectivity-ping timeout (default `5s`) |
| `WithMigrations(fsys, opts...)` | configure a migration FS (already rooted at the `.up.sql`/`.down.sql` dir) for `Migrate` / the framework job flag |
| `WithEmbeddedMigrations(fsys, dir, opts...)` | like `WithMigrations` but takes the `//go:embed` FS + directory and resolves the sub-FS internally (mismatched dir panics at construction) |
| `WithMigrateOnInit()` | `Init` calls `Migrate` (local/single-replica only) |
| `WithMigrationsTable(name)` | migrations tracking table (default `schema_migrations`) |
| `WithName(name)` | custom component name for multiple instances (default `"postgresql"`) |
| `WithQueryTracer(pgx.QueryTracer)` | pgx query tracer around every Query/QueryRow/Exec; survives pool rebuilds on config reload |
| `WithLogger(*slog.Logger)` | explicit logger override; defaults to the framework `logs` component's logger (re-delivered on `logs` `Reconfigure`), falling back to `slog.Default()` |

## Query tracing

`WithQueryTracer` exposes pgx's own hook seam (`TraceQueryStart` /
`TraceQueryEnd`, invoked around every `Query`, `QueryRow`, and `Exec`), so you
can attach tracing or slow-query logging without importing an instrumentation
library into this module — the tracer interface lives in pgx. An OpenTelemetry
example (the otel import lives in **your** app, not here):

```go
import (
    "context"

    "github.com/jackc/pgx/v5"
    "go.opentelemetry.io/otel"
    "go.opentelemetry.io/otel/trace"
)

type ctxKey struct{}

type spanTracer struct{ tracer string }

func (t spanTracer) TraceQueryStart(ctx context.Context, conn *pgx.Conn,
    data pgx.TraceQueryStartData) context.Context {
    ctx, span := otel.Tracer(t.tracer).Start(ctx, "postgres:"+data.SQL)
    return context.WithValue(ctx, ctxKey{}, span)
}

func (t spanTracer) TraceQueryEnd(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryEndData) {
    span, ok := ctx.Value(ctxKey{}).(trace.Span)
    if !ok {
        return
    }
    if data.Err != nil {
        span.RecordError(data.Err)
    }
    span.End()
}

p := cf_postgres.New(cf_postgres.WithConnString("postgres://..."),
    cf_postgres.WithQueryTracer(spanTracer{tracer: "postgres"}))
```

The tracer is part of the option base config, so a `WithConfigSource` reload
rebuilds the pool with the same tracer. Pgx calls a single tracer per query;
chain several with a small composite type. `BatchTracer` / `ConnectTracer` /
pool acquire tracing are secondary and not exposed — use
`WithPoolConfig` if you need them.

## Multiple instances

Use `WithName` to run multiple postgres clients in the same process (e.g., primary and replica):

```go
primary := cf_postgres.New(
    cf_postgres.WithName("primary"),
    cf_postgres.WithHost("primary.db.example.com"),
    cf_postgres.WithDatabase("mydb"),
)
replica := cf_postgres.New(
    cf_postgres.WithName("replica"),
    cf_postgres.WithHost("replica.db.example.com"),
    cf_postgres.WithDatabase("mydb"),
)

fw.AddComponent(primary)
fw.AddComponent(replica)

// Retrieve by name
primaryPool := cf.MustGetByName[*cf_postgres.CFPostgres](fw, "primary").Pool()
replicaPool := cf.MustGetByName[*cf_postgres.CFPostgres](fw, "replica").Pool()
```

When multiple instances exist, `cf.Get[*cf_postgres.CFPostgres](fw)` returns `false` to prevent ambiguous lookups. Always use `GetByName` for named instances.

## Migrations

Files use the [golang-migrate](https://github.com/golang-migrate/migrate)
layout (`<version>_<name>.up.sql` / `.down.sql`), may contain multiple
statements, and run under an advisory lock against the same pool TLS/credentials
as the app. "Already up to date" is success.

### Production policy (Job flag, not Init)

| Environment | Pattern |
|---|---|
| **K8s / multi-replica** | Same image; Job args `--postgresql.job=migrate` (the flag is **declared by this module** on its own configuration source — see [Same binary: `--postgresql.job=migrate`](#same-binary---postgresqljobmigrate)). Serving Deployment **omits** `WithMigrateOnInit` (keep `WithMigrations` so the Job can migrate). |
| **Local / single-replica** | `WithMigrations` + `WithMigrateOnInit()` so `Init` migrates after ping. |

Do **not** use `WithMigrateOnInit` on every replica of a Deployment. See
[Dirty migrations](#dirty-migrations) when a Job crashes mid-apply.

### Dirty migrations

golang-migrate tracks `(version, dirty)` in the migrations table (default
`schema_migrations`). While an up is in progress the row is **dirty**. If the
process dies mid-version, further `Up` calls fail until an operator repairs
state — Caerus **does not** auto-`force`.

| Situation | What happens |
|---|---|
| Concurrent migrate Jobs / Init | Serialized by golang-migrate’s **Postgres advisory lock** on that database |
| Job re-run when already current | Success (`ErrNoChange` / “up to date”) — safe and expected |
| Crash mid-version | Dirty set → later migrate Jobs fail closed until repaired |

**Why no auto-force:** dirty means “we do not know whether version *N*’s SQL
finished.” Clearing the flag without inspecting the database can mark a
half-applied schema as done. Silent heal is how replicas diverge.

**Operator runbook (default):**

1. Fail the release / stop serving until fixed (Job failed; Deployment should
   not have started).
2. Inspect DB + Job logs: did objects from version *N* exist?
3. Either finish the up manually, or undo partial objects (hand SQL / careful
   use of the matching `.down.sql`). golang-migrate does **not** auto-run downs
   on crash.
4. `migrate force VERSION` to the **true** clean version (clears dirty).
5. Re-run `myapp --postgresql.job=migrate` (or `--postgresql.<name>.job=migrate`).

**Reducing how often this hurts:** keep versions small; prefer expand/contract
and idempotent ups; avoid irreversible DDL in the middle of a multi-statement
version. Prod rollback of a bad up is usually restore/PITR or a forward fix,
not a casual `Down`.

**Not in this module:** automatic dirty policies (`force` to *N* or *N−1* and
retry). If a product ever adds that, it must be an explicit opt-in with a
documented assumption that ups are safe to retry — never the default `Migrate`
path.

### Same binary: `--postgresql.job=migrate`

Wire postgres once with `WithMigrations`. The module declares the job flag on
its own configuration source (`Source.Job: cf.JobSpec{Flag: "postgresql.job",
Tasks: ["migrate"]}`), so configuration parses/validates the value like any
other knob. The **flag names the instance** and the **value names the task**
(`--postgresql.job=migrate`, or `--postgresql.orders.job=migrate` for a
`WithName("orders")` instance). Jobs are **CLI-only** — the task never flows
from env or file. `RunWithSignals` asks configuration (via `cf.JobSource`) and
runs only the core plus the **named** postgres component's `RunJob` (`migrate`
→ `Migrate`), then exits — no serve ceremony:

```go
import "embed"

//go:embed migrations
var migrations embed.FS

func main() {
	ctx := context.Background()

	fw := cf.New(&cf.FrameworkOptions{
		Components: []cf.CaerusComponent{
			cf_postgres.New(
				cf_postgres.WithConfigSource("postgresql", "config.yaml"),
				cf_postgres.WithEmbeddedMigrations(migrations, "migrations"),
				// cf_postgres.WithMigrateOnInit(), // local only
			),
			// … logs/configuration/observability are auto-registered core …
			app, // Runnable
		},
	})

	if err := fw.RunWithSignals(ctx,
		cf.WithShutdownTimeout(15*time.Second),
	); err != nil {
		log.Fatal(err)
	}
}
```

Run it as `myapp --postgresql.job=migrate` (K8s Job), or call
`fw.Migrate(ctx, "postgresql")` directly from a migrate subcommand in a
multi-tool binary.

| Invocation | Behavior |
|---|---|
| `myapp --postgresql.job=migrate` | `Initialize` (core + named postgres) → `RunJob("migrate")` → `Shutdown` → exit |
| `myapp --postgresql.orders.job=migrate` | Same, on the `WithName("orders")` instance |
| `myapp` (serve) | Normal `RunWithSignals`; no migrate unless `WithMigrateOnInit` |

### `Migrate(ctx)` (explicit API)

```go
if err := postgres.Migrate(ctx); err != nil {
	log.Fatal(err)
}
```

Requires `Init` (live pool) and a migrations FS (`WithMigrations` or
`WithEmbeddedMigrations`). Prefer the framework job flag
(`--postgresql.job=migrate`) or `fw.Migrate(ctx, target)` for Jobs.

### Helm / GitOps Job before Deployment

- `batch/v1` Job (or Helm `pre-install`/`pre-upgrade` hook, or Argo sync-wave `-1`).
- Same container image; `args: ["--postgresql.job=migrate"]` (or equivalent).
- Same DB secret/config as the API.
- `restartPolicy: Never`; fail the release if the Job fails.
- Deployment pods start only after the Job succeeds; readiness is `/readyz`.

Migration files under `fsys`:

```
migrations/
  000001_init.up.sql
  000001_init.down.sql
```

Shared databases: give each service its own tracking table via
`WithMigrationsTable` (e.g. `orders_migrations`). Default is `schema_migrations`.

## Configuration

Drive connection settings via `caerus-framework-configuration` (file → env →
DSN). `PostgresConfig` has json/yaml/`env` tags. Durations are in seconds.

The module is **self-sufficient**: `WithConfigSource(name, path)` registers its
own `Source[PostgresConfig]` with the configuration component (via
`cf.ConfigSourceRegistrar`, run by the framework during argv absorption). The
default `EnvPrefix` is the uppercase source name (`"postgresql"` →
`"POSTGRESQL_"`); override with `WithSourceEnvPrefix("POSTGRES_")`. `POSTGRES_DSN`
is overlaid in `AfterLoad` inside the module. `main` only points the instance at
where config lives:

```go
postgres := cf_postgres.New(
	cf_postgres.WithConfigSource("postgresql", "config.yaml"), // Init + OnConfigReload reconnect
)
```

For low-level control (custom `AfterLoad`, format, env prefix), register the
source manually instead:

```go
conf := cf_configuration.New()
_ = fw.AddComponent(conf)
_ = cf_configuration.AddSource(conf, cf_configuration.Source[cf_postgres.PostgresConfig]{
	Name:      "postgresql",
	Path:      "config.yaml", // optional if EnvPrefix set
	Format:    cf_configuration.FormatYAML,
	Owner:     cf_postgres.ComponentName,
	EnvPrefix: "POSTGRES_",
	AfterLoad: func(c *cf_postgres.PostgresConfig) error {
		if dsn := os.Getenv("POSTGRES_DSN"); dsn != "" {
			return cf_postgres.OverlayDSN(c, dsn) // wins over file+env fields
		}
		return nil
	},
})

postgres := cf_postgres.New(
	cf_postgres.WithConfigSource("postgresql", ""), // bind by name only
)
```

Helpers: `ParseDSN` / `OverlayDSN` for `postgres://` URLs and keyword DSNs.
`WithConfigSource` implements `ConfigReloader`: on file reload (or
`cfg.Reload`), builds a new pool, pings, swaps, closes the old pool; on failure
keeps the previous pool. In Kubernetes prefer file-mounted secrets for
rotation; use env/DSN for local and CI.

## Fail-fast behaviour

`Init` creates the pool, pings the server, and applies pending migrations. If
the connection is refused, the ping times out, or a migration fails, `Init`
returns an error and startup aborts before any dependent component runs.
`Pool()` returns `nil` before `Init` or after `Shutdown`.

## Observability

`CFPostgres` implements `cf.HealthProvider`: `Health(ctx)` pings the pool, so
the `observability` component's `/readyz` endpoint reflects real database
connectivity. Before `Init` or after `Shutdown` (nil pool) it reports unhealthy.

It also implements `cf.MetricsProvider`: while connected it contributes
`postgresql_info` plus pool gauges and counters to `/metrics` (all
labeled `database`, `user`, `host`, `port`, `component` = `Name()`):

| Metric | Type | Meaning |
|---|---|---|
| `postgresql_pool_idle` | gauge | idle connections |
| `postgresql_pool_total` | gauge | total connections |
| `postgresql_pool_max` | gauge | pool maximum |
| `postgresql_pool_acquired` | gauge | currently acquired |
| `postgresql_pool_acquire_total` | counter | cumulative successful acquires |
| `postgresql_pool_empty_acquire_total` | counter | acquires that had to wait |
| `postgresql_pool_canceled_acquire_total` | counter | acquires canceled by context |

Scrape `postgresql_pool_acquired / max` for **pool saturation** — the
day-2 signal this module is for. Before `Init` or after `Shutdown` it reports
nothing (lazy pickup).

## Tests

Unit tests cover the component contract without a server. Integration tests are
gated on `POSTGRES_ADDR` (or `POSTGRES_DSN` for full control):

```
POSTGRES_ADDR=127.0.0.1:5433 POSTGRES_PASSWORD=secret go test -race ./...
```

## License

Apache License 2.0 — see [LICENSE](LICENSE).
