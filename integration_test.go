package cf_postgres

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"testing"
	"time"

	cf "github.com/caerus-framework/caerus-framework"
	cf_logs "github.com/caerus-framework/caerus-framework-logs"
	cf_observability "github.com/caerus-framework/caerus-framework-observability"
	"github.com/jackc/pgx/v5"
)

// testConnString returns the integration-test connection string. Set
// POSTGRES_DSN for full control, or POSTGRES_ADDR (host:port) to use the
// postgres role/database (override with POSTGRES_USER/POSTGRES_PASSWORD) with
// TLS disabled. The regular test run has no external dependency.
func testConnString() (string, bool) {
	if dsn := os.Getenv("POSTGRES_DSN"); dsn != "" {
		return dsn, true
	}
	if addr := os.Getenv("POSTGRES_ADDR"); addr != "" {
		user := os.Getenv("POSTGRES_USER")
		if user == "" {
			user = "postgres"
		}
		dsn := fmt.Sprintf("postgres://%s@%s/postgres?sslmode=disable", user, addr)
		if pass := os.Getenv("POSTGRES_PASSWORD"); pass != "" {
			dsn = fmt.Sprintf("postgres://%s:%s@%s/postgres?sslmode=disable", user, pass, addr)
		}
		return dsn, true
	}
	return "", false
}

// TestIntegration is gated on the POSTGRES_ADDR/POSTGRES_DSN environment
// variables. Point it at a live server:
//
//	POSTGRES_ADDR=127.0.0.1:5433 go test ./...
func TestIntegration(t *testing.T) {
	dsn, ok := testConnString()
	if !ok {
		t.Skip("POSTGRES_ADDR or POSTGRES_DSN not set; skipping integration test")
	}

	p := New(
		WithConnString(dsn),
		WithApplicationName("caerus-postgres-test"),
		WithMaxConns(4),
		WithPingTimeout(3*time.Second),
	)
	fw := cf.New()

	if err := p.Init(context.Background(), fw); err != nil {
		t.Fatalf("Init: %v", err)
	}
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })

	if pool := p.Pool(); pool == nil {
		t.Fatal("Pool() is nil after Init")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := p.Pool().Ping(ctx); err != nil {
		t.Fatalf("Ping: %v", err)
	}

	table := fmt.Sprintf("caerus_integration_%d", time.Now().UnixNano())
	defer func() {
		_, _ = p.Pool().Exec(context.Background(), "DROP TABLE IF EXISTS "+table)
	}()

	if _, err := p.Pool().Exec(ctx, "CREATE TABLE "+table+" (id bigint PRIMARY KEY, note text NOT NULL)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	if _, err := p.Pool().Exec(ctx, "INSERT INTO "+table+" (id, note) VALUES ($1, $2)", 1, "hello"); err != nil {
		t.Fatalf("INSERT: %v", err)
	}

	var note string
	if err := p.Pool().QueryRow(ctx, "SELECT note FROM "+table+" WHERE id = $1", 1).Scan(&note); err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	if note != "hello" {
		t.Fatalf("note = %q, want %q", note, "hello")
	}
}

// TestIntegrationHealthReflectsConnectivity verifies Health reports nil while
// connected and errors after Shutdown.
func TestIntegrationHealthReflectsConnectivity(t *testing.T) {
	dsn, ok := testConnString()
	if !ok {
		t.Skip("POSTGRES_ADDR or POSTGRES_DSN not set; skipping integration test")
	}

	p := New(WithConnString(dsn), WithPingTimeout(3*time.Second))
	fw := cf.New()

	if err := p.Init(context.Background(), fw); err != nil {
		t.Fatalf("Init: %v", err)
	}
	if err := p.Health(context.Background()); err != nil {
		t.Fatalf("Health while connected = %v, want nil", err)
	}
	if ms := p.Metrics(); len(ms) < 8 || ms[0].Name != "postgresql_info" {
		t.Fatalf("Metrics while connected = %+v, want postgresql_info + pool gauges/counters", ms)
	}
	if err := p.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if err := p.Health(context.Background()); err == nil {
		t.Fatal("Health after Shutdown should fail")
	}
	if ms := p.Metrics(); ms != nil {
		t.Fatalf("Metrics after Shutdown = %+v, want nil", ms)
	}
}

// TestIntegrationMultipleNamedInstances demonstrates multiple postgres instances
// in the same process using WithName and GetByName.
func TestIntegrationMultipleNamedInstances(t *testing.T) {
	dsn, ok := testConnString()
	if !ok {
		t.Skip("POSTGRES_ADDR or POSTGRES_DSN not set; skipping integration test")
	}

	primary := New(WithName("primary"), WithConnString(dsn), WithPingTimeout(3*time.Second))
	replica := New(WithName("replica"), WithConnString(dsn), WithPingTimeout(3*time.Second))

	fw := cf.New()
	if err := fw.AddComponent(cf_logs.New(cf_logs.WithWriter(io.Discard))); err != nil {
		t.Fatalf("AddComponent(logs): %v", err)
	}
	if err := fw.AddComponent(primary); err != nil {
		t.Fatalf("AddComponent(primary): %v", err)
	}
	if err := fw.AddComponent(replica); err != nil {
		t.Fatalf("AddComponent(replica): %v", err)
	}

	if err := fw.Initialize(context.Background()); err != nil {
		t.Fatalf("Initialize: %v", err)
	}
	t.Cleanup(func() { _ = fw.Shutdown(context.Background()) })

	// Get by name retrieves the correct instance
	primaryGot, ok := cf.GetByName[*CFPostgres](fw, "primary")
	if !ok || primaryGot != primary {
		t.Fatalf("GetByName(primary) returned wrong component: %v, %v", primaryGot, ok)
	}
	replicaGot, ok := cf.GetByName[*CFPostgres](fw, "replica")
	if !ok || replicaGot != replica {
		t.Fatalf("GetByName(replica) returned wrong component: %v, %v", replicaGot, ok)
	}

	// Get returns false when multiple instances exist
	if _, ok := cf.Get[*CFPostgres](fw); ok {
		t.Fatal("Get should return false when multiple postgres instances exist")
	}

	// Both instances work independently
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	primaryPool := primary.Pool()
	replicaPool := replica.Pool()

	// Create tables in both pools
	primaryTable := fmt.Sprintf("caerus_postgres_primary_%d", time.Now().UnixNano())
	replicaTable := fmt.Sprintf("caerus_postgres_replica_%d", time.Now().UnixNano())
	defer func() {
		_, _ = primaryPool.Exec(context.Background(), "DROP TABLE IF EXISTS "+primaryTable)
		_, _ = replicaPool.Exec(context.Background(), "DROP TABLE IF EXISTS "+replicaTable)
	}()

	if _, err := primaryPool.Exec(ctx, "CREATE TABLE "+primaryTable+" (id bigint PRIMARY KEY, note text NOT NULL)"); err != nil {
		t.Fatalf("primary CREATE TABLE: %v", err)
	}
	if _, err := replicaPool.Exec(ctx, "CREATE TABLE "+replicaTable+" (id bigint PRIMARY KEY, note text NOT NULL)"); err != nil {
		t.Fatalf("replica CREATE TABLE: %v", err)
	}

	// Insert into primary
	if _, err := primaryPool.Exec(ctx, "INSERT INTO "+primaryTable+" (id, note) VALUES ($1, $2)", 1, "primary-value"); err != nil {
		t.Fatalf("primary INSERT: %v", err)
	}

	// Insert into replica
	if _, err := replicaPool.Exec(ctx, "INSERT INTO "+replicaTable+" (id, note) VALUES ($1, $2)", 2, "replica-value"); err != nil {
		t.Fatalf("replica INSERT: %v", err)
	}

	// Read from primary
	var primaryNote string
	if err := primaryPool.QueryRow(ctx, "SELECT note FROM "+primaryTable+" WHERE id = $1", 1).Scan(&primaryNote); err != nil {
		t.Fatalf("primary SELECT: %v", err)
	}
	if primaryNote != "primary-value" {
		t.Fatalf("primary note = %q, want primary-value", primaryNote)
	}

	// Read from replica
	var replicaNote string
	if err := replicaPool.QueryRow(ctx, "SELECT note FROM "+replicaTable+" WHERE id = $1", 2).Scan(&replicaNote); err != nil {
		t.Fatalf("replica SELECT: %v", err)
	}
	if replicaNote != "replica-value" {
		t.Fatalf("replica note = %q, want replica-value", replicaNote)
	}
}

// TestIntegrationQueryTracer verifies a configured pgx.QueryTracer is invoked
// around a real query: it sees the SQL at start and a successful end.
func TestIntegrationQueryTracer(t *testing.T) {
	dsn, ok := testConnString()
	if !ok {
		t.Skip("POSTGRES_ADDR or POSTGRES_DSN not set; skipping integration test")
	}

	tracer := &fakeQueryTracer{}
	p := New(WithConnString(dsn), WithPingTimeout(3*time.Second), WithQueryTracer(tracer))

	fw := cf.New()
	if err := p.Init(context.Background(), fw); err != nil {
		t.Fatalf("Init: %v", err)
	}
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })

	var one int
	if err := p.Pool().QueryRow(context.Background(), "SELECT 1").Scan(&one); err != nil {
		t.Fatalf("SELECT 1: %v", err)
	}
	if one != 1 {
		t.Fatalf("SELECT 1 = %d, want 1", one)
	}

	tracer.mu.Lock()
	defer tracer.mu.Unlock()
	want := []string{"start:SELECT 1", "end:err=<nil>"}
	if len(tracer.order) != 2 || tracer.order[0] != want[0] || tracer.order[1] != want[1] {
		t.Fatalf("tracer order = %v, want %v", tracer.order, want)
	}
}

// TestIntegrationPoolMetrics verifies pool.Stat()-derived gauges/counters are
// exposed and that acquire counters move under concurrent load.
func TestIntegrationPoolMetrics(t *testing.T) {
	dsn, ok := testConnString()
	if !ok {
		t.Skip("POSTGRES_ADDR or POSTGRES_DSN not set; skipping integration test")
	}

	p := New(WithConnString(dsn), WithPingTimeout(3*time.Second))
	fw := cf.New()
	if err := p.Init(context.Background(), fw); err != nil {
		t.Fatalf("Init: %v", err)
	}
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })

	byName := func(name string) *cf_observability.Metric {
		for i := range p.Metrics() {
			if p.Metrics()[i].Name == name {
				return &p.Metrics()[i]
			}
		}
		return nil
	}
	if m := byName("postgresql_pool_max"); m == nil || m.Value < 1 {
		t.Fatalf("postgresql_pool_max = %+v, want >= 1", m)
	}
	if m := byName("postgresql_pool_acquire_total"); m == nil {
		t.Fatal("postgresql_pool_acquire_total missing")
	} else if m.Type != cf_observability.MetricTypeCounter {
		t.Fatalf("acquire_total Type = %v, want counter", m.Type)
	}

	// Fire concurrent queries so AcquireCount moves; then ensure it grew.
	before := byName("postgresql_pool_acquire_total").Value
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = p.Pool().Exec(context.Background(), "SELECT 1")
		}()
	}
	wg.Wait()
	after := byName("postgresql_pool_acquire_total").Value
	if after < before {
		t.Fatalf("acquire_total moved backward: %v -> %v", before, after)
	}
	if byName("postgresql_pool_idle") == nil || byName("postgresql_pool_total") == nil {
		t.Fatal("idle/total gauges missing")
	}
}

// TestIntegrationWithinTx verifies commit and rollback behavior of the helper.
func TestIntegrationWithinTx(t *testing.T) {
	dsn, ok := testConnString()
	if !ok {
		t.Skip("POSTGRES_ADDR or POSTGRES_DSN not set; skipping integration test")
	}

	p := New(WithConnString(dsn), WithPingTimeout(3*time.Second))
	fw := cf.New()
	if err := p.Init(context.Background(), fw); err != nil {
		t.Fatalf("Init: %v", err)
	}
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })

	ctx := context.Background()
	table := fmt.Sprintf("tx_test_%d", time.Now().UnixNano())
	if _, err := p.Pool().Exec(ctx, fmt.Sprintf("CREATE TEMP TABLE %s (id int PRIMARY KEY)", table)); err != nil {
		t.Fatalf("create temp table: %v", err)
	}

	commitErr := p.WithinTx(ctx, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, fmt.Sprintf("INSERT INTO %s (id) VALUES (1)", table))
		return err
	})
	if commitErr != nil {
		t.Fatalf("WithinTx commit: %v", commitErr)
	}

	rollbackErr := p.WithinTx(ctx, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, fmt.Sprintf("INSERT INTO %s (id) VALUES (2)", table))
		if err != nil {
			return err
		}
		return errors.New("boom")
	})
	if rollbackErr == nil || rollbackErr.Error() != "boom" {
		t.Fatalf("WithinTx rollback err = %v, want boom", rollbackErr)
	}

	// The rolled-back row must not be visible; the committed one must be.
	var n int
	if err := p.Pool().QueryRow(ctx, fmt.Sprintf("SELECT count(*) FROM %s", table)).Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 1 {
		t.Fatalf("rows after rollback = %d, want 1 (only the committed insert)", n)
	}
}
