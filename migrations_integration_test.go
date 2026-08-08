package cf_postgres

import (
	"context"
	"embed"
	"fmt"
	"testing"
	"testing/fstest"
	"time"

	cf "github.com/caerus-framework/caerus-framework"
)

// testMigrations mirrors the golang-migrate layout used by caerus-auth-api
// (multi-statement files, -- comments, seed INSERTs) so the integration test
// exercises the same real-world shape.
//
//go:embed testmigrations
var testMigrations embed.FS

// TestIntegrationMigrations verifies WithMigrations applies pending
// multi-statement migrations at Init, that a second startup is a no-op, and
// that the custom table name is honored.
func TestIntegrationMigrations(t *testing.T) {
	dsn, ok := testConnString()
	if !ok {
		t.Skip("POSTGRES_ADDR or POSTGRES_DSN not set; skipping integration test")
	}

	table := fmt.Sprintf("caerus_migrations_%d", time.Now().UnixNano())

	init := func(customTable string) *CFPostgres {
		t.Helper()
		p := New(
			WithConnString(dsn),
			WithEmbeddedMigrations(testMigrations, "testmigrations", WithMigrationsTable(customTable)),
			WithMigrateOnInit(),
			WithPingTimeout(3*time.Second),
		)
		fw := cf.New()
		if err := p.Init(context.Background(), fw); err != nil {
			t.Fatalf("Init: %v", err)
		}
		t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
		return p
	}

	// first startup: 2 migrations applied
	p := init(table)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// registered last so it runs first (pools still alive): leave the schema
	// clean for a subsequent run of this test
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = p.Pool().Exec(ctx, "DROP TABLE IF EXISTS widgets")
		_, _ = p.Pool().Exec(ctx, "DROP TABLE IF EXISTS "+table)
	})

	var version int
	var dirty bool
	if err := p.Pool().QueryRow(ctx, "SELECT version, dirty FROM "+table).Scan(&version, &dirty); err != nil {
		t.Fatalf("read migrations table: %v", err)
	}
	if version != 2 || dirty {
		t.Fatalf("migrations state = version %d dirty %v, want version 2 clean", version, dirty)
	}

	var name string
	if err := p.Pool().QueryRow(ctx, "SELECT name FROM widgets WHERE id = 1").Scan(&name); err != nil {
		t.Fatalf("query migrated table: %v", err)
	}
	if name != "widget-one" {
		t.Fatalf("widget name = %q, want widget-one (seed data applied)", name)
	}

	// second startup: already up to date must not fail
	p2 := init(table)
	var version2 int
	if err := p2.Pool().QueryRow(ctx, "SELECT version FROM "+table).Scan(&version2); err != nil {
		t.Fatalf("read migrations table on second startup: %v", err)
	}
	if version2 != 2 {
		t.Fatalf("version after second startup = %d, want 2", version2)
	}
}

// TestIntegrationMigrationsFailFast verifies a broken migration aborts Init
// (fail-fast) and leaves no usable pool.
func TestIntegrationMigrationsFailFast(t *testing.T) {
	dsn, ok := testConnString()
	if !ok {
		t.Skip("POSTGRES_ADDR or POSTGRES_DSN not set; skipping integration test")
	}

	broken := fstest.MapFS{
		"000001_broken.up.sql":   {Data: []byte("THIS IS NOT SQL;")},
		"000001_broken.down.sql": {Data: []byte("DROP TABLE IF EXISTS x;")},
	}
	p := New(
		WithConnString(dsn),
		WithMigrations(broken, WithMigrationsTable("caerus_broken_migrations")),
		WithMigrateOnInit(),
		WithPingTimeout(3*time.Second),
	)
	fw := cf.New()
	err := p.Init(context.Background(), fw)
	if err == nil {
		t.Fatal("Init with a broken migration should fail")
	}
	if pool := p.Pool(); pool != nil {
		t.Fatal("Pool() should be nil after failed Init")
	}
}
