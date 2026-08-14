package cf_postgres

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"testing/fstest"
	"time"

	cf "github.com/caerus-framework/caerus-framework"
	cf_logs "github.com/caerus-framework/caerus-framework-logs"
	cf_observability "github.com/caerus-framework/caerus-framework-observability"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// writeTestCAPEM generates a throwaway self-signed CA certificate at path and
// returns the path. Used to exercise TLS file loading without a real server.
func writeTestCAPEM(t *testing.T, path string) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("rsa key: %v", err)
	}
	tmpl := x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, &tmpl, &tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create cert: %v", err)
	}
	pemBytes := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	if err := os.WriteFile(path, pemBytes, 0o600); err != nil {
		t.Fatalf("write ca.pem: %v", err)
	}
}

func newFramework(t *testing.T) *cf.CaerusFramework {
	t.Helper()
	fw := cf.New()
	return fw
}

func TestComponentContract(t *testing.T) {
	p := New()
	if p.Name() != ComponentName {
		t.Fatalf("Name() = %q, want %q", p.Name(), ComponentName)
	}
	if p.GetInitOrderStage() != ComponentStage {
		t.Fatalf("GetInitOrderStage() = %q, want %q", p.GetInitOrderStage(), ComponentStage)
	}
	var _ cf.CaerusComponent = p

	if pool := p.Pool(); pool != nil {
		t.Fatal("Pool() should be nil before Init")
	}
	if err := p.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown before Init: %v", err)
	}
}

func TestHealthBeforeInit(t *testing.T) {
	p := New()
	if err := p.Health(context.Background()); err == nil {
		t.Fatal("Health before Init should fail")
	}
	if ms := p.Metrics(); ms != nil {
		t.Fatalf("Metrics before Init = %+v, want nil", ms)
	}
	var _ cf.HealthProvider = p
	var _ cf_observability.MetricsProvider = p
}

func TestNewDefaults(t *testing.T) {
	p := New()
	cc := p.poolConfig.ConnConfig
	if cc.Host != "127.0.0.1" {
		t.Fatalf("default Host = %q, want 127.0.0.1", cc.Host)
	}
	if cc.Port != 5432 {
		t.Fatalf("default Port = %d, want 5432", cc.Port)
	}
	if cc.ConnectTimeout != 0 {
		t.Fatalf("default ConnectTimeout = %v, want 0 (no timeout, libpq default)", cc.ConnectTimeout)
	}
	if p.pingTimeout != 5*time.Second {
		t.Fatalf("default pingTimeout = %v, want 5s", p.pingTimeout)
	}
}

func TestNewOptions(t *testing.T) {
	p := New(
		WithConnString("postgres://u1:p1@old:9999/db1"),
		WithHost("newhost"),
		WithUser("u2"),
		WithPassword("p2"),
		WithDatabase("db2"),
		WithMaxConns(11),
		WithMinConns(2),
		WithMaxConnLifetime(90*time.Second),
		WithMaxConnIdleTime(91*time.Second),
		WithHealthCheckPeriod(92*time.Second),
		WithConnectTimeout(93*time.Second),
	)
	cc := p.poolConfig.ConnConfig
	if cc.Host != "newhost" {
		t.Fatalf("Host = %q, want newhost", cc.Host)
	}
	if cc.User != "u2" {
		t.Fatalf("User = %q, want u2", cc.User)
	}
	if cc.Password != "p2" {
		t.Fatalf("Password = %q, want p2", cc.Password)
	}
	if cc.Database != "db2" {
		t.Fatalf("Database = %q, want db2", cc.Database)
	}
	if p.poolConfig.MaxConns != 11 || p.poolConfig.MinConns != 2 {
		t.Fatalf("pool conns = %d/%d, want 11/2", p.poolConfig.MaxConns, p.poolConfig.MinConns)
	}
	if p.poolConfig.MaxConnLifetime != 90*time.Second {
		t.Fatalf("MaxConnLifetime = %v, want 90s", p.poolConfig.MaxConnLifetime)
	}
	if p.poolConfig.MaxConnIdleTime != 91*time.Second {
		t.Fatalf("MaxConnIdleTime = %v, want 91s", p.poolConfig.MaxConnIdleTime)
	}
	if p.poolConfig.HealthCheckPeriod != 92*time.Second {
		t.Fatalf("HealthCheckPeriod = %v, want 92s", p.poolConfig.HealthCheckPeriod)
	}
	if cc.ConnectTimeout != 93*time.Second {
		t.Fatalf("ConnectTimeout = %v, want 93s", cc.ConnectTimeout)
	}
}

func TestWithName(t *testing.T) {
	// Default name
	p1 := New()
	if p1.Name() != ComponentName {
		t.Fatalf("default Name() = %q, want %q", p1.Name(), ComponentName)
	}

	// Custom name
	p2 := New(WithName("primary"))
	if p2.Name() != "primary" {
		t.Fatalf("custom Name() = %q, want primary", p2.Name())
	}

	// Multiple instances with different names
	p3 := New(WithName("replica"))
	if p3.Name() != "replica" {
		t.Fatalf("custom Name() = %q, want replica", p3.Name())
	}
}

func TestWithPoolConfigThenSetters(t *testing.T) {
	base, err := pgxpool.ParseConfig("postgres://base:9999/base")
	if err != nil {
		t.Fatalf("ParseConfig: %v", err)
	}
	p := New(
		WithPoolConfig(base),
		WithHost("final"),
		WithPort(5433),
	)
	if p.poolConfig.ConnConfig.Host != "final" || p.poolConfig.ConnConfig.Port != 5433 {
		t.Fatalf("Host/Port = %s:%d, want final:5433 (setters apply after WithPoolConfig)", p.poolConfig.ConnConfig.Host, p.poolConfig.ConnConfig.Port)
	}
	// setter after WithPoolConfig must not have shared the base config with another instance
	p2 := New(WithPoolConfig(base))
	if p2.poolConfig.ConnConfig.Host != "base" {
		t.Fatalf("second instance Host = %q, want base (configs must not be shared)", p2.poolConfig.ConnConfig.Host)
	}
}

func TestWithConfigOverridesOptions(t *testing.T) {
	p := New(
		WithHost("options:5432"),
		WithUser("from-options"),
		WithMaxConns(7),
		WithConfig(PostgresConfig{
			Host:               "config-host",
			Port:               5433,
			Database:           "cfgdb",
			SSLMode:            "disable",
			MaxConns:           12,
			ConnectTimeoutSec:  9,
			MaxConnLifetimeSec: 120,
		}),
	)
	cc := p.poolConfig.ConnConfig
	if cc.Host != "config-host" {
		t.Fatalf("Host = %q, want config-host (config wins)", cc.Host)
	}
	if cc.Port != 5433 {
		t.Fatalf("Port = %d, want 5433", cc.Port)
	}
	if cc.Database != "cfgdb" {
		t.Fatalf("Database = %q, want cfgdb", cc.Database)
	}
	if cc.TLSConfig != nil {
		t.Fatalf("TLSConfig = %v, want nil for ssl_mode disable", cc.TLSConfig)
	}
	if p.poolConfig.MaxConns != 12 {
		t.Fatalf("MaxConns = %d, want 12 (config wins)", p.poolConfig.MaxConns)
	}
	if cc.ConnectTimeout != 9*time.Second {
		t.Fatalf("ConnectTimeout = %v, want 9s", cc.ConnectTimeout)
	}
	if p.poolConfig.MaxConnLifetime != 120*time.Second {
		t.Fatalf("MaxConnLifetime = %v, want 120s", p.poolConfig.MaxConnLifetime)
	}
	// zero fields in config keep the option-set defaults
	if cc.User != "from-options" {
		t.Fatalf("User = %q, want from-options (empty config field keeps default)", cc.User)
	}
}

func TestSSLModeRequireAndPrefer(t *testing.T) {
	p := New(WithHost("db.example.com"), WithSSLMode("require"))
	cc := p.poolConfig.ConnConfig
	if cc.TLSConfig == nil {
		t.Fatal("TLSConfig is nil for sslmode require")
	}
	if !cc.TLSConfig.InsecureSkipVerify {
		t.Fatal("sslmode require should skip hostname verification (matches libpq)")
	}
	if len(cc.Fallbacks) != 0 {
		t.Fatalf("sslmode require should have no fallbacks, got %d", len(cc.Fallbacks))
	}

	p2 := New(WithHost("db.example.com"), WithSSLMode("prefer"))
	cc2 := p2.poolConfig.ConnConfig
	if cc2.TLSConfig == nil {
		t.Fatal("TLSConfig is nil for sslmode prefer")
	}
	if len(cc2.Fallbacks) != 1 {
		t.Fatalf("sslmode prefer should have one plaintext fallback, got %d", len(cc2.Fallbacks))
	}
	fb := cc2.Fallbacks[0]
	if fb.Host != "db.example.com" || fb.TLSConfig != nil {
		t.Fatalf("fallback = host %q tls %v, want db.example.com with no TLS", fb.Host, fb.TLSConfig != nil)
	}
}

func TestSSLModeInvalidInitError(t *testing.T) {
	p := New(WithHost("127.0.0.1"), WithSSLMode("garbage"))
	err := p.Init(context.Background(), newFramework(t))
	if err == nil {
		t.Fatal("invalid sslmode should fail Init")
	}
	if !strings.Contains(err.Error(), "ssl_mode") {
		t.Fatalf("err = %v, want ssl_mode mention", err)
	}
}

func TestWithConnStringInvalidDoesNotEchoPassword(t *testing.T) {
	p := New(WithConnString("postgres://alice:LEAKSECRET@["))
	err := p.Init(context.Background(), newFramework(t))
	if err == nil {
		t.Fatal("invalid connection string should fail Init")
	}
	if strings.Contains(err.Error(), "LEAKSECRET") {
		t.Fatalf("password leaked in error: %v", err)
	}
}

func TestWithConnStringInvalidInitError(t *testing.T) {
	p := New(WithConnString("::not-a-dsn::"))
	err := p.Init(context.Background(), newFramework(t))
	if err == nil {
		t.Fatal("invalid connection string should fail Init")
	}
	if !strings.Contains(err.Error(), "parse connection string") {
		t.Fatalf("err = %v, want parse connection string", err)
	}
}

func TestInitRejectsUnknownServer(t *testing.T) {
	p := New(
		WithHost("127.0.0.1"),
		WithPort(1), // closed port
		WithPingTimeout(500*time.Millisecond),
	)
	fw := newFramework(t)
	err := p.Init(context.Background(), fw)
	if err == nil {
		t.Fatal("Init against a closed port should fail")
	}
	if !strings.Contains(err.Error(), "ping") && !strings.Contains(err.Error(), "create pool") {
		t.Fatalf("error should mention ping or create pool, got: %v", err)
	}
	if pool := p.Pool(); pool != nil {
		t.Fatal("Pool() should be nil after failed Init")
	}
}

func TestDegradedModeAllowsFailedPing(t *testing.T) {
	p := New(
		WithHost("127.0.0.1"),
		WithPort(1),
		WithPingTimeout(200*time.Millisecond),
		WithDegradedMode(true),
		WithHealthWhenDegraded("not_ready"),
	)
	fw := newFramework(t)
	if err := p.Init(context.Background(), fw); err != nil {
		t.Fatalf("DegradedMode should allow Init after ping fail: %v", err)
	}
	if p.Pool() == nil {
		t.Fatal("DegradedMode keeps the pool for later reconnect attempts")
	}
	if err := p.Health(context.Background()); err == nil {
		t.Fatal("Health should fail when not_ready and ping fails")
	}
	ms := p.Metrics()
	if ms == nil {
		t.Fatal("Metrics should scream after DegradedMode")
	}
	_ = p.Shutdown(context.Background())
}

func TestDegradedModeHealthWhenReady(t *testing.T) {
	p := New(
		WithHost("127.0.0.1"),
		WithPort(1),
		WithPingTimeout(200*time.Millisecond),
		WithDegradedMode(true),
		WithHealthWhenDegraded("ready"),
	)
	fw := newFramework(t)
	if err := p.Init(context.Background(), fw); err != nil {
		t.Fatalf("Init: %v", err)
	}
	if err := p.Health(context.Background()); err != nil {
		t.Fatalf("health_when_degraded=ready should make Health nil while down: %v", err)
	}
	_ = p.Shutdown(context.Background())
}

func TestWithMigrationsOptions(t *testing.T) {
	fsys := fstest.MapFS{
		"000001_init.up.sql":   {Data: []byte("CREATE TABLE t (id int);")},
		"000001_init.down.sql": {Data: []byte("DROP TABLE t;")},
	}

	p := New(WithMigrations(fsys))
	if p.migrations == nil {
		t.Fatal("WithMigrations should configure migrations")
	}
	if p.migrations.table != "schema_migrations" {
		t.Fatalf("default migrations table = %q, want schema_migrations", p.migrations.table)
	}
	if p.migrations.fsys == nil {
		t.Fatal("migrations fsys is nil")
	}

	p2 := New(WithMigrations(fsys, WithMigrationsTable("my_service_migrations")))
	if p2.migrations.table != "my_service_migrations" {
		t.Fatalf("migrations table = %q, want my_service_migrations", p2.migrations.table)
	}

	// default (no WithMigrations) leaves the option nil
	p3 := New()
	if p3.migrations != nil {
		t.Fatal("migrations should be nil without WithMigrations")
	}
}

func TestMigrateRequiresInitAndWithMigrations(t *testing.T) {
	fsys := fstest.MapFS{
		"000001_init.up.sql":   {Data: []byte("CREATE TABLE t (id int);")},
		"000001_init.down.sql": {Data: []byte("DROP TABLE t;")},
	}
	if err := New(WithMigrations(fsys)).Migrate(context.Background()); err == nil {
		t.Fatal("Migrate before Init should fail")
	}
	if err := New().Migrate(context.Background()); err == nil {
		t.Fatal("Migrate without WithMigrations should fail")
	}
}

func TestInitUsesFrameworkLogger(t *testing.T) {
	logs := cf_logs.New(cf_logs.WithWriter(io.Discard))
	fw := cf.New()
	if err := fw.AddComponent(logs); err != nil {
		t.Fatalf("AddComponent(logs): %v", err)
	}

	p := New(WithHost("127.0.0.1"), WithPort(1), WithPingTimeout(200*time.Millisecond))
	_ = p.Init(context.Background(), fw)
	if p.logger == nil || p.logsSub == nil {
		t.Fatal("Init should subscribe to the framework logs component")
	}
	before := p.logger
	if before == logs.Logger() {
		t.Fatal("component logger must be OnReconfigureFor-scoped, not the process-global Logger()")
	}

	logs.Reconfigure(cf_logs.WithWriter(io.Discard))
	if p.logger == before {
		t.Fatal("component should receive the rebuilt logger on Reconfigure")
	}
	if p.logger == logs.Logger() {
		t.Fatal("rebuilt logger must remain OnReconfigureFor-scoped")
	}

	// an explicit WithLogger wins over the framework logger
	custom := slog.New(slog.NewTextHandler(io.Discard, nil))
	p2 := New(WithHost("127.0.0.1"), WithPort(1), WithPingTimeout(200*time.Millisecond), WithLogger(custom))
	_ = p2.Init(context.Background(), fw)
	if p2.logger != custom {
		t.Fatal("explicit WithLogger should win over the framework logger")
	}
}

func TestConcurrentPoolAccess(t *testing.T) {
	p := New(WithHost("127.0.0.1"), WithPort(1))
	fw := newFramework(t)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); _ = p.Init(context.Background(), fw) }()
	go func() { defer wg.Done(); _ = p.Init(context.Background(), fw) }()
	wg.Wait()

	// concurrent Shutdown/Pool must not race
	var wg2 sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg2.Add(2)
		go func() { defer wg2.Done(); _ = p.Shutdown(context.Background()) }()
		go func() { defer wg2.Done(); _ = p.Pool() }()
	}
	wg2.Wait()
}

type fakeQueryTracer struct {
	mu    sync.Mutex
	order []string
}

func (f *fakeQueryTracer) TraceQueryStart(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.order = append(f.order, "start:"+data.SQL)
	return ctx
}

func (f *fakeQueryTracer) TraceQueryEnd(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryEndData) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.order = append(f.order, "end:err="+fmt.Sprint(data.Err))
}

func TestWithQueryTracer(t *testing.T) {
	tracer := &fakeQueryTracer{}
	p := New(WithQueryTracer(tracer))
	if p.baseConfig.ConnConfig.Tracer != tracer {
		t.Fatal("WithQueryTracer should set the base config tracer (survives reload rebuilds)")
	}
	if p.poolConfig.ConnConfig.Tracer != tracer {
		t.Fatal("WithQueryTracer should set the active pool config tracer")
	}
	if New().baseConfig.ConnConfig.Tracer != nil {
		t.Fatal("default config should have no tracer")
	}

	// sanity: the tracer records a start/end pair around a call
	ctx := tracer.TraceQueryStart(context.Background(), nil, pgx.TraceQueryStartData{SQL: "SELECT 1"})
	tracer.TraceQueryEnd(ctx, nil, pgx.TraceQueryEndData{})
	tracer.mu.Lock()
	defer tracer.mu.Unlock()
	want := []string{"start:SELECT 1", "end:err=<nil>"}
	if len(tracer.order) != 2 || tracer.order[0] != want[0] || tracer.order[1] != want[1] {
		t.Fatalf("tracer order = %v, want %v", tracer.order, want)
	}
}

func TestRuntimeParamOptions(t *testing.T) {
	p := New(
		WithApplicationName("motors"),
		WithStatementTimeout(5*time.Second),
		WithLockTimeout(2*time.Second),
	)
	cc := p.poolConfig.ConnConfig
	if cc.RuntimeParams["application_name"] != "motors" {
		t.Fatalf("application_name = %q, want motors", cc.RuntimeParams["application_name"])
	}
	if cc.RuntimeParams["statement_timeout"] != "5000" {
		t.Fatalf("statement_timeout = %q, want 5000", cc.RuntimeParams["statement_timeout"])
	}
	if cc.RuntimeParams["lock_timeout"] != "2000" {
		t.Fatalf("lock_timeout = %q, want 2000", cc.RuntimeParams["lock_timeout"])
	}
	// baseConfig copy must carry them too (reload rebuilds from baseConfig).
	if New(WithStatementTimeout(3 * time.Second)).baseConfig.ConnConfig.RuntimeParams["statement_timeout"] != "3000" {
		t.Fatal("baseConfig should carry the statement_timeout runtime param")
	}
}

func TestTLSFileOptions(t *testing.T) {
	p := New(WithTLSRootCAFile("/secrets/ca.pem"), WithTLSClientCertFile("/secrets/tls.crt", "/secrets/tls.key"))
	if p.tlsFiles.rootCA != "/secrets/ca.pem" {
		t.Fatalf("rootCA = %q", p.tlsFiles.rootCA)
	}
	if p.tlsFiles.clientCert != "/secrets/tls.crt" || p.tlsFiles.clientKey != "/secrets/tls.key" {
		t.Fatalf("client cert files = %q/%q", p.tlsFiles.clientCert, p.tlsFiles.clientKey)
	}

	// a half-specified client pair is rejected at apply time
	if err := applyTLSFiles(&pgx.ConnConfig{}, tlsFiles{clientCert: "/x"}); err == nil {
		t.Fatal("cert without key should error")
	}
}

func TestApplyTLSFilesFromPEM(t *testing.T) {
	dir := t.TempDir()
	caPath := filepath.Join(dir, "ca.pem")
	writeTestCAPEM(t, caPath)

	cc := &pgx.ConnConfig{}
	if err := applyTLSFiles(cc, tlsFiles{rootCA: caPath}); err != nil {
		t.Fatalf("applyTLSFiles: %v", err)
	}
	if cc.TLSConfig == nil {
		t.Fatal("TLSConfig should be created for a CA file")
	}
	if cc.TLSConfig.RootCAs == nil {
		t.Fatal("RootCAs should be populated from ca.pem")
	}

	// missing file -> error
	if err := applyTLSFiles(&pgx.ConnConfig{}, tlsFiles{rootCA: filepath.Join(dir, "missing.pem")}); err == nil {
		t.Fatal("missing CA file should error")
	}
}

func TestApplyLoadedConfigNewFields(t *testing.T) {
	cfg, err := pgxpool.ParseConfig(defaultConnString)
	if err != nil {
		t.Fatalf("ParseConfig: %v", err)
	}
	loaded := PostgresConfig{
		ApplicationName:     "from-config",
		StatementTimeoutSec: 10,
		LockTimeoutSec:      1,
	}
	if err := applyLoadedConfig(cfg, loaded); err != nil {
		t.Fatalf("applyLoadedConfig: %v", err)
	}
	cc := cfg.ConnConfig
	if cc.RuntimeParams["application_name"] != "from-config" {
		t.Fatalf("application_name = %q", cc.RuntimeParams["application_name"])
	}
	if cc.RuntimeParams["statement_timeout"] != "10000" {
		t.Fatalf("statement_timeout = %q, want 10000", cc.RuntimeParams["statement_timeout"])
	}
	if cc.RuntimeParams["lock_timeout"] != "1000" {
		t.Fatalf("lock_timeout = %q, want 1000", cc.RuntimeParams["lock_timeout"])
	}
}

func TestWithMigrateOnInitRequiresWithMigrations(t *testing.T) {
	p := New(WithMigrateOnInit())
	fw := cf.New()
	if err := p.Init(context.Background(), fw); err == nil {
		t.Fatal("WithMigrateOnInit without WithMigrations should fail Init")
	}
}

func TestWithMigrateOnInitFlagOnNew(t *testing.T) {
	fsys := fstest.MapFS{
		"000001_init.up.sql":   {Data: []byte("SELECT 1;")},
		"000001_init.down.sql": {Data: []byte("SELECT 1;")},
	}
	p := New(WithMigrations(fsys), WithMigrateOnInit())
	if !p.migrateOnInit {
		t.Fatal("migrateOnInit should be true")
	}
	if p.migrations == nil {
		t.Fatal("migrations should be set")
	}
}

func TestWithEmbeddedMigrationsResolvesSubFS(t *testing.T) {
	p := New(WithEmbeddedMigrations(testMigrations, "testmigrations"))
	if p.migrations == nil {
		t.Fatal("migrations should be set")
	}
	if _, err := fs.Stat(p.migrations.fsys, "000001_init.up.sql"); err != nil {
		t.Fatalf("fsys should be rooted at the migrations dir: %v", err)
	}
}

func TestWithEmbeddedMigrationsPanicsOnMismatchedDir(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("WithEmbeddedMigrations with a mismatched dir should panic")
		}
	}()
	New(WithEmbeddedMigrations(testMigrations, "sql"))
}
