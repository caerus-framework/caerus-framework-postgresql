// Package cf_postgres provides the caerus-framework PostgreSQL component. It
// wraps a pgx/v5 connection pool, verifies connectivity with a fail-fast ping
// at Init, and exposes the pool to dependent components via Pool().
package cf_postgres

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	cf "github.com/caerus-framework/caerus-framework"
	cf_configuration "github.com/caerus-framework/caerus-framework-configuration"
	cf_logs "github.com/caerus-framework/caerus-framework-logs"
	cf_observability "github.com/caerus-framework/caerus-framework-observability"
	"github.com/golang-migrate/migrate/v4"
	pgxDriver "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
)

const (
	// ComponentName is the framework component name for the postgresql
	// component. It is the identifier other components use in GetDependencies
	// to require postgres.
	ComponentName = "postgresql"

	// ComponentStage is the stage data-layer components initialize in. It is
	// not a built-in bootstrap stage; AddComponent registers it automatically
	// the first time a component declares it.
	ComponentStage = cf.Stage("data")
)

// defaultConnString is parsed when no explicit connection string or config is
// given: localhost on the standard port, current OS user, no password.
const defaultConnString = "postgres://127.0.0.1:5432"

// PostgresConfig is the file/env-drivable connection configuration. Load it
// through the configuration component (caerus-framework-configuration) and
// pass it via WithConfig; both JSON and YAML tags are provided. Durations are
// in seconds.
type PostgresConfig struct {
	Host                 string `json:"host,omitempty" yaml:"host,omitempty" env:"HOST"`
	Port                 int    `json:"port,omitempty" yaml:"port,omitempty" env:"PORT"`
	User                 string `json:"user,omitempty" yaml:"user,omitempty" env:"USER"`
	Password             string `json:"password,omitempty" yaml:"password,omitempty" env:"PASSWORD" secret:"redact"`
	Database             string `json:"database,omitempty" yaml:"database,omitempty" env:"DATABASE"`
	SSLMode              string `json:"ssl_mode,omitempty" yaml:"ssl_mode,omitempty" env:"SSL_MODE"`
	MaxConns             int32  `json:"max_conns,omitempty" yaml:"max_conns,omitempty" env:"MAX_CONNS"`
	MinConns             int32  `json:"min_conns,omitempty" yaml:"min_conns,omitempty" env:"MIN_CONNS"`
	MaxConnLifetimeSec   int32  `json:"max_conn_lifetime_sec,omitempty" yaml:"max_conn_lifetime_sec,omitempty" env:"MAX_CONN_LIFETIME_SEC"`
	MaxConnIdleTimeSec   int32  `json:"max_conn_idle_time_sec,omitempty" yaml:"max_conn_idle_time_sec,omitempty" env:"MAX_CONN_IDLE_TIME_SEC"`
	HealthCheckPeriodSec int32  `json:"health_check_period_sec,omitempty" yaml:"health_check_period_sec,omitempty" env:"HEALTH_CHECK_PERIOD_SEC"`
	ConnectTimeoutSec    int32  `json:"connect_timeout_sec,omitempty" yaml:"connect_timeout_sec,omitempty" env:"CONNECT_TIMEOUT_SEC"`
	// ApplicationName sets the application_name runtime parameter on every
	// connection (parity with WithApplicationName, which wins when both set).
	ApplicationName string `json:"application_name,omitempty" yaml:"application_name,omitempty" env:"APPLICATION_NAME"`
	// StatementTimeoutSec and LockTimeoutSec are per-connection statement/lock
	// timeouts (Postgres milliseconds are derived from the seconds value; 0 =
	// unset). Guards against runaway queries and stuck advisory locks.
	StatementTimeoutSec int32 `json:"statement_timeout_sec,omitempty" yaml:"statement_timeout_sec,omitempty" env:"STATEMENT_TIMEOUT_SEC"`
	LockTimeoutSec      int32 `json:"lock_timeout_sec,omitempty" yaml:"lock_timeout_sec,omitempty" env:"LOCK_TIMEOUT_SEC"`
	// TLSRootCAFile, TLSClientCertFile, TLSClientKeyFile point at PEM files
	// (typically External Secrets / Secret mounts) used to build the TLS
	// config on every connect and reload — rotation is picked up without a
	// process restart. Requires an ssl_mode of require/verify-ca/verify-full.
	TLSRootCAFile     string `json:"tls_root_ca_file,omitempty" yaml:"tls_root_ca_file,omitempty" env:"TLS_ROOT_CA_FILE"`
	TLSClientCertFile string `json:"tls_client_cert_file,omitempty" yaml:"tls_client_cert_file,omitempty" env:"TLS_CLIENT_CERT_FILE"`
	TLSClientKeyFile  string `json:"tls_client_key_file,omitempty" yaml:"tls_client_key_file,omitempty" env:"TLS_CLIENT_KEY_FILE"`
	// DegradedMode — when true, a failed Init ping (or pool create) does not
	// abort the process. The pool is kept when create succeeded so later
	// reconnect can work; metrics/logs scream. Default off (pointer so
	// omitted ≠ explicit false). Off by default (hard Init).
	DegradedMode *bool `json:"degraded_mode,omitempty" yaml:"degraded_mode,omitempty" env:"DEGRADED_MODE"`
	// HealthWhenDegraded: "not_ready" (default) or "ready". Controls Health()
	// (and thus /readyz) while the pool cannot ping after a degraded Init
	// or while disconnected. "ready" is break-glass: send LB traffic anyway.
	HealthWhenDegraded string `json:"health_when_degraded,omitempty" yaml:"health_when_degraded,omitempty" env:"HEALTH_WHEN_DEGRADED"`
}

// Option configures the postgresql component at construction time.
type Option func(*options)

type options struct {
	poolConfig         *pgxpool.Config
	loaded             *PostgresConfig // set by WithConfig; overrides option-set defaults
	configSource       string          // named configuration source for live reload
	configPath         string          // source file path (module self-registration)
	srcEnvPrefix       string          // source env overlay prefix (default: NAME_)
	srcFormat          cf_configuration.Format
	srcFormatSet       bool
	migrations         *migrationConfig
	migrateOnInit      bool
	logger             *slog.Logger
	loggerSet          bool // true when WithLogger was called explicitly
	pingTimeout        time.Duration
	name               string // custom component name; empty means use ComponentName
	optErr             error  // deferred construction errors; returned from Init
	tlsFiles           tlsFiles
	degradedMode       bool
	healthWhenDegraded string // "ready" | "not_ready"
}

// tlsFiles holds PEM file paths for TLS/mTLS. Files are re-read whenever a
// pool is built (Init and reload) so rotated mounts take effect without a
// process restart.
type tlsFiles struct {
	rootCA     string
	clientCert string
	clientKey  string
}

// MigrationOption configures how WithMigrations applies schema migrations.
type MigrationOption func(*migrationConfig)

type migrationConfig struct {
	fsys  fs.FS
	table string
}

// WithMigrationsTable sets the table golang-migrate uses to track applied
// versions (default "schema_migrations"). Pick a distinct name when several
// services share one database.
func WithMigrationsTable(name string) MigrationOption {
	return func(m *migrationConfig) { m.table = name }
}

// WithMigrations configures the migration filesystem (and optional tracking
// table) used by [CFPostgres.Migrate]. It does not migrate by itself at Init;
// combine with [WithMigrateOnInit] for local single-process apps, or use the
// framework job flag --postgresql.job=migrate so the same binary can run as a
// K8s Job without a separate cmd/migrate.
//
// fsys must be rooted at the directory containing golang-migrate files
// (<version>_<name>.up.sql / .down.sql). For go:embed use
// [WithEmbeddedMigrations], which takes the embedded filesystem and directory
// and resolves the sub-filesystem for you.
func WithMigrations(fsys fs.FS, opts ...MigrationOption) Option {
	m := migrationConfig{fsys: fsys, table: "schema_migrations"}
	for _, opt := range opts {
		opt(&m)
	}
	return func(o *options) { o.migrations = &m }
}

// WithEmbeddedMigrations is [WithMigrations] for go:embed: it takes the
// embedded filesystem plus the directory holding the golang-migrate files and
// resolves the sub-filesystem internally. embed guarantees the directory
// exists at build time, so the only failure mode is a mismatched dir string —
// a programmer error that panics loudly:
//
//	//go:embed migrations
//	var migrations embed.FS
//
//	p := cf_postgres.New(
//		cf_postgres.WithEmbeddedMigrations(migrations, "migrations"),
//	)
func WithEmbeddedMigrations(fsys embed.FS, dir string, opts ...MigrationOption) Option {
	if _, err := fs.ReadDir(fsys, dir); err != nil {
		panic(fmt.Sprintf("cf_postgres: WithEmbeddedMigrations: dir %q: %v", dir, err))
	}
	sub, err := fs.Sub(fsys, dir)
	if err != nil {
		panic(fmt.Sprintf("cf_postgres: WithEmbeddedMigrations: fs.Sub(%q): %v", dir, err))
	}
	return WithMigrations(sub, opts...)
}

// WithMigrateOnInit makes Init call [CFPostgres.Migrate] after the connectivity
// ping (fail-fast). Use for local/single-replica only. Production should keep
// WithMigrations (so the framework job flag works) but omit WithMigrateOnInit on
// the serving Deployment, and run the Job with --postgresql.job=migrate instead.
func WithMigrateOnInit() Option {
	return func(o *options) { o.migrateOnInit = true }
}

// WithConfig sets a static connection configuration snapshot. Non-zero fields
// of cfg override the values set by the convenience options. Prefer
// WithConfigSource when using caerus-framework-configuration with hot-reload.
//
//	cfg, err := cf_configuration.Lookup[cf_postgres.PostgresConfig](conf, "postgresql")
//	p := cf_postgres.New(cf_postgres.WithConfig(*cfg))
func WithConfig(cfg PostgresConfig) Option {
	return func(o *options) { o.loaded = &cfg }
}

// SourceOption configures the self-registered configuration source created by
// WithConfigSource.
type SourceOption func(*sourceOptions)

type sourceOptions struct {
	envPrefix string
	format    cf_configuration.Format
	formatSet bool
}

// WithSourceEnvPrefix sets the environment overlay prefix for the source
// (default: the uppercase source name with "-" replaced by "_", plus "_" —
// "valkey-cache" → "VALKEY_CACHE_"). An empty prefix disables env overlay.
func WithSourceEnvPrefix(prefix string) SourceOption {
	return func(o *sourceOptions) { o.envPrefix = prefix }
}

// WithSourceFormat forces the file format instead of inferring it from the
// path extension (".yaml"/".yml" → YAML; anything else JSON).
func WithSourceFormat(f cf_configuration.Format) SourceOption {
	return func(o *sourceOptions) { o.format = f; o.formatSet = true }
}

// defaultSourceEnvPrefix derives an environment prefix from a source name.
func defaultSourceEnvPrefix(name string) string {
	return strings.ToUpper(strings.ReplaceAll(name, "-", "_")) + "_"
}

// WithConfigSource binds this component to a named configuration source and
// registers that source with the configuration component (via the framework's
// ConfigSourceRegistrar pass during argv absorption). The module owns the
// Source: the config type, default EnvPrefix, the POSTGRES_DSN AfterLoad
// overlay and its Owner (Name(), so named instances reload correctly). main
// only points the instance at where the config lives.
//
//	cf_postgres.New(cf_postgres.WithConfigSource("postgresql", "config/postgresql.json"))
//	cf_postgres.New(cf_postgres.WithConfigSource("orders", "/etc/app/orders.yaml",
//	    cf_postgres.WithSourceFormat(cf_configuration.FormatYAML)))
//
// A path of "" registers an env-only (fileless) source when the EnvPrefix is
// non-empty. The path CLI override stays --<source-name> (ParseFlags).
// Declares a dependency on "configuration".
func WithConfigSource(name, path string, opts ...SourceOption) Option {
	return func(o *options) {
		so := sourceOptions{envPrefix: defaultSourceEnvPrefix(name)}
		for _, opt := range opts {
			opt(&so)
		}
		o.configSource = name
		o.configPath = path
		o.srcEnvPrefix = so.envPrefix
		o.srcFormat = so.format
		o.srcFormatSet = so.formatSet
	}
}

// WithPoolConfig sets the full pgxpool.Config; the config is deep-copied, so
// later mutation by the caller does not affect the component and instances
// never share state. Convenience setters (WithConnString, WithHost, WithPort,
// WithUser, WithPassword, WithDatabase, WithSSLMode, WithMaxConns, ...)
// override the matching fields, so call them after WithPoolConfig if you
// combine them. Pass a config returned by pgxpool.ParseConfig when you need
// TLS roots, runtime params, or other pgx-only settings.
func WithPoolConfig(cfg *pgxpool.Config) Option {
	return func(o *options) { o.poolConfig = cfg.Copy() }
}

// WithConnString sets the full connection string (DSN or URL), parsed via
// pgxpool.ParseConfig. A bad DSN is recorded and returned from Init (New does
// not panic). Later convenience setters override individual fields.
func WithConnString(dsn string) Option {
	return func(o *options) {
		cfg, err := pgxpool.ParseConfig(dsn)
		if err != nil {
			if o.optErr == nil {
				// Do not wrap the pgx error: it interpolates the DSN (password).
				o.optErr = errors.New("cf_postgres: parse connection string: invalid connection string")
			}
			return
		}
		o.poolConfig = cfg
	}
}

// WithHost sets the server host (default "127.0.0.1").
func WithHost(host string) Option {
	return func(o *options) { o.poolConfig.ConnConfig.Host = host }
}

// WithPort sets the server port (default 5432).
func WithPort(port int) Option {
	return func(o *options) { o.poolConfig.ConnConfig.Port = uint16(port) }
}

// WithUser sets the role to connect as (default: current OS user).
func WithUser(user string) Option {
	return func(o *options) { o.poolConfig.ConnConfig.User = user }
}

// WithPassword sets the authentication password.
func WithPassword(password string) Option {
	return func(o *options) { o.poolConfig.ConnConfig.Password = password }
}

// WithDatabase sets the database name (default: the user name).
func WithDatabase(db string) Option {
	return func(o *options) { o.poolConfig.ConnConfig.Database = db }
}

// WithSSLMode sets the sslmode (disable, prefer, require, verify-ca,
// verify-full). The default from the base connection string is "prefer".
// The TLS config and fallback chain are re-derived from the current
// host/port, so sslmode="prefer" still attempts plaintext as a fallback on
// the same address. An unknown mode is recorded and returned from Init
// (New does not panic): silently ignoring an explicit TLS requirement would
// be worse than failing startup.
func WithSSLMode(mode string) Option {
	return func(o *options) {
		if err := applySSLMode(o.poolConfig.ConnConfig, mode); err != nil && o.optErr == nil {
			o.optErr = err
		}
	}
}

// WithMaxConns sets the pool's maximum number of connections (default: max(4,
// runtime.GOMAXPROCS)).
func WithMaxConns(n int32) Option {
	return func(o *options) { o.poolConfig.MaxConns = n }
}

// WithMinConns sets the pool's minimum number of connections (default 0).
func WithMinConns(n int32) Option {
	return func(o *options) { o.poolConfig.MinConns = n }
}

// WithMaxConnLifetime sets how long a connection may live before being closed
// and replaced (default 1h).
func WithMaxConnLifetime(d time.Duration) Option {
	return func(o *options) { o.poolConfig.MaxConnLifetime = d }
}

// WithMaxConnIdleTime sets how long an idle connection may live before being
// closed (default 30m).
func WithMaxConnIdleTime(d time.Duration) Option {
	return func(o *options) { o.poolConfig.MaxConnIdleTime = d }
}

// WithHealthCheckPeriod sets how often the pool health-checks idle
// connections (default 1m).
func WithHealthCheckPeriod(d time.Duration) Option {
	return func(o *options) { o.poolConfig.HealthCheckPeriod = d }
}

// WithConnectTimeout sets how long a single connection attempt may take
// (default 0 = no timeout, the pgx/libpq default).
func WithConnectTimeout(d time.Duration) Option {
	return func(o *options) { o.poolConfig.ConnConfig.ConnectTimeout = d }
}

// WithApplicationName sets the application_name runtime parameter, identifying
// this process's connections to the server (default: unset, so the server-side
// default applies).
func WithApplicationName(name string) Option {
	return func(o *options) {
		cc := o.poolConfig.ConnConfig
		if cc.RuntimeParams == nil {
			cc.RuntimeParams = make(map[string]string)
		}
		cc.RuntimeParams["application_name"] = name
	}
}

// WithStatementTimeout sets the per-connection statement_timeout (0 = unset).
// The value is applied as a runtime parameter in milliseconds on every
// connection, guarding against runaway queries.
func WithStatementTimeout(d time.Duration) Option {
	return func(o *options) {
		cc := o.poolConfig.ConnConfig
		if cc.RuntimeParams == nil {
			cc.RuntimeParams = make(map[string]string)
		}
		cc.RuntimeParams["statement_timeout"] = strconv.FormatInt(d.Milliseconds(), 10)
	}
}

// WithLockTimeout sets the per-connection lock_timeout (0 = unset). The value
// is applied as a runtime parameter in milliseconds on every connection, so a
// blocked row lock aborts instead of waiting forever.
func WithLockTimeout(d time.Duration) Option {
	return func(o *options) {
		cc := o.poolConfig.ConnConfig
		if cc.RuntimeParams == nil {
			cc.RuntimeParams = make(map[string]string)
		}
		cc.RuntimeParams["lock_timeout"] = strconv.FormatInt(d.Milliseconds(), 10)
	}
}

// WithTLSRootCAFile configures a PEM CA bundle used to verify the server
// certificate (mutual trust for sslmode verify-ca/verify-full, or a private
// CA for require). The file is re-read on every pool build, so a rotated
// Secret mount is picked up on the next configuration reload. Requires
// sslmode require/verify-ca/verify-full.
func WithTLSRootCAFile(path string) Option {
	return func(o *options) {
		o.tlsFiles.rootCA = path
	}
}

// WithTLSClientCertFile configures the client PEM certificate and key pair
// for mTLS. Both paths must be non-empty. Re-read on every pool build, so
// rotated Secret mounts are picked up on the next configuration reload.
func WithTLSClientCertFile(certPath, keyPath string) Option {
	return func(o *options) {
		o.tlsFiles.clientCert = certPath
		o.tlsFiles.clientKey = keyPath
	}
}

// WithQueryTracer sets the pgx query tracer, invoked around every Query,
// QueryRow, and Exec call. Use it for observability hooks (otel spans,
// slow-query logging) without importing an instrumentation library into this
// module — the tracer interface lives in pgx. It is part of the option base
// config, so it survives pool rebuilds on configuration reload. Apps that need
// several tracers chain them with a small composite type; pgx calls a single
// tracer per query.
func WithQueryTracer(t pgx.QueryTracer) Option {
	return func(o *options) { o.poolConfig.ConnConfig.Tracer = t }
}

// WithPingTimeout sets how long Init waits for the connectivity ping before
// failing (default 5s).
func WithPingTimeout(d time.Duration) Option {
	return func(o *options) { o.pingTimeout = d }
}

// WithLogger overrides the logger used for component diagnostics. By default
// the component logs through the framework logs component (declared in
// GetDependencies); WithLogger is an explicit override for tests and embedded
// use and wins over the framework logger. slog.Default() remains the fallback
// only when neither is available.
func WithLogger(logger *slog.Logger) Option {
	return func(o *options) { o.logger = logger; o.loggerSet = true }
}

// WithName sets a custom component name, allowing multiple postgres instances
// in the same process. The default name is "postgresql" (ComponentName). Use
// this when you need multiple postgres clients (e.g., primary and replica) in
// one binary. Retrieve named instances with GetByName[*CFPostgres](fw, "primary").
func WithName(name string) Option {
	return func(o *options) { o.name = name }
}

// WithDegradedMode allows Init to succeed when the connectivity ping (or pool
// create) fails. Default is hard-fail. Degraded mode screams in logs/metrics;
// Health still fails ping unless HealthWhenDegraded is "ready".
func WithDegradedMode(enabled bool) Option {
	return func(o *options) { o.degradedMode = enabled }
}

// WithHealthWhenDegraded sets Health() behaviour while unreachable after
// DegradedMode: "not_ready" (default) or "ready" (break-glass LB traffic).
func WithHealthWhenDegraded(policy string) Option {
	return func(o *options) { o.healthWhenDegraded = policy }
}

// CFPostgres is the caerus-framework-postgresql component. It wraps a pgx
// connection pool, verifies connectivity at Init, and closes it at Shutdown.
type CFPostgres struct {
	mu                 sync.RWMutex
	baseConfig         *pgxpool.Config // option defaults (before config source)
	poolConfig         *pgxpool.Config
	configSource       string
	configPath         string
	srcEnvPrefix       string
	srcFormat          cf_configuration.Format
	srcFormatSet       bool
	pingTimeout        time.Duration
	migrations         *migrationConfig
	migrateOnInit      bool
	loggerSet          bool
	pool               *pgxpool.Pool
	logger             *slog.Logger
	logsSub            *cf_logs.Subscription
	fw                 *cf.CaerusFramework
	name               string // custom name; empty means use ComponentName
	optErr             error  // deferred from New options / WithConfig; returned by Init
	tlsFiles           tlsFiles
	degradedMode       bool
	healthWhenDegraded string // "ready" | "not_ready"
	initDone           atomic.Bool
	// liveConnected is true after a successful ping (Init or Health recovery).
	liveConnected atomic.Bool
	// degradedUnreachable: Init ping failed under DegradedMode (or later ping lost).
	degradedUnreachable atomic.Bool
	degradedModeUses    atomic.Uint64
	reconnectCancel     context.CancelFunc
	reconnectWG         sync.WaitGroup
}

// New creates a postgresql component. The pool is created and pinged at Init,
// not here. Invalid user-facing options (bad DSN, unknown ssl_mode) are
// deferred to Init rather than panicking; only an unparseable built-in default
// connection string panics (internal invariant).
func New(opts ...Option) *CFPostgres {
	o := options{
		logger:      slog.Default(),
		pingTimeout: 5 * time.Second,
	}
	base, err := pgxpool.ParseConfig(defaultConnString)
	if err != nil {
		panic(fmt.Sprintf("cf_postgres: parse default connection string: %v", err))
	}
	o.poolConfig = base
	for _, opt := range opts {
		opt(&o)
	}
	baseCopy := o.poolConfig.Copy()
	degrade := o.degradedMode
	healthDegraded := normalizeHealthWhenDegraded(o.healthWhenDegraded)
	if o.loaded != nil {
		if err := applyLoadedConfig(o.poolConfig, *o.loaded); err != nil && o.optErr == nil {
			o.optErr = err
		}
		degrade, healthDegraded = degradedModeFromConfig(*o.loaded, degrade, healthDegraded)
	}
	return &CFPostgres{
		baseConfig:         baseCopy,
		poolConfig:         o.poolConfig,
		configSource:       o.configSource,
		configPath:         o.configPath,
		srcEnvPrefix:       o.srcEnvPrefix,
		srcFormat:          o.srcFormat,
		srcFormatSet:       o.srcFormatSet,
		logger:             o.logger,
		loggerSet:          o.loggerSet,
		pingTimeout:        o.pingTimeout,
		migrations:         o.migrations,
		migrateOnInit:      o.migrateOnInit,
		name:               o.name,
		optErr:             o.optErr,
		tlsFiles:           o.tlsFiles,
		degradedMode:       degrade,
		healthWhenDegraded: healthDegraded,
	}
}

func normalizeHealthWhenDegraded(policy string) string {
	switch strings.ToLower(strings.TrimSpace(policy)) {
	case "ready":
		return "ready"
	default:
		return "not_ready"
	}
}

func degradedModeFromConfig(cfg PostgresConfig, degrade bool, healthDegraded string) (bool, string) {
	if cfg.DegradedMode != nil {
		degrade = *cfg.DegradedMode
	}
	if cfg.HealthWhenDegraded != "" {
		healthDegraded = normalizeHealthWhenDegraded(cfg.HealthWhenDegraded)
	}
	return degrade, healthDegraded
}

// applyLoadedConfig overlays non-zero fields of cfg onto the pool config. It
// runs last, so a loaded config always wins over option-set defaults.
func applyLoadedConfig(cfg *pgxpool.Config, loaded PostgresConfig) error {
	cc := cfg.ConnConfig
	if loaded.Host != "" {
		cc.Host = loaded.Host
	}
	if loaded.Port != 0 {
		cc.Port = uint16(loaded.Port)
	}
	if loaded.User != "" {
		cc.User = loaded.User
	}
	if loaded.Password != "" {
		cc.Password = loaded.Password
	}
	if loaded.Database != "" {
		cc.Database = loaded.Database
	}
	if loaded.SSLMode != "" {
		if err := applySSLMode(cc, loaded.SSLMode); err != nil {
			return err
		}
	}
	if loaded.MaxConns != 0 {
		cfg.MaxConns = loaded.MaxConns
	}
	if loaded.MinConns != 0 {
		cfg.MinConns = loaded.MinConns
	}
	if loaded.MaxConnLifetimeSec != 0 {
		cfg.MaxConnLifetime = time.Duration(loaded.MaxConnLifetimeSec) * time.Second
	}
	if loaded.MaxConnIdleTimeSec != 0 {
		cfg.MaxConnIdleTime = time.Duration(loaded.MaxConnIdleTimeSec) * time.Second
	}
	if loaded.HealthCheckPeriodSec != 0 {
		cfg.HealthCheckPeriod = time.Duration(loaded.HealthCheckPeriodSec) * time.Second
	}
	if loaded.ConnectTimeoutSec != 0 {
		cc.ConnectTimeout = time.Duration(loaded.ConnectTimeoutSec) * time.Second
	}
	if loaded.ApplicationName != "" {
		setRuntimeParam(cc, "application_name", loaded.ApplicationName)
	}
	if loaded.StatementTimeoutSec != 0 {
		setRuntimeParam(cc, "statement_timeout", strconv.FormatInt(int64(loaded.StatementTimeoutSec)*1000, 10))
	}
	if loaded.LockTimeoutSec != 0 {
		setRuntimeParam(cc, "lock_timeout", strconv.FormatInt(int64(loaded.LockTimeoutSec)*1000, 10))
	}
	if loaded.TLSRootCAFile != "" || loaded.TLSClientCertFile != "" || loaded.TLSClientKeyFile != "" {
		if err := applyTLSFiles(cc, tlsFiles{
			rootCA:     loaded.TLSRootCAFile,
			clientCert: loaded.TLSClientCertFile,
			clientKey:  loaded.TLSClientKeyFile,
		}); err != nil {
			return err
		}
	}
	return nil
}

// setRuntimeParam sets a pgx runtime parameter, initializing the map when nil.
func setRuntimeParam(cc *pgx.ConnConfig, key, value string) {
	if cc.RuntimeParams == nil {
		cc.RuntimeParams = make(map[string]string)
	}
	cc.RuntimeParams[key] = value
}

// applyTLSFiles loads PEM CA / client certificate files into the connection's
// TLS config. It re-reads the files on every call, so rotation of mounted
// Secret/ConfigMap files takes effect when the configuration source reloads
// the connection. Files are loaded only for TLS-enabled modes: if the config
// carries no TLS config yet (sslmode disable), the CA/cert pair creates one
// (treating the request as TLS-required).
func applyTLSFiles(cc *pgx.ConnConfig, files tlsFiles) error {
	if files.rootCA == "" && files.clientCert == "" && files.clientKey == "" {
		return nil
	}
	if (files.clientCert == "") != (files.clientKey == "") {
		return fmt.Errorf("cf_postgres: TLS client cert and key must be set together")
	}
	if cc.TLSConfig == nil {
		cc.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	}
	if files.rootCA != "" {
		pemData, err := os.ReadFile(files.rootCA)
		if err != nil {
			return fmt.Errorf("cf_postgres: read tls_root_ca_file: %w", err)
		}
		roots := x509.NewCertPool()
		if !roots.AppendCertsFromPEM(pemData) {
			return fmt.Errorf("cf_postgres: tls_root_ca_file %q contains no PEM certificates", files.rootCA)
		}
		cc.TLSConfig.RootCAs = roots
	}
	if files.clientCert != "" {
		pair, err := tls.LoadX509KeyPair(files.clientCert, files.clientKey)
		if err != nil {
			return fmt.Errorf("cf_postgres: load client certificate: %w", err)
		}
		cc.TLSConfig.Certificates = []tls.Certificate{pair}
	}
	return nil
}

// applySSLMode re-derives the TLS config and fallback chain from the current
// host/port, reusing pgx's own parser so keyword handling (prefer, require,
// verify-ca, verify-full, disable, ...) stays correct and fallbacks point at
// the real address. Returns an error on an unknown mode. sslmode is ignored
// for unix socket hosts, matching libpq.
func applySSLMode(cc *pgx.ConnConfig, mode string) error {
	host := cc.Host
	if host == "" {
		host = "localhost"
	}
	parsed, err := pgconn.ParseConfig(fmt.Sprintf("host=%s port=%d sslmode=%s", host, cc.Port, mode))
	if err != nil {
		return fmt.Errorf("cf_postgres: invalid ssl_mode %q: %w", mode, err)
	}
	cc.TLSConfig = parsed.TLSConfig
	cc.Fallbacks = parsed.Fallbacks
	return nil
}

// Name implements cf.CaerusComponent.
// Name implements cf.CaerusComponent. Returns the custom name set via WithName,
// or the default ComponentName ("postgresql") if no custom name was set.
func (c *CFPostgres) Name() string {
	if c.name != "" {
		return c.name
	}
	return ComponentName
}

// GetInitOrderStage implements cf.CaerusComponent.
func (c *CFPostgres) GetInitOrderStage() cf.Stage { return ComponentStage }

// GetDependencies implements cf.Dependencies. The component logs through the
// framework logs component, and depends on configuration when WithConfigSource
// is set.
func (c *CFPostgres) GetDependencies() []string {
	deps := []string{cf_logs.ComponentName}
	if c.configSource != "" {
		deps = append(deps, cf_configuration.ComponentName)
	}
	return deps
}

// Init implements cf.CaerusComponent. It creates the pgx connection pool and
// verifies connectivity with a ping. By default a broken database fails
// startup (fail-fast). With DegradedMode, a failed ping keeps the pool (or a
// failed create leaves Pool nil) and lets Initialize continue (metrics/logs
// scream; Health stays honest unless health_when_degraded=ready).
func (c *CFPostgres) Init(ctx context.Context, fw *cf.CaerusFramework) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.initDone.Load() {
		return nil // already initialized
	}
	if c.optErr != nil {
		return c.optErr
	}
	c.fw = fw
	if !c.loggerSet {
		if logs, ok := cf.Get[*cf_logs.Logs](fw); ok {
			c.logsSub = logs.OnReconfigureFor(c.Name(), func(l *slog.Logger) { c.logger = l })
		}
	}

	poolCfg := c.poolConfig
	if c.configSource != "" {
		cfg, degrade, healthDegraded, err := c.poolConfigFromSource()
		if err != nil {
			return err
		}
		poolCfg = cfg
		c.poolConfig = cfg
		c.degradedMode = degrade
		c.healthWhenDegraded = healthDegraded
	} else if err := applyTLSFiles(poolCfg.ConnConfig, c.tlsFiles); err != nil {
		return err
	}

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		if !c.degradedMode {
			return fmt.Errorf("cf_postgres: create pool: %w", err)
		}
		c.initDone.Store(true)
		c.liveConnected.Store(false)
		c.degradedUnreachable.Store(true)
		c.degradedModeUses.Add(1)
		c.logger.Error("cf_postgres: DegradedMode — create pool failed; Init continues; Health/readyz follow health_when_degraded",
			"err", err,
			"host", poolCfg.ConnConfig.Host,
			"port", poolCfg.ConnConfig.Port,
			"health_when_degraded", c.healthWhenDegraded,
		)
		c.startReconnectLocked()
		return nil
	}

	pingCtx, cancel := context.WithTimeout(ctx, c.pingTimeout)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		if !c.degradedMode {
			pool.Close()
			return fmt.Errorf("cf_postgres: ping %s:%d failed: %w",
				poolCfg.ConnConfig.Host, poolCfg.ConnConfig.Port, err)
		}
		c.pool = pool
		c.initDone.Store(true)
		c.liveConnected.Store(false)
		c.degradedUnreachable.Store(true)
		c.degradedModeUses.Add(1)
		c.logger.Error("cf_postgres: DegradedMode — ping failed; Init continues; Health/readyz follow health_when_degraded",
			"err", err,
			"host", poolCfg.ConnConfig.Host,
			"port", poolCfg.ConnConfig.Port,
			"health_when_degraded", c.healthWhenDegraded,
		)
		c.startReconnectLocked()
		return nil
	}

	c.pool = pool
	c.initDone.Store(true)
	c.liveConnected.Store(true)
	c.degradedUnreachable.Store(false)
	if c.migrateOnInit {
		if c.migrations == nil {
			pool.Close()
			c.pool = nil
			c.initDone.Store(false)
			c.liveConnected.Store(false)
			return errors.New("cf_postgres: WithMigrateOnInit requires WithMigrations")
		}
		if err := c.applyMigrations(c.pool, c.migrations); err != nil {
			pool.Close()
			c.pool = nil
			c.initDone.Store(false)
			c.liveConnected.Store(false)
			return fmt.Errorf("cf_postgres: migrations: %w", err)
		}
	}
	c.logger.Info("cf_postgres: connected",
		"host", poolCfg.ConnConfig.Host,
		"port", poolCfg.ConnConfig.Port,
		"database", poolCfg.ConnConfig.Database,
		"user", poolCfg.ConnConfig.User,
		"max_conns", poolCfg.MaxConns,
		cf_logs.SecretSet("password", poolCfg.ConnConfig.Password),
	)
	if c.degradedMode {
		c.startReconnectLocked()
	}
	return nil
}

// OnConfigReload implements cf.ConfigReloader. It rebuilds the pool from the
// bound configuration source (file → env → DSN already applied by
// configuration). The fresh value is delivered as cfg but the pool is rebuilt
// from the source so the translation stays in one place. On failure the
// previous pool is kept (last-good).
func (c *CFPostgres) OnConfigReload(source string, cfg any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if source != c.configSource || !c.initDone.Load() || c.fw == nil {
		return
	}
	poolCfg, degrade, healthDegraded, err := c.poolConfigFromSource()
	if err != nil {
		c.logger.Error("cf_postgres: config reload rejected", "err", err)
		return
	}
	c.degradedMode = degrade
	c.healthWhenDegraded = healthDegraded
	ctx, cancel := context.WithTimeout(context.Background(), c.pingTimeout)
	defer cancel()
	newPool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		c.logger.Error("cf_postgres: config reload create pool failed; keeping previous", "err", err)
		return
	}
	if err := newPool.Ping(ctx); err != nil {
		newPool.Close()
		c.logger.Error("cf_postgres: config reload ping failed; keeping previous", "err", err)
		return
	}
	old := c.pool
	c.pool = newPool
	c.poolConfig = poolCfg
	c.liveConnected.Store(true)
	c.degradedUnreachable.Store(false)
	if old != nil {
		old.Close()
	}
	c.logger.Info("cf_postgres: reconnected after config reload",
		"host", poolCfg.ConnConfig.Host,
		"port", poolCfg.ConnConfig.Port,
		"database", poolCfg.ConnConfig.Database,
		cf_logs.SecretSet("password", poolCfg.ConnConfig.Password),
	)
}

func (c *CFPostgres) poolConfigFromSource() (*pgxpool.Config, bool, string, error) {
	conf, ok := cf.Get[*cf_configuration.Configuration](c.fw)
	if !ok {
		return nil, false, "", errors.New("cf_postgres: configuration component not registered")
	}
	loaded, ok := cf_configuration.Get[PostgresConfig](conf, c.configSource)
	if !ok {
		return nil, false, "", fmt.Errorf("cf_postgres: configuration source %q not found", c.configSource)
	}
	cfg := c.baseConfig.Copy()
	if err := applyLoadedConfig(cfg, *loaded); err != nil {
		return nil, false, "", err
	}
	if err := applyTLSFiles(cfg.ConnConfig, c.tlsFiles); err != nil {
		return nil, false, "", err
	}
	degrade, healthDegraded := degradedModeFromConfig(*loaded, c.degradedMode, c.healthWhenDegraded)
	return cfg, degrade, healthDegraded, nil
}

// RegisterConfigSources implements cf.ConfigSourceRegistrar. The framework
// calls it during argv absorption; it registers this component's configuration
// source (name, path, env prefix, format, Owner and the POSTGRES_DSN AfterLoad
// overlay) with the configuration component. No-op when no source is bound.
func (c *CFPostgres) RegisterConfigSources(conf any) error {
	cfg, ok := conf.(*cf_configuration.Configuration)
	if !ok {
		return fmt.Errorf("cf_postgres: RegisterConfigSources: expected configuration component, got %T", conf)
	}
	if c.configSource == "" {
		return nil
	}
	format := c.srcFormat
	if !c.srcFormatSet {
		if p := strings.ToLower(c.configPath); strings.HasSuffix(p, ".yaml") || strings.HasSuffix(p, ".yml") {
			format = cf_configuration.FormatYAML
		} else {
			format = cf_configuration.FormatJSON
		}
	}
	// Job declares the CLI-only job flag this instance serves: the flag names
	// the instance and the value names the task. The default instance gets
	// "--postgresql.job"; a named instance (WithName) gets
	// "--postgresql.<name>.job". The only supported task is "migrate", routed
	// to this instance's Migrate by the framework's job-only init path.
	jobFlag := ComponentName + ".job"
	if c.Name() != ComponentName {
		jobFlag = ComponentName + "." + c.Name() + ".job"
	}
	return cf_configuration.AddSource(cfg, cf_configuration.Source[PostgresConfig]{
		Name:      c.configSource,
		Path:      c.configPath,
		Format:    format,
		Owner:     c.Name(),
		EnvPrefix: c.srcEnvPrefix,
		Job:       cf.JobSpec{Flag: jobFlag, Tasks: []string{"migrate"}},
		AfterLoad: func(pc *PostgresConfig) error {
			if dsn := os.Getenv("POSTGRES_DSN"); dsn != "" {
				return OverlayDSN(pc, dsn)
			}
			return nil
		},
	})
}

// Migrate applies pending up migrations using the live pool and the filesystem
// configured via [WithMigrations]. Requires a successful Init. "Already up to
// date" (migrate.ErrNoChange) is success. Concurrent callers are serialized by
// golang-migrate's advisory lock.
//
// Production: prefer the framework job flag --postgresql.job=migrate from the
// same binary; omit [WithMigrateOnInit] on serving Deployments so Init does not
// migrate.
func (c *CFPostgres) Migrate(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.RLock()
	pool := c.pool
	m := c.migrations
	c.mu.RUnlock()
	if pool == nil {
		return errors.New("cf_postgres: Migrate requires Init")
	}
	if m == nil {
		return errors.New("cf_postgres: Migrate requires WithMigrations")
	}
	return c.applyMigrations(pool, m)
}

// RunJob implements cf.JobRunner. The only supported task is "migrate", which
// it dispatches to [CFPostgres.Migrate]; any other task is an error (the
// framework validates the task against the source's declared set before this
// runs, so this is the last line of defense).
func (c *CFPostgres) RunJob(ctx context.Context, task string) error {
	if task != "migrate" {
		return fmt.Errorf("cf_postgres: unknown job task %q (supported: migrate)", task)
	}
	return c.Migrate(ctx)
}

// applyMigrations runs golang-migrate Up against pool. Caller must pass a
// non-nil pool and migration config. Reuses the pool via the pgx stdlib
// adapter so migrations see the same TLS and credentials as the app.
func (c *CFPostgres) applyMigrations(pool *pgxpool.Pool, m *migrationConfig) error {
	src, err := iofs.New(m.fsys, ".")
	if err != nil {
		return fmt.Errorf("open migrations filesystem: %w", err)
	}

	db := stdlib.OpenDBFromPool(pool)
	drv, err := pgxDriver.WithInstance(db, &pgxDriver.Config{
		MigrationsTable: m.table,
	})
	if err != nil {
		db.Close()
		return fmt.Errorf("create migration driver: %w", err)
	}

	migrator, err := migrate.NewWithInstance("iofs", src, "pgx5", drv)
	if err != nil {
		db.Close()
		return fmt.Errorf("create migrator: %w", err)
	}

	applyErr := migrator.Up()
	srcErr, dbErr := migrator.Close() // releases the sql.DB (pool stays open)

	switch {
	case applyErr == nil:
		c.logger.Info("cf_postgres: migrations applied", "migrations_table", m.table)
	case errors.Is(applyErr, migrate.ErrNoChange):
		c.logger.Info("cf_postgres: migrations up to date", "migrations_table", m.table)
	default:
		return applyErr
	}
	if srcErr != nil || dbErr != nil {
		return fmt.Errorf("close migrator: source=%v database=%v", srcErr, dbErr)
	}
	return nil
}

// Shutdown implements cf.CaerusComponent. It closes the pgx pool; further use
// of Pool() after shutdown returns nil.
func (c *CFPostgres) Shutdown(ctx context.Context) error {
	c.mu.Lock()
	stop := c.reconnectCancel
	c.reconnectCancel = nil
	c.mu.Unlock()
	if stop != nil {
		stop()
		c.reconnectWG.Wait()
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.logsSub != nil {
		c.logsSub.Unsubscribe()
		c.logsSub = nil
	}
	c.initDone.Store(false)
	c.liveConnected.Store(false)
	c.degradedUnreachable.Store(false)
	if c.pool == nil {
		return nil
	}
	pool := c.pool
	c.pool = nil
	pool.Close()
	return nil
}

// Pool returns the pgx connection pool. It is non-nil after a successful Init
// and nil before Init or after Shutdown.
func (c *CFPostgres) Pool() *pgxpool.Pool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.pool
}

// WithinTx runs fn inside a single transaction, committing when it returns nil
// and rolling back otherwise (including on an error from fn or a canceled
// context). Use it for multi-statement operations that must be atomic — the
// framework keeps the pool lifecycle; this helper keeps the commit/rollback
// boilerplate out of every caller. Requires a successful Init.
func (c *CFPostgres) WithinTx(ctx context.Context, fn func(pgx.Tx) error) error {
	pool := c.Pool()
	if pool == nil {
		return errors.New("cf_postgres: WithinTx requires Init")
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// Health implements cf.HealthProvider. It pings the pool, so the observability
// component's readiness endpoint reflects real database connectivity. A nil
// pool (before Init or after Shutdown) is unhealthy. After DegradedMode with a
// failed ping (or nil pool from a failed create), behaviour follows
// health_when_degraded (default not_ready → still unhealthy for /readyz).
func (c *CFPostgres) Health(ctx context.Context) error {
	pool := c.Pool()
	if pool == nil {
		c.liveConnected.Store(false)
		if c.initDone.Load() {
			c.degradedUnreachable.Store(true)
		}
		if c.initDone.Load() && c.degradedMode && c.healthWhenDegraded == "ready" {
			return nil
		}
		return errors.New("cf_postgres: pool is not initialized")
	}
	if err := pool.Ping(ctx); err != nil {
		c.liveConnected.Store(false)
		c.degradedUnreachable.Store(true)
		if c.degradedMode && c.healthWhenDegraded == "ready" {
			return nil
		}
		return err
	}
	c.liveConnected.Store(true)
	c.degradedUnreachable.Store(false)
	return nil
}

// Metrics implements cf_observability.MetricsProvider. Before Init or after
// Shutdown it returns nil. After Init (including DegradedMode without a live
// ping) it always returns samples so degrade/unreachable state is visible.
//
// Pool gauges come from pgxpool.Stat on every scrape when the pool is non-nil;
// *_total counters are cumulative for the pool's lifetime and reset only when
// the pool is rebuilt (config reload / reconnect). The "component" label
// carries Name() so named primary/replica instances are distinguishable on
// /metrics.
func (c *CFPostgres) Metrics() []cf_observability.Metric {
	if !c.initDone.Load() {
		return nil
	}
	live := 0.0
	if c.liveConnected.Load() {
		live = 1
	}
	degraded := 0.0
	if c.degradedUnreachable.Load() {
		degraded = 1
	}
	labels := map[string]string{"component": c.Name()}
	if cc := c.poolConfig.ConnConfig; cc != nil {
		labels["database"] = cc.Database
		labels["user"] = cc.User
		labels["host"] = cc.Host
		labels["port"] = strconv.Itoa(int(cc.Port))
	}
	infoLabels := copyLabels(labels)
	infoLabels["live"] = strconv.FormatBool(c.liveConnected.Load())
	degradedLabels := copyLabels(labels)
	degradedLabels["degraded_mode"] = strconv.FormatBool(c.degradedMode)
	degradedLabels["health_when_degraded"] = c.healthWhenDegraded
	ms := []cf_observability.Metric{
		{
			Name:   "postgresql_info",
			Help:   "PostgreSQL pool descriptor; 1 while Init completed.",
			Value:  1,
			Labels: infoLabels,
		},
		{
			Name:   "postgresql_live_connected",
			Help:   "1 when the last successful ping succeeded.",
			Value:  live,
			Labels: copyLabels(labels),
		},
		{
			Name:   "postgresql_degraded_unreachable",
			Help:   "1 when running without a successful ping (DegradedMode path or lost connectivity).",
			Value:  degraded,
			Labels: degradedLabels,
		},
		{
			Name:   "postgresql_degraded_mode_uses_total",
			Help:   "Times Init continued after a failed ping/create because DegradedMode was enabled.",
			Value:  float64(c.degradedModeUses.Load()),
			Labels: copyLabels(labels),
			Type:   cf_observability.MetricTypeCounter,
		},
	}

	pool := c.Pool()
	if pool == nil {
		return ms
	}
	st := pool.Stat()
	gauges := []struct {
		name  string
		help  string
		value float64
	}{
		{"postgresql_pool_idle", "PostgreSQL pool idle connections.", float64(st.IdleConns())},
		{"postgresql_pool_total", "PostgreSQL pool total connections.", float64(st.TotalConns())},
		{"postgresql_pool_max", "PostgreSQL pool maximum connections.", float64(st.MaxConns())},
		{"postgresql_pool_acquired", "PostgreSQL pool currently acquired connections.", float64(st.AcquiredConns())},
	}
	for _, g := range gauges {
		ms = append(ms, cf_observability.Metric{Name: g.name, Help: g.help, Value: g.value, Labels: copyLabels(labels)})
	}
	counters := []struct {
		name  string
		help  string
		value float64
	}{
		{"postgresql_pool_acquire_total", "Cumulative successful acquires from the PostgreSQL pool.", float64(st.AcquireCount())},
		{"postgresql_pool_empty_acquire_total", "Cumulative acquires that had to wait for a free connection.", float64(st.EmptyAcquireCount())},
		{"postgresql_pool_canceled_acquire_total", "Cumulative acquires canceled by context.", float64(st.CanceledAcquireCount())},
	}
	for _, ct := range counters {
		ms = append(ms, cf_observability.Metric{Name: ct.name, Help: ct.help, Value: ct.value, Labels: copyLabels(labels), Type: cf_observability.MetricTypeCounter})
	}
	return ms
}

// copyLabels returns a shallow copy of a label map so callers cannot mutate
// the component's internal state.
func copyLabels(m map[string]string) map[string]string {
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

var _ cf.CaerusComponent = (*CFPostgres)(nil)
var _ cf.Dependencies = (*CFPostgres)(nil)
var _ cf.HealthProvider = (*CFPostgres)(nil)
var _ cf_observability.MetricsProvider = (*CFPostgres)(nil)
var _ cf.ConfigReloader = (*CFPostgres)(nil)
var _ cf.ConfigSourceRegistrar = (*CFPostgres)(nil)
var _ cf.JobRunner = (*CFPostgres)(nil)
var _ cf.Migrator = (*CFPostgres)(nil)
