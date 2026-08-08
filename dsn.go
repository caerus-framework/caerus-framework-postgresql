package cf_postgres

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ParseDSN parses a PostgreSQL connection URL or keyword/value DSN into a
// PostgresConfig. Supported forms are those accepted by pgxpool.ParseConfig
// (e.g. postgres://user:pass@host:5432/db?sslmode=require).
func ParseDSN(dsn string) (PostgresConfig, error) {
	var zero PostgresConfig
	if strings.TrimSpace(dsn) == "" {
		return zero, fmt.Errorf("cf_postgres: empty DSN")
	}
	pc, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return zero, fmt.Errorf("cf_postgres: parse DSN: %w", err)
	}
	cfg := PostgresConfig{
		Host:     pc.ConnConfig.Host,
		Port:     int(pc.ConnConfig.Port),
		User:     pc.ConnConfig.User,
		Password: pc.ConnConfig.Password,
		Database: pc.ConnConfig.Database,
	}
	if pc.MaxConns > 0 {
		cfg.MaxConns = pc.MaxConns
	}
	if pc.MinConns > 0 {
		cfg.MinConns = pc.MinConns
	}
	if rtl := pc.ConnConfig.RuntimeParams["sslmode"]; rtl != "" {
		cfg.SSLMode = rtl
	} else if sm := sslModeFromDSN(dsn); sm != "" {
		cfg.SSLMode = sm
	}
	return cfg, nil
}

// OverlayDSN merges connection fields from dsn into cfg. DSN-derived fields
// win over existing values (file/env). Pool sizing from the DSN is applied
// only when the parsed pool sets a non-zero value.
func OverlayDSN(cfg *PostgresConfig, dsn string) error {
	if cfg == nil {
		return fmt.Errorf("cf_postgres: OverlayDSN nil config")
	}
	parsed, err := ParseDSN(dsn)
	if err != nil {
		return err
	}
	if parsed.Host != "" {
		cfg.Host = parsed.Host
	}
	if parsed.Port != 0 {
		cfg.Port = parsed.Port
	}
	if parsed.User != "" {
		cfg.User = parsed.User
	}
	if parsed.Password != "" {
		cfg.Password = parsed.Password
	}
	if parsed.Database != "" {
		cfg.Database = parsed.Database
	}
	if parsed.SSLMode != "" {
		cfg.SSLMode = parsed.SSLMode
	}
	if parsed.MaxConns != 0 {
		cfg.MaxConns = parsed.MaxConns
	}
	if parsed.MinConns != 0 {
		cfg.MinConns = parsed.MinConns
	}
	return nil
}

func sslModeFromDSN(dsn string) string {
	if strings.Contains(dsn, "://") {
		u, err := url.Parse(dsn)
		if err != nil {
			return ""
		}
		return u.Query().Get("sslmode")
	}
	// keyword/value: sslmode=require
	for _, part := range strings.Fields(dsn) {
		k, v, ok := strings.Cut(part, "=")
		if ok && strings.EqualFold(k, "sslmode") {
			return v
		}
	}
	return ""
}
