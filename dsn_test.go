package cf_postgres

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"

	cf_configuration "github.com/caerus-framework/caerus-framework-configuration"
)

func TestParseDSN(t *testing.T) {
	cfg, err := ParseDSN("postgres://alice:s3cret@db.example:5433/appdb?sslmode=require")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Host != "db.example" || cfg.Port != 5433 || cfg.User != "alice" || cfg.Password != "s3cret" || cfg.Database != "appdb" {
		t.Fatalf("unexpected config: %+v", cfg)
	}
	if cfg.SSLMode != "require" {
		t.Fatalf("SSLMode = %q", cfg.SSLMode)
	}
}

func TestParseDSNInvalidDoesNotEchoPassword(t *testing.T) {
	_, err := ParseDSN("postgres://alice:LEAKSECRET@[")
	if err == nil {
		t.Fatal("want parse error")
	}
	if strings.Contains(err.Error(), "LEAKSECRET") {
		t.Fatalf("password leaked in error: %v", err)
	}
}

func TestPostgresConfigLogArgsNeverCleartext(t *testing.T) {
	cfg := PostgresConfig{Host: "db.example", Password: "s3cret-value", Database: "app"}
	var buf bytes.Buffer
	l := slog.New(slog.NewTextHandler(&buf, nil))
	l.Info("summary", cf_configuration.LogArgs(cfg)...)
	out := buf.String()
	if strings.Contains(out, "s3cret-value") {
		t.Fatalf("password leaked: %s", out)
	}
	if !strings.Contains(out, "[redacted]") {
		t.Fatalf("want [redacted] in %s", out)
	}
	if !strings.Contains(out, "host=db.example") {
		t.Fatalf("host should stay visible: %s", out)
	}
}

func TestOverlayDSNWins(t *testing.T) {
	cfg := PostgresConfig{Host: "file-host", Port: 1, User: "file-user", Database: "file-db"}
	if err := OverlayDSN(&cfg, "postgres://env:pw@env-host:9999/envdb?sslmode=disable"); err != nil {
		t.Fatal(err)
	}
	if cfg.Host != "env-host" || cfg.Port != 9999 || cfg.User != "env" || cfg.Password != "pw" || cfg.Database != "envdb" {
		t.Fatalf("overlay failed: %+v", cfg)
	}
	if cfg.SSLMode != "disable" {
		t.Fatalf("SSLMode = %q", cfg.SSLMode)
	}
}
