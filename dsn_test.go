package cf_postgres

import "testing"

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
