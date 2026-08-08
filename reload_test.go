package cf_postgres

import (
	"testing"

	cf_configuration "github.com/caerus-framework/caerus-framework-configuration"
)

func TestWithConfigSourceDeclaresConfigurationDependency(t *testing.T) {
	p := New(WithConfigSource("postgresql", ""))
	deps := p.GetDependencies()
	found := false
	for _, d := range deps {
		if d == cf_configuration.ComponentName {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("deps = %v, want %q", deps, cf_configuration.ComponentName)
	}
}

func TestOnConfigReloadNoopWithoutSource(t *testing.T) {
	p := New()
	// Must not panic when unbound / not initialized.
	p.OnConfigReload("postgresql", nil)
}
