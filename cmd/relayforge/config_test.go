package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// writeConfigFile marshals cfg to JSON and writes it to a temp file,
// returning the path.
func writeConfigFile(t *testing.T, cfg Config) string {
	t.Helper()
	data, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("marshal config: %v", err)
	}
	path := filepath.Join(t.TempDir(), "relayforge.json")
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("write config file: %v", err)
	}
	return path
}

func validConfig() Config {
	return Config{
		Listen:   "127.0.0.1:8080",
		Backends: []string{"127.0.0.1:9090", "127.0.0.1:9091"},
	}
}

func TestLoadAndValidateConfigAcceptsValidFile(t *testing.T) {
	cfg := validConfig()
	cfg.Strategy = "least-connections"
	cfg.HealthInterval = "5s"
	cfg.MaxInFlight = 100
	path := writeConfigFile(t, cfg)

	resolved, err := loadAndValidateConfig(path)
	if err != nil {
		t.Fatalf("loadAndValidateConfig: %v", err)
	}
	if resolved.Listen != cfg.Listen {
		t.Errorf("Listen = %q, want %q", resolved.Listen, cfg.Listen)
	}
	if len(resolved.Backends) != 2 {
		t.Errorf("Backends = %v, want 2 entries", resolved.Backends)
	}
	if resolved.HealthInterval.String() != "5s" {
		t.Errorf("HealthInterval = %v, want 5s", resolved.HealthInterval)
	}
	if resolved.MaxInFlight != 100 {
		t.Errorf("MaxInFlight = %d, want 100", resolved.MaxInFlight)
	}
}

func TestLoadAndValidateConfigRejectsMissingFile(t *testing.T) {
	if _, err := loadAndValidateConfig(filepath.Join(t.TempDir(), "nope.json")); err == nil {
		t.Fatal("expected an error for a missing config file, got nil")
	}
}

func TestLoadAndValidateConfigRejectsMalformedJSON(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bad.json")
	if err := os.WriteFile(path, []byte("{not json"), 0o644); err != nil {
		t.Fatalf("write bad config file: %v", err)
	}
	if _, err := loadAndValidateConfig(path); err == nil {
		t.Fatal("expected an error for malformed JSON, got nil")
	}
}

func TestConfigValidation(t *testing.T) {
	cases := []struct {
		name   string
		modify func(*Config)
	}{
		{"missing listen", func(c *Config) { c.Listen = "" }},
		{"empty backends", func(c *Config) { c.Backends = nil }},
		{"malformed backend address", func(c *Config) { c.Backends = []string{"not-a-host-port"} }},
		{"duplicate backend", func(c *Config) { c.Backends = []string{"127.0.0.1:9090", "127.0.0.1:9090"} }},
		{"unrecognized strategy", func(c *Config) { c.Strategy = "random" }},
		{"unparseable health_interval", func(c *Config) { c.HealthInterval = "banana" }},
		{"non-positive health_interval", func(c *Config) { c.HealthInterval = "0s" }},
		{"negative max_connections", func(c *Config) { c.MaxConnections = -1 }},
		{"negative max_in_flight", func(c *Config) { c.MaxInFlight = -1 }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cfg := validConfig()
			c.modify(&cfg)
			if _, err := cfg.validate(); err == nil {
				t.Fatalf("expected validate() to reject %s, got nil error", c.name)
			}
		})
	}
}

func TestConfigValidationAcceptsEmptyOptionalDurations(t *testing.T) {
	cfg := validConfig() // every duration field left as "", every count left as 0
	resolved, err := cfg.validate()
	if err != nil {
		t.Fatalf("validate() on minimal valid config: %v", err)
	}
	if resolved.HealthInterval != 0 || resolved.ConnectTimeout != 0 {
		t.Fatalf("expected zero durations to resolve to 0 (proxy package default), got HealthInterval=%v ConnectTimeout=%v", resolved.HealthInterval, resolved.ConnectTimeout)
	}
}
