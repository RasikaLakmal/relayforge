package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"time"

	"github.com/RasikaLakmal/relayforge/internal/proxy"
)

// Config is the on-disk JSON configuration format for relayforge. Every
// field matches a command-line flag of the same purpose; durations are
// strings (e.g. "5s") rather than nanosecond integers, since this file is
// meant to be hand-edited, not just machine-generated.
type Config struct {
	Listen          string   `json:"listen"`
	Backends        []string `json:"backends"`
	Strategy        string   `json:"strategy,omitempty"`
	HealthInterval  string   `json:"health_interval,omitempty"`
	HealthTimeout   string   `json:"health_timeout,omitempty"`
	ConnectTimeout  string   `json:"connect_timeout,omitempty"`
	HeaderTimeout   string   `json:"header_timeout,omitempty"`
	ResponseTimeout string   `json:"response_timeout,omitempty"`
	ShutdownTimeout string   `json:"shutdown_timeout,omitempty"`
	MaxConnections  int      `json:"max_connections,omitempty"`
	MaxInFlight     int      `json:"max_in_flight,omitempty"`
	MetricsListen   string   `json:"metrics_listen,omitempty"`
}

// resolvedConfig is Config after validation: durations parsed, defaults
// left as zero (the proxy package itself applies its own defaults for
// anything zero), and every value known to be at least well-formed.
type resolvedConfig struct {
	Listen          string
	Backends        []string
	Strategy        string
	HealthInterval  time.Duration
	HealthTimeout   time.Duration
	ConnectTimeout  time.Duration
	HeaderTimeout   time.Duration
	ResponseTimeout time.Duration
	ShutdownTimeout time.Duration
	MaxConnections  int
	MaxInFlight     int
	MetricsListen   string
}

// loadAndValidateConfig reads path as JSON and validates it, rejecting
// anything malformed (bad backend addresses, an unrecognized strategy,
// an unparseable or non-positive duration, a negative count) before the
// caller ever tries to start or reload a server with it, rather than
// discovering the problem at runtime.
func loadAndValidateConfig(path string) (*resolvedConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config file: %w", err)
	}
	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse config file: %w", err)
	}
	return cfg.validate()
}

func (c *Config) validate() (*resolvedConfig, error) {
	if c.Listen == "" {
		return nil, errors.New("listen is required")
	}
	if len(c.Backends) == 0 {
		return nil, errors.New("at least one backend is required")
	}
	seen := make(map[string]bool, len(c.Backends))
	for _, b := range c.Backends {
		if _, _, err := net.SplitHostPort(b); err != nil {
			return nil, fmt.Errorf("invalid backend address %q: %w", b, err)
		}
		if seen[b] {
			return nil, fmt.Errorf("duplicate backend %q", b)
		}
		seen[b] = true
	}
	switch c.Strategy {
	case "", proxy.StrategyRoundRobin, proxy.StrategyLeastConnections:
	default:
		return nil, fmt.Errorf("unrecognized strategy %q", c.Strategy)
	}
	if c.MaxConnections < 0 {
		return nil, fmt.Errorf("max_connections must be >= 0, got %d", c.MaxConnections)
	}
	if c.MaxInFlight < 0 {
		return nil, fmt.Errorf("max_in_flight must be >= 0, got %d", c.MaxInFlight)
	}

	healthInterval, err := parsePositiveDuration("health_interval", c.HealthInterval)
	if err != nil {
		return nil, err
	}
	healthTimeout, err := parsePositiveDuration("health_timeout", c.HealthTimeout)
	if err != nil {
		return nil, err
	}
	connectTimeout, err := parsePositiveDuration("connect_timeout", c.ConnectTimeout)
	if err != nil {
		return nil, err
	}
	headerTimeout, err := parsePositiveDuration("header_timeout", c.HeaderTimeout)
	if err != nil {
		return nil, err
	}
	responseTimeout, err := parsePositiveDuration("response_timeout", c.ResponseTimeout)
	if err != nil {
		return nil, err
	}
	shutdownTimeout, err := parsePositiveDuration("shutdown_timeout", c.ShutdownTimeout)
	if err != nil {
		return nil, err
	}

	return &resolvedConfig{
		Listen:          c.Listen,
		Backends:        c.Backends,
		Strategy:        c.Strategy,
		HealthInterval:  healthInterval,
		HealthTimeout:   healthTimeout,
		ConnectTimeout:  connectTimeout,
		HeaderTimeout:   headerTimeout,
		ResponseTimeout: responseTimeout,
		ShutdownTimeout: shutdownTimeout,
		MaxConnections:  c.MaxConnections,
		MaxInFlight:     c.MaxInFlight,
		MetricsListen:   c.MetricsListen,
	}, nil
}

// parsePositiveDuration parses s (if non-empty) as a Go duration string
// and requires it to be positive; an empty string resolves to zero,
// which the proxy package treats as "use the built-in default."
func parsePositiveDuration(name, s string) (time.Duration, error) {
	if s == "" {
		return 0, nil
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return 0, fmt.Errorf("invalid %s %q: %w", name, s, err)
	}
	if d <= 0 {
		return 0, fmt.Errorf("%s must be positive, got %q", name, s)
	}
	return d, nil
}
