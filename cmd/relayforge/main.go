// Command relayforge is an HTTP/1.1 reverse proxy and load balancer.
package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/RasikaLakmal/relayforge/internal/proxy"
)

func main() {
	listenAddr := flag.String("listen", ":8080", "address to listen on")
	backendsFlag := flag.String("backends", "", "comma-separated list of backend addresses to load-balance across")
	strategy := flag.String("strategy", proxy.StrategyRoundRobin, "load-balancing strategy: round-robin or least-connections")
	healthInterval := flag.Duration("health-interval", 5*time.Second, "how often to probe each backend")
	healthTimeout := flag.Duration("health-timeout", 2*time.Second, "how long a single health probe waits to connect")
	connectTimeout := flag.Duration("connect-timeout", 3*time.Second, "how long dialing a backend may take")
	headerTimeout := flag.Duration("header-timeout", 10*time.Second, "how long a connection may wait for its next request, and how long that request has to finish once it starts arriving")
	responseTimeout := flag.Duration("response-timeout", 30*time.Second, "how long the whole exchange with a backend (request write plus response read) may take")
	shutdownTimeout := flag.Duration("shutdown-timeout", 10*time.Second, "how long to wait for in-flight requests to finish on shutdown before forcing connections closed")
	maxConnections := flag.Int("max-connections", 0, "maximum concurrent client connections, 0 means unbounded")
	maxInFlight := flag.Int("max-in-flight", 0, "maximum concurrent in-flight requests across all backends, 0 means unbounded; a request past this is rejected with 503")
	metricsListen := flag.String("metrics-listen", "", "address to serve Prometheus-style metrics on at /metrics, empty disables it")
	configPath := flag.String("config", "", "path to a JSON config file; if set, every other flag is ignored and SIGHUP reloads backends/strategy/timeouts/max-in-flight from it without dropping traffic")
	flag.Parse()

	var cfg *resolvedConfig
	if *configPath != "" {
		loaded, err := loadAndValidateConfig(*configPath)
		if err != nil {
			log.Fatalf("relayforge: %v", err)
		}
		cfg = loaded
	} else {
		if *backendsFlag == "" {
			log.Fatal("relayforge: -backends is required")
		}
		cfg = &resolvedConfig{
			Listen:          *listenAddr,
			Backends:        strings.Split(*backendsFlag, ","),
			Strategy:        *strategy,
			HealthInterval:  *healthInterval,
			HealthTimeout:   *healthTimeout,
			ConnectTimeout:  *connectTimeout,
			HeaderTimeout:   *headerTimeout,
			ResponseTimeout: *responseTimeout,
			ShutdownTimeout: *shutdownTimeout,
			MaxConnections:  *maxConnections,
			MaxInFlight:     *maxInFlight,
			MetricsListen:   *metricsListen,
		}
	}

	srv := &proxy.Server{
		ListenAddr:          cfg.Listen,
		Backends:            cfg.Backends,
		Strategy:            cfg.Strategy,
		HealthCheckInterval: cfg.HealthInterval,
		HealthCheckTimeout:  cfg.HealthTimeout,
		ConnectTimeout:      cfg.ConnectTimeout,
		HeaderTimeout:       cfg.HeaderTimeout,
		ResponseTimeout:     cfg.ResponseTimeout,
		MaxConnections:      cfg.MaxConnections,
		MaxInFlight:         cfg.MaxInFlight,
		MetricsListenAddr:   cfg.MetricsListen,
	}

	serveErr := make(chan error, 1)
	go func() {
		serveErr <- srv.ListenAndServe()
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM, syscall.SIGHUP)

	for {
		select {
		case err := <-serveErr:
			if err != nil && !errors.Is(err, proxy.ErrServerClosed) {
				log.Fatalf("relayforge: %v", err)
			}
			return

		case sig := <-sigCh:
			if sig == syscall.SIGHUP {
				if *configPath == "" {
					log.Print("relayforge: received SIGHUP but no -config file was given, nothing to reload")
					continue
				}
				reloaded, err := loadAndValidateConfig(*configPath)
				if err != nil {
					log.Printf("relayforge: reload failed, keeping existing configuration: %v", err)
					continue
				}
				if err := srv.Reload(proxy.ReloadableConfig{
					Backends:            reloaded.Backends,
					Strategy:            reloaded.Strategy,
					HealthCheckInterval: reloaded.HealthInterval,
					HealthCheckTimeout:  reloaded.HealthTimeout,
					ConnectTimeout:      reloaded.ConnectTimeout,
					HeaderTimeout:       reloaded.HeaderTimeout,
					ResponseTimeout:     reloaded.ResponseTimeout,
					MaxInFlight:         reloaded.MaxInFlight,
				}); err != nil {
					log.Printf("relayforge: reload rejected: %v", err)
				}
				continue
			}

			log.Printf("relayforge: received %s, draining in-flight requests (up to %s)", sig, cfg.ShutdownTimeout)
			ctx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
			if err := srv.Shutdown(ctx); err != nil {
				log.Printf("relayforge: shutdown timed out, forced remaining connections closed: %v", err)
			} else {
				log.Print("relayforge: shutdown complete, all connections drained")
			}
			cancel()
			return
		}
	}
}
