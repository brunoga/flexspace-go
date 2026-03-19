// flexsrv exposes a flexkv database as an authenticated HTTPS key-value service.
//
// Usage:
//
//	flexsrv serve [flags]           Start the server
//	flexsrv keygen                  Generate a new API key
//
// Flags (serve):
//
//	--config <file>   JSON config file (see Config struct)
//	--db <path>       Database path (overrides config)
//	--listen <addr>   Listen address, default 0.0.0.0:7700 (overrides config)
//	--cert <file>     TLS certificate file (overrides config)
//	--key <file>      TLS private key file (overrides config)
//	--no-tls          Disable TLS — do not use in production
//
// API (all endpoints require Authorization: Bearer <key>):
//
//	GET  /v1/ping                                       Health check (no auth)
//	GET  /v1/metrics                                    Per-route latency & request metrics
//	GET  /v1/stats                                      Database statistics
//
//	GET  /v1/tables                                     List tables
//	POST /v1/tables                                     Create table  {"name":"t"}
//	DEL  /v1/tables/{table}                             Drop table
//	GET  /v1/tables/{table}/count                       Count entries
//	GET  /v1/tables/{table}/stats                       Table statistics
//
//	GET  /v1/tables/{table}/keys/{key...}               Get value (raw bytes)
//	PUT  /v1/tables/{table}/keys/{key...}               Put value (raw bytes body)
//	DEL  /v1/tables/{table}/keys/{key...}               Delete key
//	HEAD /v1/tables/{table}/keys/{key...}               Exists check
//
//	GET  /v1/tables/{table}/scan?start=&end=&prefix=&limit=N
//	     Streams NDJSON: {"key":"...","value":"<base64>"}
//
//	GET  /v1/tables/{table}/indexes                     List indexes
//	POST /v1/tables/{table}/indexes                     Create index (SchemaIndex JSON)
//	DEL  /v1/tables/{table}/indexes/{index}             Drop index
//	GET  /v1/tables/{table}/indexes/{index}/scan?start=&end=&prefix=&value=&limit=N&records=true
//	     Streams NDJSON: {"indexed_value":"...","primary_key":"...","record":"<base64>"}
//
//	POST /v1/batch                                      Atomic batch (BatchFile JSON)
//	GET  /v1/schema                                     Dump schema
//	POST /v1/schema                                     Load/apply schema
//	GET  /v1/dump                                       Dump all data (NDJSON)
//	POST /v1/dump                                       Load data (NDJSON)
package main

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/pem"
	"flag"
	"fmt"
	"log/slog"
	"math/big"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/brunoga/flexspace-go/flexkv"
)

func main() {
	if len(os.Args) < 2 || os.Args[1] == "serve" {
		// shift args past "serve" if present
		if len(os.Args) >= 2 && os.Args[1] == "serve" {
			os.Args = append(os.Args[:1], os.Args[2:]...)
		}
		cmdServe()
		return
	}
	switch os.Args[1] {
	case "keygen":
		cmdKeygen()
	default:
		fmt.Fprintf(os.Stderr, "usage: flexsrv [serve|keygen] [flags]\n")
		os.Exit(2)
	}
}

func cmdKeygen() {
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		slog.Error("keygen", "err", err)
		os.Exit(1)
	}
	key := "flex_" + base64.RawURLEncoding.EncodeToString(raw)
	h := sha256.Sum256([]byte(key))
	fmt.Printf("Key:  %s\n", key)
	fmt.Printf("Hash: sha256:%x\n", h)
	fmt.Println()
	fmt.Println("Add the hash to your config's auth.keys list.")
	fmt.Println("The raw key is shown only once — store it securely.")
}

func cmdServe() {
	fs := flag.NewFlagSet("flexsrv serve", flag.ExitOnError)
	configFile := fs.String("config", "", "JSON config file")
	dbPath     := fs.String("db", "", "database path")
	listen     := fs.String("listen", "", "listen address (default 0.0.0.0:7700)")
	certFile   := fs.String("cert", "", "TLS certificate PEM file")
	keyFile    := fs.String("key", "", "TLS private key PEM file")
	noTLS      := fs.Bool("no-tls", false, "disable TLS (not for production)")
	logFormat  := fs.String("log-format", "text", "log format: text or json")
	logLevel   := fs.String("log-level", "info", "log level: debug, info, warn, error")
	fs.Parse(os.Args[1:]) //nolint:errcheck

	initLogger(*logFormat, *logLevel)

	cfg, err := loadConfig(*configFile)
	if err != nil {
		slog.Error("config", "err", err)
		os.Exit(1)
	}
	// CLI flags override config file.
	if *dbPath   != "" { cfg.DBPath         = *dbPath   }
	if *listen   != "" { cfg.Listen         = *listen   }
	if *certFile != "" { cfg.TLS.CertFile   = *certFile }
	if *keyFile  != "" { cfg.TLS.KeyFile    = *keyFile  }
	if *noTLS         { cfg.TLS.Disabled    = true      }

	if cfg.DBPath == "" {
		slog.Error("database path required: --db <path> or config.db_path")
		os.Exit(1)
	}
	if cfg.Listen == "" {
		cfg.Listen = "0.0.0.0:7700"
	}

	db, err := flexkv.Open(cfg.DBPath, nil)
	if err != nil {
		slog.Error("open db", "err", err)
		os.Exit(1)
	}
	defer db.Close()

	ss, err := loadAndRegisterSchema(db, cfg.DBPath)
	if err != nil {
		slog.Error("schema", "err", err)
		os.Exit(1)
	}

	// Startup banner — emitted after schema load so we have the table count.
	scheme := "https"
	tlsMode := "self-signed"
	if cfg.TLS.Disabled {
		scheme = "http"
		tlsMode = "disabled"
	} else if cfg.TLS.CertFile != "" {
		tlsMode = "cert-file"
	}
	tableCount := 0
	if tables, err := db.Tables(); err == nil {
		tableCount = len(tables)
	}
	slog.Info("flexsrv starting",
		"version", version,
		"pid", os.Getpid(),
		"db", cfg.DBPath,
		"addr", scheme+"://"+cfg.Listen,
		"tls", tlsMode,
		"auth_keys", len(cfg.Auth.Keys),
		"tables", tableCount,
		"max_scan_results", cfg.Limits.MaxScanResults,
		"max_value_bytes", cfg.Limits.MaxValueBytes,
		"log_level", *logLevel,
	)

	srv := newServer(db, ss, cfg)
	httpSrv := &http.Server{
		Addr:        cfg.Listen,
		Handler:     srv.routes(),
		ReadTimeout: 30 * time.Second,
		// WriteTimeout left at 0: scan/dump responses stream indefinitely.
		IdleTimeout: 120 * time.Second,
	}

	if !cfg.TLS.Disabled {
		tlsCfg, err := buildTLSConfig(cfg)
		if err != nil {
			slog.Error("tls", "err", err)
			os.Exit(1)
		}
		httpSrv.TLSConfig = tlsCfg
		if cfg.TLS.CertFile == "" {
			slog.Warn("using auto-generated self-signed certificate — not for production")
		}
	}

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		var listenErr error
		if cfg.TLS.Disabled {
			listenErr = httpSrv.ListenAndServe()
		} else {
			listenErr = httpSrv.ListenAndServeTLS(cfg.TLS.CertFile, cfg.TLS.KeyFile)
		}
		if listenErr != nil && listenErr != http.ErrServerClosed {
			slog.Error("listen", "err", listenErr)
			os.Exit(1)
		}
	}()

	slog.Info("ready")

	<-stop
	slog.Info("shutting down")
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := httpSrv.Shutdown(ctx); err != nil {
		slog.Error("shutdown", "err", err)
	}
	slog.Info("stopped")
}

// initLogger configures the slog default logger.
func initLogger(format, level string) {
	var lvl slog.Level
	switch level {
	case "debug":
		lvl = slog.LevelDebug
	case "warn":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}
	opts := &slog.HandlerOptions{Level: lvl}
	var h slog.Handler
	if format == "json" {
		h = slog.NewJSONHandler(os.Stderr, opts)
	} else {
		h = slog.NewTextHandler(os.Stderr, opts)
	}
	slog.SetDefault(slog.New(h))
}

func buildTLSConfig(cfg *Config) (*tls.Config, error) {
	tlsCfg := &tls.Config{MinVersion: tls.VersionTLS12}
	if cfg.TLS.CertFile == "" {
		cert, err := generateSelfSignedCert()
		if err != nil {
			return nil, fmt.Errorf("generate self-signed cert: %w", err)
		}
		tlsCfg.Certificates = []tls.Certificate{cert}
	}
	return tlsCfg, nil
}

func generateSelfSignedCert() (tls.Certificate, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return tls.Certificate{}, err
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return tls.Certificate{}, err
	}
	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{Organization: []string{"flexsrv"}},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(10 * 365 * 24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:  []net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback},
		DNSNames:     []string{"localhost"},
	}
	certDER, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, key.Public(), key)
	if err != nil {
		return tls.Certificate{}, err
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return tls.Certificate{}, err
	}
	return tls.X509KeyPair(
		pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER}),
		pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}),
	)
}
