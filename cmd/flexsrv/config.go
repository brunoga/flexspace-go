package main

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
)

// Config is the top-level server configuration, loaded from a JSON file.
// All fields can be overridden by CLI flags.
//
// Example config.json:
//
//	{
//	  "db_path": "/var/lib/flexsrv/data",
//	  "listen":  "0.0.0.0:7700",
//	  "tls": {
//	    "cert_file": "/etc/flexsrv/cert.pem",
//	    "key_file":  "/etc/flexsrv/key.pem"
//	  },
//	  "auth": {
//	    "keys": [
//	      {"id": "admin-key", "hash": "sha256:<hex>", "role": "admin"},
//	      {"id": "ro-key",    "hash": "sha256:<hex>", "role": "read"}
//	    ]
//	  },
//	  "limits": {
//	    "max_scan_results": 10000,
//	    "max_value_bytes":  67108864
//	  }
//	}
type Config struct {
	DBPath string     `json:"db_path"`
	Listen string     `json:"listen"`
	TLS    TLSConfig  `json:"tls"`
	Auth   AuthConfig `json:"auth"`
	Limits Limits     `json:"limits"`
}

// TLSConfig holds TLS settings.
// If CertFile/KeyFile are both empty and Disabled is false, an in-memory
// self-signed certificate is generated automatically (development mode).
type TLSConfig struct {
	Disabled bool   `json:"disabled"`
	CertFile string `json:"cert_file"`
	KeyFile  string `json:"key_file"`
}

// AuthConfig holds authentication settings.
type AuthConfig struct {
	Keys []APIKeyConfig `json:"keys"`
}

// APIKeyConfig is one entry in the API key table.
// Hash must be of the form "sha256:<lowercase-hex>" as printed by `flexsrv keygen`.
// Role must be one of "read", "write", or "admin".
//
//   - read  — Get, Scan, index queries, stats
//   - write — read + Put, Delete, Batch
//   - admin — write + CreateTable, DropTable, CreateIndex, DropIndex, schema ops
type APIKeyConfig struct {
	ID   string `json:"id"`   // human-readable label
	Hash string `json:"hash"` // "sha256:<hex>"
	Role string `json:"role"` // "read" | "write" | "admin"
}

// Limits caps resource usage per request.
type Limits struct {
	MaxScanResults int `json:"max_scan_results"` // default 10 000
	MaxValueBytes  int `json:"max_value_bytes"`  // default 64 MiB; storage layer cap is 1 GiB
}

// loadConfig parses the JSON config file at path. An empty path returns
// defaults with no API keys configured.
func loadConfig(path string) (*Config, error) {
	cfg := &Config{
		Limits: Limits{
			MaxScanResults: 10_000,
			MaxValueBytes:  64 << 20, // 64 MiB
		},
	}
	if path == "" {
		return cfg, nil
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	if err := json.NewDecoder(f).Decode(cfg); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	if cfg.Limits.MaxScanResults <= 0 {
		cfg.Limits.MaxScanResults = 10_000
	}
	if cfg.Limits.MaxValueBytes <= 0 {
		cfg.Limits.MaxValueBytes = 64 << 20
	}
	return cfg, nil
}

// authenticate validates a bearer token and returns the role ("read", "write",
// "admin") or "" if the token is not recognised.
func (cfg *Config) authenticate(token string) string {
	h := sha256.Sum256([]byte(token))
	want := fmt.Sprintf("sha256:%x", h)
	for _, k := range cfg.Auth.Keys {
		if k.Hash == want {
			return k.Role
		}
	}
	return ""
}
