# flexsrv

`flexsrv` exposes a flexkv database as an authenticated HTTPS key-value service. It supports the full flexkv feature set — tables, secondary indexes, atomic batches, schema management, and data import/export — over a clean HTTP API.

[flexctl](../flexctl/) can connect to flexsrv directly, using it as a drop-in replacement for a local database path.

---

## Installation

```bash
cd flexspace-go
go build -o flexsrv ./cmd/flexsrv/
```

---

## Quick start

```bash
# 1. Generate an API key
./flexsrv keygen
#   Key:  flex_MwxLBkhDy8xOIkJU91z9IhSCTjQ3P_xUQt55ySWRo3s
#   Hash: sha256:6f24db885c2097ce1fc540b902f9cbb57b62aa98331cbac4f4002610cad75f67

# 2. Write a config file
cat > config.json <<'EOF'
{
  "db_path": "/data/mydb",
  "listen":  "0.0.0.0:7700",
  "auth": {
    "keys": [
      { "id": "admin", "hash": "sha256:6f24db885c2097ce1fc540b902f9cbb57b62aa98331cbac4f4002610cad75f67", "role": "admin" }
    ]
  }
}
EOF

# 3. Start the server (auto-generates a self-signed TLS cert for dev)
./flexsrv serve --config config.json
# time=... level=WARN  msg="using auto-generated self-signed certificate — not for production"
# time=... level=INFO  msg="flexsrv starting" version=0.0.1 pid=... db=/data/mydb addr=https://0.0.0.0:7700 tls=self-signed auth_keys=1 tables=0 max_scan_results=10000 max_value_bytes=4096 log_level=info
# time=... level=INFO  msg="ready"

# 4. Test with curl
curl -sk -H "Authorization: Bearer flex_MwxLB..." https://localhost:7700/v1/ping
# {"status":"ok"}
```

---

## Configuration

All settings can be provided via a JSON config file and overridden with CLI flags.

### Config file

```json
{
  "db_path": "/var/lib/flexsrv/data",
  "listen":  "0.0.0.0:7700",

  "tls": {
    "cert_file": "/etc/flexsrv/cert.pem",
    "key_file":  "/etc/flexsrv/key.pem"
  },

  "auth": {
    "keys": [
      { "id": "admin-key", "hash": "sha256:<hex>", "role": "admin"  },
      { "id": "app-key",   "hash": "sha256:<hex>", "role": "write"  },
      { "id": "ro-key",    "hash": "sha256:<hex>", "role": "read"   }
    ]
  },

  "limits": {
    "max_scan_results": 10000,
    "max_value_bytes":  4096
  }
}
```

### CLI flags (`flexsrv serve`)

| Flag | Default | Description |
|------|---------|-------------|
| `--config <file>` | — | JSON config file |
| `--db <path>` | — | Database path (required if not in config) |
| `--listen <addr>` | `0.0.0.0:7700` | Listen address |
| `--cert <file>` | — | TLS certificate PEM |
| `--key <file>` | — | TLS private key PEM |
| `--no-tls` | false | Disable TLS — do not use in production |
| `--log-format` | `text` | Log format: `text` or `json` |
| `--log-level` | `info` | Log level: `debug`, `info`, `warn`, `error` |

CLI flags override the config file.

---

## Authentication

Every request (except `GET /v1/ping`) must include an API key:

```
Authorization: Bearer <key>
```

### Generating a key

```bash
./flexsrv keygen
```

This prints a random key and its SHA-256 hash:

```
Key:  flex_MwxLBkhDy8xOIkJU91z9IhSCTjQ3P_xUQt55ySWRo3s
Hash: sha256:6f24db885c2097ce1fc540b902f9cbb57b62aa98331cbac4f4002610cad75f67
```

- Add the **hash** to `config.json` under `auth.keys`.
- Give the **raw key** to the client. It cannot be recovered from the hash.

### Roles

| Role | Allowed operations |
|------|--------------------|
| `read` | GET, HEAD, scan, stats, schema dump, data dump |
| `write` | read + PUT, DELETE, batch, data load |
| `admin` | write + create/drop table, create/drop index, schema load |

Assign the least-privileged role appropriate for each client.

---

## TLS

### Production — provide a certificate

```json
"tls": {
  "cert_file": "/etc/flexsrv/cert.pem",
  "key_file":  "/etc/flexsrv/key.pem"
}
```

Any PEM-encoded certificate chain and private key works (Let's Encrypt, internal CA, etc.). TLS 1.2 is the minimum accepted version.

### Development — auto-generated self-signed certificate

If `cert_file` and `key_file` are both omitted, flexsrv generates an in-memory ECDSA P-256 certificate at startup. The server logs a warning:

```
flexsrv: WARNING — using auto-generated self-signed certificate
```

Clients connecting to a self-signed server must skip certificate verification (`--insecure` in flexctl, `-k` in curl).

### Disabling TLS (not for production)

```bash
./flexsrv serve --db /tmp/mydb --no-tls
```

---

## Schema persistence

flexsrv stores index definitions in `flexsrv_schema.json` inside the database directory. On restart, all registered indexers are re-attached automatically — no manual `schema load` is required.

The schema file uses the same JSON format as `flexctl schema dump`, so the two tools are interoperable:

```bash
# Export schema from a local DB and load it into a remote server
flexctl /local/db schema dump | curl -sk -X POST \
  -H "Authorization: Bearer $FLEXSRV_KEY" \
  -H "Content-Type: application/json" \
  -d @- https://host:7700/v1/schema
```

---

## API reference

All endpoints live under `/v1`. Responses use `application/json` unless noted. Errors always return JSON: `{"error": "message"}`.

### Authentication header

```
Authorization: Bearer <key>
```

`GET /v1/ping` is the only endpoint that does not require authentication.

---

### System

#### `GET /v1/ping`
Health check — no authentication required.

```json
{ "status": "ok" }
```

#### `GET /v1/metrics` — read
Per-route request counts, error counts, active in-flight requests, and latency statistics (mean, min, max, p50, p90, p95, p99) with a cumulative millisecond histogram. Resets when the server restarts.

```json
{
  "uptime_seconds": 3600,
  "routes": {
    "GET /v1/tables/{table}/keys/{key...}": {
      "requests": 10000,
      "errors": 5,
      "active": 2,
      "latency_ms": {
        "mean": 1.23,
        "min": 0.05,
        "max": 45.3,
        "p50": 0.85,
        "p90": 2.4,
        "p95": 3.1,
        "p99": 12.4,
        "histogram": {
          "le_0.5": 4500, "le_1": 7800, "le_2.5": 9100,
          "le_5": 9700,   "le_10": 9900, "le_25": 9970,
          "le_50": 9990,  "le_100": 9998, "le_250": 9999,
          "le_500": 9999, "le_1000": 10000, "+Inf": 10000
        }
      }
    }
  }
}
```

Histogram values are **cumulative** (each bucket includes all smaller buckets), matching the Prometheus convention.

#### `GET /v1/stats` — read
Database and disk statistics.

```json
{
  "seq": 2847,
  "table_count": 7,
  "active_mt_bytes": 128,
  "inactive_mt_bytes": 0,
  "wal0_bytes": 4096,
  "wal1_bytes": 4096,
  "registry_bytes": 4096,
  "tables_dir_bytes": 12632064
}
```

---

### Tables

#### `GET /v1/tables` — read
List all user tables.

```json
{ "tables": ["orders", "users"] }
```

#### `POST /v1/tables` — admin
Create a table. Idempotent.

```json
{ "name": "users" }
```

Returns `201 Created`:
```json
{ "name": "users" }
```

#### `DELETE /v1/tables/{table}` — admin
Drop a table and all its secondary indexes. Returns `204 No Content`.

#### `GET /v1/tables/{table}/count` — read

```json
{ "count": 1042 }
```

#### `GET /v1/tables/{table}/stats` — read
Per-table and per-index storage statistics.

```json
{
  "file_bytes": 3276,
  "anchor_count": 47,
  "cache_used": 0,
  "cache_cap": 67108864,
  "indexes": [
    { "name": "by_dept",  "file_bytes": 1843, "anchor_count": 18 },
    { "name": "by_level", "file_bytes": 1126, "anchor_count": 11 }
  ]
}
```

---

### Key-value operations

Keys are URL-path segments — percent-encode any special characters. Slashes in keys are supported via path remainder matching.

#### `GET /v1/tables/{table}/keys/{key}` — read
Returns the raw value as `application/octet-stream`. `404` if not found.

#### `PUT /v1/tables/{table}/keys/{key}` — write
Request body is the raw value (`application/octet-stream`). Returns `204 No Content`.

```bash
curl -sk -X PUT \
  -H "Authorization: Bearer $KEY" \
  -H "Content-Type: application/octet-stream" \
  --data-binary "engineering,senior" \
  https://host:7700/v1/tables/users/keys/alice
```

#### `DELETE /v1/tables/{table}/keys/{key}` — write
Returns `204 No Content`.

#### `HEAD /v1/tables/{table}/keys/{key}` — read
Returns `200` if the key exists, `404` otherwise. No body.

---

### Scanning

#### `GET /v1/tables/{table}/scan` — read

Streams results as **NDJSON** (`application/x-ndjson`), one JSON object per line. Values are base64-encoded.

Query parameters:

| Parameter | Description |
|-----------|-------------|
| `start` | Range start (inclusive). Omit for beginning. |
| `end` | Range end (exclusive). Omit for no upper bound. |
| `prefix` | Prefix scan. Overrides `start`/`end`. |
| `limit` | Maximum results. Defaults to server `max_scan_results` (10 000). |

```
{"key":"alice","value":"ZW5naW5lZXJpbmcsc2VuaW9y"}
{"key":"bob","value":"ZGVzaWduLG1pZA=="}
```

```bash
# Range scan
curl -sk -H "Authorization: Bearer $KEY" \
  "https://host:7700/v1/tables/users/scan?start=alice&end=carol&limit=50"

# Prefix scan
curl -sk -H "Authorization: Bearer $KEY" \
  "https://host:7700/v1/tables/users/scan?prefix=ali"
```

---

### Secondary indexes

#### `GET /v1/tables/{table}/indexes` — read

```json
{
  "indexes": [
    { "name": "by_dept",  "type": "field",  "field": 0, "delim": "," },
    { "name": "by_role",  "type": "exact" },
    { "name": "by_pfx",   "type": "prefix", "length": 4 }
  ]
}
```

#### `POST /v1/tables/{table}/indexes` — admin
Create an index and populate it from existing data. Idempotent.

Request body is a `SchemaIndex` object:

```json
{ "name": "by_dept", "type": "field", "field": 0, "delim": "," }
```

| Type | Required fields | Description |
|------|----------------|-------------|
| `exact` | — | Entire value |
| `prefix` | `length` | First `length` bytes |
| `suffix` | `length` | Last `length` bytes |
| `field` | `field` (int), optionally `delim` | Field `field` (0-based), split by `delim` (default `,`) |

Returns `201 Created` with the echo of the index spec.

#### `DELETE /v1/tables/{table}/indexes/{index}` — admin
Drop an index. Returns `204 No Content`.

#### `GET /v1/tables/{table}/indexes/{index}/count` — read
Count the number of entries in an index. For single-valued indexes this equals the primary table count; for multi-valued indexes it may be higher.

```json
{ "count": 1042 }
```

#### `GET /v1/tables/{table}/indexes/{index}/scan` — read

Streams index entries as **NDJSON**. Values are strings; records are base64-encoded.

Query parameters:

| Parameter | Description |
|-----------|-------------|
| `value` | Exact match on indexed value |
| `prefix` | Prefix scan on indexed value |
| `start` / `end` | Range scan on indexed value |
| `limit` | Maximum results |
| `records=true` | Include the full record value in each row |

```
{"indexed_value":"engineering","primary_key":"alice","record":"ZW5naW5lZXJpbmcsc2VuaW9y"}
{"indexed_value":"engineering","primary_key":"carol","record":"ZW5naW5lZXJpbmcsanVuaW9y"}
```

```bash
# Exact match
curl -sk -H "Authorization: Bearer $KEY" \
  "https://host:7700/v1/tables/users/indexes/by_dept/scan?value=engineering&records=true"

# Prefix scan
curl -sk -H "Authorization: Bearer $KEY" \
  "https://host:7700/v1/tables/users/indexes/by_dept/scan?prefix=eng"
```

---

### Atomic batch

#### `POST /v1/batch` — write

Applies pre-condition checks followed by a sequence of put/delete operations atomically. All checks are evaluated and all operations — including secondary-index mutations — are committed as a single WAL record. Either every write succeeds or none do.

Request:

```json
{
  "checks": [
    { "table": "inventory", "key": "widget-A", "expected": "42" },
    { "table": "staging",   "key": "widget-A", "expected": null }
  ],
  "ops": [
    { "op": "put",    "table": "inventory", "key": "widget-A", "value": "41" },
    { "op": "delete", "table": "staging",   "key": "widget-A" }
  ]
}
```

- `expected: null` — the key must be absent.
- `expected: "v"` — the key must have exactly that value.
- `checks` is optional.

Response when committed:

```json
{ "committed": true }
```

Response when a check fails (no ops were applied):

```json
{ "committed": false, "reason": "check failed: table=\"inventory\" key=\"widget-A\"" }
```

---

### Schema

#### `GET /v1/schema` — read
Returns the current schema (tables + index definitions) in flexctl-compatible format.

```json
{
  "version": 1,
  "tables": [
    {
      "name": "users",
      "indexes": [
        { "name": "by_dept", "type": "field", "field": 0 }
      ]
    }
  ]
}
```

#### `POST /v1/schema` — admin
Apply a schema: creates missing tables and indexes. Existing tables and indexes are left untouched (idempotent). Uses the same JSON format as `GET /v1/schema`.

---

### Data dump and load

#### `GET /v1/dump` — read
Streams all table data as **NDJSON**. Each line is either a table header or a data entry:

```
{"type":"table","name":"users"}
{"type":"entry","key":"alice","value":"ZW5naW5lZXJpbmcsc2VuaW9y"}
{"type":"entry","key":"bob","value":"ZGVzaWduLG1pZA=="}
{"type":"table","name":"orders"}
{"type":"entry","key":"order-1","value":"cGVuZGluZw=="}
```

Values are base64-encoded. Tables are emitted in sorted order; entries are in key order.

#### `POST /v1/dump` — write
Loads data from the NDJSON format produced by `GET /v1/dump`. Returns a summary:

```json
{ "imported": 1042 }
```

On partial errors:
```json
{ "imported": 1038, "errors": ["put key-x: value too large", "..."] }
```

---

## Using flexctl as a client

[flexctl](../flexctl/) supports remote mode natively. Pass the server URL instead of a database path:

```bash
export FLEXSRV_KEY=flex_...

flexctl https://host:7700 table list
flexctl https://host:7700 value put users alice "engineering,senior"
flexctl https://host:7700 table scan users --limit 20
flexctl https://host:7700 index create users by_dept field 0
flexctl https://host:7700 database stats
flexctl https://host:7700 database dump backup.json

# Self-signed cert
flexctl https://localhost:7700 --insecure table list

# Interactive REPL
flexctl https://host:7700
```

See the [flexctl README](../flexctl/README.md#remote-mode--connecting-to-flexsrv) for full details.

---

## Production deployment notes

- Run behind a reverse proxy (nginx, Caddy) if you need HTTP/2 push, path-based routing, or certificate automation via ACME.
- Use a `read`-role key for read-only application traffic and an `admin`-role key only for administration.
- flexsrv opens the database exclusively — only one flexsrv process should access a given database directory at a time. (flexctl in local mode will conflict with a running flexsrv on the same path.)
- `max_value_bytes` defaults to 4096, matching the flexkv `MaxKVSize` limit. Raising it in config has no effect without a corresponding change to the storage layer.
- Graceful shutdown drains in-flight requests for up to 15 seconds on `SIGINT`/`SIGTERM`.
