# flexctl

`flexctl` is a command-line interface and interactive REPL for `flexkv` databases. It provides complete database administration without writing any Go code — think `sqlite3` but for flexkv.

---

## Installation

```bash
cd flexspace-go
go build -o flexctl ./cmd/flexctl/
```

---

## Usage

```
flexctl <db-path-or-url> <command> [args...]
flexctl <db-path-or-url>              # start interactive REPL
```

The first argument is either a **local database path** or a **flexsrv URL** (`http://` or `https://`). Every command works identically in both modes.

Confirmation and status messages go to **stderr**; all data output goes to **stdout**, so results are safely pipeable.

---

## Remote mode — connecting to flexsrv

`flexctl` can connect to a running [flexsrv](../flexsrv/) instance instead of opening a local database file. Pass an HTTP/HTTPS URL as the first argument.

### Quick start

```bash
# 1. Generate an API key on the server
flexsrv keygen
#   Key:  flex_MwxLBkhDy8xOIkJU91z9IhSCTjQ3P_xUQt55ySWRo3s
#   Hash: sha256:6f24db885c2097ce1fc540b902f9cbb57b62aa98331cbac4f4002610cad75f67

# 2. Add the hash to the server's config.json, then start flexsrv
flexsrv serve --db /data/mydb --config config.json

# 3. Give the raw key to flexctl via the environment variable
export FLEXSRV_KEY=flex_MwxLBkhDy8xOIkJU91z9IhSCTjQ3P_xUQt55ySWRo3s

# 4. Use flexctl normally — just replace the db path with the server URL
flexctl https://host:7700 table list
flexctl https://host:7700 value put users alice "engineering,senior"
flexctl https://host:7700 database stats
```

### Authentication

Set `FLEXSRV_KEY` to the bearer token printed by `flexsrv keygen`. The variable is read on every invocation; there is no login step.

```bash
export FLEXSRV_KEY=flex_...
flexctl https://host:7700 table list
```

If `FLEXSRV_KEY` is not set, requests are sent without authentication. They will be rejected by the server unless the server has no keys configured (not recommended for production).

### TLS

flexsrv uses HTTPS by default. If the server is running with a certificate from a trusted CA, no extra flags are needed.

For a server using a **self-signed certificate** (flexsrv's default when no cert files are provided), pass `--insecure` immediately after the URL to skip certificate verification:

```bash
flexctl https://localhost:7700 --insecure table list
```

`--insecure` is safe for local development. Do not use it when connecting over a network you do not control.

### REPL over a remote connection

The interactive REPL works exactly as it does locally, including batch mode:

```bash
export FLEXSRV_KEY=flex_...
flexctl https://host:7700

flexctl> table create orders
flexctl> value put orders order-1 "pending"
flexctl> database batch
flexctl (batch)> check orders order-1 pending
flexctl (batch)> value put orders order-1 shipped
flexctl (batch)> commit
flexctl> exit
```

### Command compatibility

Every flexctl command is supported in remote mode. The only visible difference is in `database stats`, where the **Path** line shows the server URL instead of a local path:

```
=== Database ===
  Server:   https://host:7700
  Write seq:  ...
```

### Non-interactive / scripted use

Remote mode works in pipes and scripts exactly like local mode:

```bash
# Migrate data from a local DB to a remote server
export FLEXSRV_KEY=flex_...
flexctl /local/db database dump | flexctl https://host:7700 --insecure database load
```

---

## Interactive REPL

When invoked with only a database path, `flexctl` opens an interactive session:

```
$ ./flexctl /data/mydb
flexctl 0.0.1  (local: /data/mydb)
  Tables (2): orders, users
  Type 'help' for commands, 'exit' to quit.

flexctl> help
flexctl> table create users
flexctl> value put users alice "engineering,senior"
flexctl> value put users bob "design,mid"
flexctl> table scan users
alice  engineering,senior
bob    design,mid
flexctl> table scan prefix users a
alice  engineering,senior
flexctl> index create users by_dept field 0
flexctl> index scan users by_dept engineering
engineering  alice  engineering,senior
flexctl> database stats
...
flexctl> exit
```

The REPL supports:
- **Shell-style tokenisation**: single quotes, double quotes, and `\`-escape inside double quotes.
- **Comments**: lines starting with `#` are ignored.
- **Error recovery**: command errors print to stderr and the session continues (unlike one-shot mode, which calls `os.Exit`).
- **Non-interactive mode**: when stdin is a pipe, the prompt is suppressed and commands are read line-by-line.

```bash
# Run a script non-interactively:
cat setup.txt | ./flexctl /data/mydb

# Or:
./flexctl /data/mydb repl < setup.txt
```

---

## Commands

### Table management

```bash
# List all user tables.
./flexctl mydb table list

# Explicitly create a table (also happens automatically on first value put).
./flexctl mydb table create users

# Drop a table and all its secondary indexes (O(1) — deletes the directory).
./flexctl mydb table drop users
```

### Scanning

All scan commands accept `--limit N` (or `-n N`) to stop after N results.

```bash
# Full table scan.
./flexctl mydb table scan users

# Range scan [start, end) — either bound may be omitted.
./flexctl mydb table scan users alice carol
./flexctl mydb table scan users alice        # alice to end
./flexctl mydb table scan users "" carol     # beginning to carol

# Prefix scan on a table.
./flexctl mydb table scan prefix users ali

# Limit results.
./flexctl mydb table scan users --limit 10
./flexctl mydb table scan users -n 10
```

### Value CRUD

```bash
./flexctl mydb value put    users alice "engineering,senior,90000"
./flexctl mydb value get    users alice              # exits 1 if not found
./flexctl mydb value exists users alice              # exits 0 if found, 1 if absent; no output
./flexctl mydb value delete users alice
./flexctl mydb value count  users
```

`value get` exits with code 1 when the key is not found, so it works naturally in scripts:

```bash
if ./flexctl mydb value get users alice > /dev/null; then
  echo "alice exists"
fi
```

#### Looking up by index

Pass an index name as an optional third argument to `value get` or `value count` to operate through a secondary index instead of the primary key.

```bash
# Find all records whose "by_dept" indexed value is exactly "engineering".
# Output: indexed_value  primary_key  record  (tab-separated, one row per match)
./flexctl mydb value get users engineering by_dept

# Count how many records are indexed under "by_dept" (may differ from primary
# count for multi-valued indexes where one record produces multiple entries).
./flexctl mydb value count users by_dept
```

### Secondary indexes

Index scans output three tab-separated columns: `indexed_value`, `primary_key`, `record`.

```bash
# List indexes on a table.
./flexctl mydb index list users

# Create indexes.
./flexctl mydb index create users by_dept  field  0          # first comma-field
./flexctl mydb index create users by_level field  1          # second comma-field
./flexctl mydb index create users by_delim field  0 :        # field with custom delimiter
./flexctl mydb index create users by_role  exact             # whole value
./flexctl mydb index create users by_pfx   prefix 4          # first 4 bytes
./flexctl mydb index create users by_sfx   suffix 4          # last 4 bytes

# Drop an index.
./flexctl mydb index drop users by_pfx

# Range scan on an index — returns indexed_value/primary_key/record triples.
./flexctl mydb index scan users by_dept engineering
./flexctl mydb index scan users by_dept design engineering    # range [design, engineering)

# Prefix scan on an index.
./flexctl mydb index scan prefix users by_dept eng

# Limit index scan results.
./flexctl mydb index scan users by_dept --limit 5
```

#### Index types summary

| Type | Syntax | Indexed value |
|------|--------|---------------|
| `exact` | `index create T I exact` | Entire value |
| `prefix` | `index create T I prefix N` | First N bytes of value |
| `suffix` | `index create T I suffix N` | Last N bytes of value |
| `field` | `index create T I field N [delim]` | Field N (0-based), split by `delim` (default `,`) |

### Bulk operations

#### Batch — atomic multi-table operations

You can apply a batch from a JSON file, or create one interactively in the REPL.

**Interactive Batch Mode (REPL only):**

Starting a batch in the REPL allows you to queue multiple operations and apply them atomically.

```
flexctl> database batch
flexctl (batch)> check inventory widget-A 42
flexctl (batch)> value put inventory widget-A 41
flexctl (batch)> value delete staging widget-A
flexctl (batch)> database batch show
2 check(s), 2 op(s) queued
  check  inventory  widget-A  == "42"
  put    inventory  widget-A  41
  delete staging    widget-A
flexctl (batch)> commit
flexctl>
```

- While in batch mode, the prompt changes to `flexctl (batch)>`.
- **`value put`**, **`value delete`**, and **`check`** commands are queued instead of executed immediately.
- **`check <table> <key> [expected]`** adds a pre-condition. If `expected` is omitted, the key must be absent.
- **`database batch show`** lists all queued checks and ops.
- **`commit`** applies all queued checks and operations atomically. If any check fails, the entire batch is rejected.
- **`abort`** discards all queued operations and exits batch mode.
- Other commands are **disabled** while a batch is active to avoid confusion.

**JSON Batch File:**

```bash
./flexctl mydb database batch ops.json
./flexctl mydb database batch            # read from stdin
```

Batch file format (`ops.json`):

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

- `checks` are evaluated first. An `expected` of `null` means the key must be absent.
- If any check fails the entire batch is rejected; no ops are applied.

#### Dump and load

```bash
# Export all tables to JSON.
./flexctl mydb database dump backup.json
./flexctl mydb database dump                  # stdout

# Import a JSON dump.
./flexctl mydb database load backup.json
./flexctl mydb database load                  # stdin
```

Dump file format:

```json
{
  "version": 1,
  "tables": [
    {
      "name": "users",
      "entries": [
        { "key": "alice", "value": "engineering,senior" },
        { "key": "bob",   "value": "design,mid" }
      ]
    }
  ]
}
```

### Schema management

Schema files declare the tables and their index definitions. They are useful for recreating a database structure without replaying individual commands.

```bash
# Export the current schema.
./flexctl mydb schema dump schema.json
./flexctl mydb schema dump             # stdout

# Apply a schema file (creates tables and indexes that don't exist yet).
./flexctl mydb schema load schema.json
```

Schema file format:

```json
{
  "version": 1,
  "tables": [
    {
      "name": "users",
      "indexes": [
        { "name": "by_dept",  "type": "field",  "field": 0 },
        { "name": "by_level", "type": "field",  "field": 1, "delim": "," },
        { "name": "by_role",  "type": "exact" },
        { "name": "by_pfx",   "type": "prefix", "length": 4 },
        { "name": "by_sfx",   "type": "suffix", "length": 4 }
      ]
    }
  ]
}
```

Applying a schema is idempotent: tables and indexes that already exist are left as-is (their indexer functions are re-registered in memory but existing data is not rescanned).

### Statistics

```bash
./flexctl mydb database stats
```

Sample output:

```
=== Database ===
  Path:               /data/mydb
  Write seq:          2 847
  Open tables:        7 (flexdb internal)
  Active memtable:    128 B
  Inactive memtable:  0 B

=== Disk Usage ===
  MEMTABLE_LOG0:   4.00 KiB
  MEMTABLE_LOG1:   4.00 KiB
  TABLE_REGISTRY:  4.00 KiB
  tables/:         12.05 MiB
  Total:           12.06 MiB

=== Tables ===
  users     [indexes: by_dept, by_level]
    data:   3.20 KiB   47 anchors   cache 0 B / 64.00 MiB
    .by_dept:  1.80 KiB   18 anchors
    .by_level: 1.10 KiB   11 anchors
  products  [no indexes]
    data:   640 B   8 anchors   cache 0 B / 64.00 MiB
```

**Fields explained:**
- **Write seq** — total number of committed writes since the database was created (persisted across reopens via the WAL header).
- **Open tables** — total internal flexdb tables (user tables + index tables + `_schema`).
- **Active / Inactive memtable** — bytes of unflushed writes currently in memory.
- **Disk usage** — actual disk blocks used (sparse pre-allocated regions are not counted).
- **data** — logical bytes stored in the table's flexfile (not the pre-allocated file size).
- **anchors** — number of sorted key intervals in the in-memory B+tree index; a useful proxy for data density and fragmentation.
- **cache** — bytes of decoded KV intervals currently in the per-table cache vs. total cache capacity.

---

## Internal schema storage

`flexctl` persists index definitions (name, type, parameters) in a hidden `_schema` table within the database itself. This means the schema survives process restarts without any external file. The `schema dump` command exports this data; `schema load` imports and applies it.

---

## Exit codes

| Code | Meaning |
|------|---------|
| 0 | Success |
| 1 | Error, or key not found (`value get` / `value exists`) |
