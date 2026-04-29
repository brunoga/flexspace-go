// flexctl is a command-line interface for flexkv databases.
//
// Usage:
//
//	flexctl <db-path> <command> [args...]
//	flexctl <db-path>                   (starts interactive REPL)
//
// Commands:
//
//	table list                                         List all user tables
//	table create  <table>                              Create a table
//	table drop    <table>                              Drop a table and all its indexes
//	table scan    <table> [start [end]] [--limit N]    Range scan
//	table scan prefix <table> <prefix> [--limit N]     Prefix scan
//
//	value get     <table> <key> [index]                Print value; with index, print all index matches
//	value put     <table> <key> <value>                Insert or update
//	value delete  <table> <key>                        Delete a key
//	value exists  <table> <key>                        Exit 0 if present, 1 if absent
//	value count   <table> [index]                      Count entries; with index, count index entries
//
//	index list    <table>                              List indexes on a table
//	index create  <table> <name> <type> [opts]         Create a secondary index
//	index drop    <table> <name>                       Drop a secondary index
//	index scan    <table> <name> [start [end]] [--limit N]    Range scan on index
//	index scan prefix <table> <name> <prefix> [--limit N]     Prefix scan on index
//
//	schema dump   [file]                               Export schema as JSON
//	schema load   <file>                               Import and apply schema
//
//	database stats                                     Show database statistics
//	database dump [file]                               Dump all tables as JSON
//	database load [file]                               Import JSON dump
//	database batch [file]                              Apply JSON batch
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"text/tabwriter"

	"github.com/brunoga/flexspace-go/flexkv"
	"github.com/chzyer/readline"
)

// replError is used to signal command errors inside the REPL without calling
// os.Exit. die() panics with this type when replMode is true.
type replError string

// replMode is set to true while the REPL is running so that die() panics
// instead of calling os.Exit.
var replMode bool

// ---- schema types ----------------------------------------------------------

// SchemaIndex describes a single secondary index in declarative form.
// Built-in indexer types:
//
//	"prefix"  — first Length bytes of the value
//	"suffix"  — last Length bytes of the value
//	"exact"   — the entire value
//	"field"   — field number Field (0-based) split by Delim (default ",")
type SchemaIndex struct {
	Name   string `json:"name"`
	Type   string `json:"type"`
	Length int    `json:"length,omitempty"`
	Delim  string `json:"delim,omitempty"`
	Field  int    `json:"field,omitempty"`
}

// SchemaTable describes one user table.
type SchemaTable struct {
	Name    string        `json:"name"`
	Indexes []SchemaIndex `json:"indexes,omitempty"`
}

// Schema is the root of a schema file.
type Schema struct {
	Version int           `json:"version"`
	Tables  []SchemaTable `json:"tables"`
}

// ---- batch file types ------------------------------------------------------

// BatchCheck is an optional pre-condition in a batch file.
// If Expected is nil the key must be absent; otherwise it must equal *Expected.
type BatchCheck struct {
	Table    string  `json:"table"`
	Key      string  `json:"key"`
	Expected *string `json:"expected"`
}

// BatchOp is one operation in a batch file.
type BatchOp struct {
	Op    string `json:"op"` // "put" or "delete"
	Table string `json:"table"`
	Key   string `json:"key"`
	Value string `json:"value,omitempty"`
}

// BatchFile is the top-level structure of a batch JSON file.
type BatchFile struct {
	Checks []BatchCheck `json:"checks,omitempty"`
	Ops    []BatchOp    `json:"ops"`
}

// ---- dump file types -------------------------------------------------------

type DumpEntry struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type DumpTable struct {
	Name    string      `json:"name"`
	Entries []DumpEntry `json:"entries"`
}

type Dump struct {
	Version int         `json:"version"`
	Tables  []DumpTable `json:"tables"`
}

// ---- constants -------------------------------------------------------------

// schemaTable is the internal flexkv table that stores per-table index
// definitions. It is excluded from user-visible table listings.
const schemaTable = "_schema"

// ---- helpers ---------------------------------------------------------------

func die(format string, args ...any) {
	msg := fmt.Sprintf("flexctl: "+format, args...)
	if replMode {
		panic(replError(msg))
	}
	fmt.Fprintln(os.Stderr, msg)
	os.Exit(1)
}

func check(err error, context string) {
	if err != nil {
		die("%s: %v", context, err)
	}
}

// info writes an informational/confirmation message to stderr so that data
// output on stdout remains clean and pipeable.
func info(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
}

// extractLimit scans args for a --limit N (or -n N) flag, removes it, and
// returns the cleaned args and the limit (0 = unlimited).
func extractLimit(args []string) ([]string, int) {
	var out []string
	limit := 0
	for i := 0; i < len(args); i++ {
		if (args[i] == "--limit" || args[i] == "-n") && i+1 < len(args) {
			n, err := strconv.Atoi(args[i+1])
			if err != nil || n <= 0 {
				die("--limit requires a positive integer")
			}
			limit = n
			i++
		} else {
			out = append(out, args[i])
		}
	}
	return out, limit
}

// nextKey returns the smallest key greater than key in lexicographic order,
// used to form an exclusive upper bound for an exact-match index range.
// Returns nil if key is all 0xFF bytes (no upper bound needed).
func nextKey(key []byte) []byte {
	next := make([]byte, len(key))
	copy(next, key)
	for i := len(next) - 1; i >= 0; i-- {
		next[i]++
		if next[i] != 0 {
			return next
		}
	}
	return nil
}

// makeIndexer returns a flexkv.Indexer for the given SchemaIndex.
func makeIndexer(idx SchemaIndex) (flexkv.Indexer, error) {
	switch idx.Type {
	case "prefix":
		n := idx.Length
		if n <= 0 {
			return nil, fmt.Errorf("prefix indexer requires length > 0")
		}
		return func(_, value []byte) [][]byte {
			if len(value) < n {
				return nil
			}
			return [][]byte{value[:n]}
		}, nil
	case "suffix":
		n := idx.Length
		if n <= 0 {
			return nil, fmt.Errorf("suffix indexer requires length > 0")
		}
		return func(_, value []byte) [][]byte {
			if len(value) < n {
				return nil
			}
			return [][]byte{value[len(value)-n:]}
		}, nil
	case "exact":
		return func(_, value []byte) [][]byte {
			if len(value) == 0 {
				return nil
			}
			return [][]byte{value}
		}, nil
	case "field":
		delim := idx.Delim
		if delim == "" {
			delim = ","
		}
		delimBytes := []byte(delim)
		n := idx.Field
		return func(_, value []byte) [][]byte {
			parts := bytes.SplitN(value, delimBytes, n+2)
			if n >= len(parts) {
				return nil
			}
			f := bytes.TrimSpace(parts[n])
			if len(f) == 0 {
				return nil
			}
			return [][]byte{f}
		}, nil
	default:
		return nil, fmt.Errorf("unknown indexer type %q (want: prefix, suffix, exact, field)", idx.Type)
	}
}

// ---- DB context ------------------------------------------------------------

// ctx holds an open database (local or remote) and schema state.
type ctx struct {
	ctx    context.Context
	db     *flexkv.DB               // nil when remote != nil
	path   string                   // DB file path or server URL
	schema map[string][]SchemaIndex // table → index defs (local only)
	tables map[string]*flexkv.Table // table name → cached handle (local only)
	batch  *BatchFile               // active interactive batch, if any
	remote *RemoteClient            // non-nil when connected to a flexsrv
}

// openCtx opens a local database or initialises a remote client depending on
// whether path looks like an HTTP URL.
//
// Remote mode: set FLEXSRV_KEY to the bearer token.  Pass --insecure after
// the URL to accept self-signed TLS certificates:
//
//	flexctl https://host:7700 --insecure table list
func openCtx(path string) *ctx {
	if strings.HasPrefix(path, "http://") || strings.HasPrefix(path, "https://") {
		key := os.Getenv("FLEXSRV_KEY")
		insecure := false
		// Allow --insecure as second argument (before the command).
		if len(os.Args) >= 3 && os.Args[2] == "--insecure" {
			insecure = true
			// Shift it out so runCommand sees the real command.
			os.Args = append(os.Args[:2], os.Args[3:]...)
		}
		return &ctx{
			ctx:    context.Background(),
			path:   path,
			remote: newRemoteClient(path, key, insecure),
			schema: make(map[string][]SchemaIndex),
		}
	}

	bg := context.Background()
	db, err := flexkv.Open(bg, path, &flexkv.Options{CacheMB: 64})
	check(err, "open db")

	c := &ctx{ctx: bg, db: db, path: path, schema: make(map[string][]SchemaIndex), tables: make(map[string]*flexkv.Table)}

	st, err := db.Table(c.ctx, schemaTable)
	check(err, "open schema table")

	it := st.Scan(nil, nil)
	defer it.Close()
	for ; it.Valid(); it.Next() {
		tableName := string(it.Key())
		var indexes []SchemaIndex
		if err := json.Unmarshal(it.Value(), &indexes); err != nil {
			fmt.Fprintf(os.Stderr, "flexctl: warning: malformed schema entry for %q: %v\n", tableName, err)
			continue
		}
		c.schema[tableName] = indexes

		// Re-open the table and re-register indexers (no re-scan).
		tbl, err := db.Table(c.ctx, tableName)
		if err != nil {
			fmt.Fprintf(os.Stderr, "flexctl: warning: could not open table %q: %v\n", tableName, err)
			continue
		}
		c.tables[tableName] = tbl
		for _, idx := range indexes {
			fn, err := makeIndexer(idx)
			if err != nil {
				fmt.Fprintf(os.Stderr, "flexctl: warning: indexer %q on %q: %v\n", idx.Name, tableName, err)
				continue
			}
			if err := tbl.RegisterIndex(c.ctx, idx.Name, fn); err != nil {
				fmt.Fprintf(os.Stderr, "flexctl: warning: RegisterIndex %q on %q: %v\n", idx.Name, tableName, err)
			}
		}
	}
	return c
}

func (c *ctx) close() {
	if c.db != nil {
		c.db.Close()
	}
}

// table returns a *flexkv.Table by name, dying on error.
// The handle is cached so that registered indexers survive across commands.
func (c *ctx) table(name string) *flexkv.Table {
	if tbl, ok := c.tables[name]; ok {
		return tbl
	}
	tbl, err := c.db.Table(c.ctx, name)
	check(err, "open table "+name)
	c.tables[name] = tbl
	return tbl
}

// saveSchemaEntry persists index definitions for one table to _schema.
func (c *ctx) saveSchemaEntry(tableName string, indexes []SchemaIndex) {
	st := c.table(schemaTable)
	data, err := json.Marshal(indexes)
	check(err, "marshal schema")
	check(st.Put(c.ctx, []byte(tableName), data), "write schema")
	c.schema[tableName] = indexes
}

// userTables returns names of all user-visible tables (excludes _schema and
// internal index tables).
func (c *ctx) userTables() []string {
	all, err := c.db.Tables()
	check(err, "list tables")
	var out []string
	for _, name := range all {
		if name != schemaTable {
			out = append(out, name)
		}
	}
	return out
}

// ---- table entity ----------------------------------------------------------

func cmdTableEntity(c *ctx, args []string) {
	if len(args) == 0 {
		die("table requires a subcommand: list, create, drop, scan")
	}
	switch args[0] {
	case "list":
		cmdTableList(c)
	case "create":
		cmdTableCreate(c, args[1:])
	case "drop":
		cmdTableDrop(c, args[1:])
	case "scan":
		rest, limit := extractLimit(args[1:])
		if len(rest) > 0 && rest[0] == "prefix" {
			cmdTableScanPrefix(c, rest[1:], limit)
		} else {
			cmdTableScan(c, rest, limit)
		}
	default:
		die("unknown table subcommand %q (want: list, create, drop, scan)", args[0])
	}
}

func cmdTableList(c *ctx) {
	if c.remote != nil {
		for _, name := range c.remote.TableList() {
			fmt.Println(name)
		}
		return
	}
	for _, name := range c.userTables() {
		fmt.Println(name)
	}
}

func cmdTableCreate(c *ctx, args []string) {
	if len(args) != 1 {
		die("table create requires <table>")
	}
	name := args[0]
	if c.remote != nil {
		c.remote.TableCreate(name)
		info("table %q created", name)
		return
	}
	c.table(name) // opens/creates the table
	if _, exists := c.schema[name]; !exists {
		c.saveSchemaEntry(name, nil)
	}
	info("table %q created", name)
}

func cmdTableDrop(c *ctx, args []string) {
	if len(args) != 1 {
		die("table drop requires <table>")
	}
	name := args[0]
	if c.remote != nil {
		c.remote.TableDrop(name)
		return
	}
	check(c.db.DropTable(c.ctx, name), "drop table")
	delete(c.tables, name)
	// Remove schema entry too.
	st := c.table(schemaTable)
	check(st.Delete(c.ctx, []byte(name)), "remove schema entry")
	delete(c.schema, name)
}

func cmdTableScan(c *ctx, args []string, limit int) {
	if len(args) < 1 {
		die("table scan requires <table> [start [end]]")
	}
	var start, end []byte
	if len(args) > 1 {
		start = []byte(args[1])
	}
	if len(args) > 2 {
		end = []byte(args[2])
	}
	if c.remote != nil {
		it := c.remote.Scan(args[0], start, end, limit)
		defer it.Close()
		printScan(it, limit)
		return
	}
	tbl := c.table(args[0])
	it := tbl.Scan(start, end)
	defer it.Close()
	printScan(it, limit)
}

func cmdTableScanPrefix(c *ctx, args []string, limit int) {
	if len(args) != 2 {
		die("table scan prefix requires <table> <prefix>")
	}
	if c.remote != nil {
		it := c.remote.ScanPrefix(args[0], []byte(args[1]), limit)
		defer it.Close()
		printScan(it, limit)
		return
	}
	it := c.table(args[0]).ScanPrefix([]byte(args[1]))
	defer it.Close()
	printScan(it, limit)
}

// ---- value entity ----------------------------------------------------------

func cmdValueEntity(c *ctx, args []string) {
	if len(args) == 0 {
		die("value requires a subcommand: get, put, delete, exists, count")
	}
	sub := args[0]
	rest := args[1:]
	switch sub {
	case "get":
		cmdValueGet(c, rest)
	case "put":
		cmdValuePut(c, rest)
	case "delete":
		cmdValueDelete(c, rest)
	case "exists":
		cmdValueExists(c, rest)
	case "count":
		cmdValueCount(c, rest)
	default:
		die("unknown value subcommand %q (want: get, put, delete, exists, count)", sub)
	}
}

func cmdValueGet(c *ctx, args []string) {
	if len(args) < 2 || len(args) > 3 {
		die("value get requires <table> <key> [index]")
	}
	// Optional index: treat key as an indexed value and return all matches.
	if len(args) == 3 {
		tableName, key, indexName := args[0], []byte(args[1]), args[2]
		if c.remote != nil {
			it := c.remote.IndexScan(tableName, indexName, key, nextKey(key), 0)
			defer it.Close()
			printIndexScan(c.ctx, it, 0)
			return
		}
		it := c.table(tableName).Index(indexName).Get(key)
		defer it.Close()
		printIndexScan(c.ctx, it, 0)
		return
	}

	if c.remote != nil {
		val := c.remote.Get(args[0], args[1])
		if val == nil {
			if !replMode {
				os.Exit(1)
			}
			return
		}
		fmt.Println(string(val))
		return
	}
	tbl := c.table(args[0])
	val, err := tbl.Get(c.ctx, []byte(args[1]))
	check(err, "get")
	if val == nil {
		if !replMode {
			os.Exit(1)
		}
		return
	}
	fmt.Println(string(val))
}

func cmdValuePut(c *ctx, args []string) {
	if len(args) != 3 {
		die("value put requires <table> <key> <value>")
	}
	if c.batch != nil {
		c.batch.Ops = append(c.batch.Ops, BatchOp{
			Op:    "put",
			Table: args[0],
			Key:   args[1],
			Value: args[2],
		})
		info("queued put on %q", args[0])
		return
	}
	if c.remote != nil {
		c.remote.Put(args[0], args[1], []byte(args[2]))
		return
	}
	tbl := c.table(args[0])
	check(tbl.Put(c.ctx, []byte(args[1]), []byte(args[2])), "put")
}

func cmdValueDelete(c *ctx, args []string) {
	if len(args) != 2 {
		die("value delete requires <table> <key>")
	}
	if c.batch != nil {
		c.batch.Ops = append(c.batch.Ops, BatchOp{
			Op:    "delete",
			Table: args[0],
			Key:   args[1],
		})
		info("queued delete on %q", args[0])
		return
	}
	if c.remote != nil {
		c.remote.Delete(args[0], args[1])
		return
	}
	tbl := c.table(args[0])
	check(tbl.Delete(c.ctx, []byte(args[1])), "delete")
}

// cmdValueExists exits 0 if the key is present, 1 if absent. In REPL mode it
// prints "found" or "not found" instead (no exit code available).
func cmdValueExists(c *ctx, args []string) {
	if len(args) != 2 {
		die("value exists requires <table> <key>")
	}
	if c.remote != nil {
		found := c.remote.Exists(args[0], args[1])
		if !found {
			if replMode {
				fmt.Println("not found")
			} else {
				os.Exit(1)
			}
		} else if replMode {
			fmt.Println("found")
		}
		return
	}
	val, err := c.table(args[0]).Get(c.ctx, []byte(args[1]))
	check(err, "exists")
	if val == nil {
		if replMode {
			fmt.Println("not found")
		} else {
			os.Exit(1)
		}
		return
	}
	if replMode {
		fmt.Println("found")
	}
}

func cmdValueCount(c *ctx, args []string) {
	if len(args) < 1 || len(args) > 2 {
		die("value count requires <table> [index]")
	}
	// Optional index: count entries in the index instead of the primary table.
	if len(args) == 2 {
		tableName, indexName := args[0], args[1]
		if c.remote != nil {
			fmt.Println(c.remote.IndexCount(tableName, indexName))
			return
		}
		it := c.table(tableName).Index(indexName).Scan(nil, nil)
		defer it.Close()
		n := 0
		for ; it.Valid(); it.Next() {
			n++
		}
		fmt.Println(n)
		return
	}
	if c.remote != nil {
		fmt.Println(c.remote.Count(args[0]))
		return
	}
	it := c.table(args[0]).Scan(nil, nil)
	defer it.Close()
	n := 0
	for ; it.Valid(); it.Next() {
		n++
	}
	fmt.Println(n)
}

// ---- index entity ----------------------------------------------------------

func cmdIndexEntity(c *ctx, args []string) {
	if len(args) == 0 {
		die("index requires a subcommand: list, create, drop, scan")
	}
	sub := args[0]
	rest := args[1:]
	switch sub {
	case "list":
		cmdIndexList(c, rest)
	case "create":
		cmdIndexCreate(c, rest)
	case "drop":
		cmdIndexDrop(c, rest)
	case "scan":
		rest, limit := extractLimit(rest)
		if len(rest) > 0 && rest[0] == "prefix" {
			cmdIndexScanPrefix(c, rest[1:], limit)
		} else {
			cmdIndexScan(c, rest, limit)
		}
	default:
		die("unknown index subcommand %q (want: list, create, drop, scan)", sub)
	}
}

func cmdIndexList(c *ctx, args []string) {
	if len(args) != 1 {
		die("index list requires <table>")
	}
	var indexes []SchemaIndex
	if c.remote != nil {
		indexes = c.remote.IndexList(args[0])
	} else {
		indexes = c.schema[args[0]]
	}
	if len(indexes) == 0 {
		return
	}
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	for _, idx := range indexes {
		switch idx.Type {
		case "exact":
			fmt.Fprintf(w, "%s\t%s\n", idx.Name, idx.Type)
		case "prefix", "suffix":
			fmt.Fprintf(w, "%s\t%s %d\n", idx.Name, idx.Type, idx.Length)
		case "field":
			delim := idx.Delim
			if delim == "" {
				delim = ","
			}
			fmt.Fprintf(w, "%s\t%s %d %s\n", idx.Name, idx.Type, idx.Field, delim)
		}
	}
	w.Flush()
}

func cmdIndexCreate(c *ctx, args []string) {
	if len(args) < 3 {
		die("index create requires <table> <name> <type> [options...]")
	}
	tableName, idxName := args[0], args[1]

	idxSpec, err := parseIndexSpec(idxName, args[2:])
	check(err, "index spec")

	if c.remote != nil {
		c.remote.IndexCreate(tableName, idxSpec)
		info("index %q created on %q", idxName, tableName)
		return
	}

	fn, err := makeIndexer(idxSpec)
	check(err, "make indexer")

	tbl := c.table(tableName)
	existing := c.schema[tableName]

	for _, idx := range existing {
		if idx.Name == idxName {
			// Already exists: re-register without re-scanning data.
			check(tbl.RegisterIndex(c.ctx, idxName, fn), "register index")
			info("index %q on %q already exists (re-registered)", idxName, tableName)
			return
		}
	}

	// New index: create and populate from existing data.
	check(tbl.CreateIndex(c.ctx, idxName, fn), "create index")
	c.saveSchemaEntry(tableName, append(existing, idxSpec))
	info("index %q created on %q", idxName, tableName)
}

func cmdIndexDrop(c *ctx, args []string) {
	if len(args) != 2 {
		die("index drop requires <table> <name>")
	}
	tableName, idxName := args[0], args[1]

	if c.remote != nil {
		c.remote.IndexDrop(tableName, idxName)
		info("index %q dropped from %q", idxName, tableName)
		return
	}

	tbl := c.table(tableName)
	check(tbl.DropIndex(c.ctx, idxName), "drop index")

	existing := c.schema[tableName]
	var updated []SchemaIndex
	for _, idx := range existing {
		if idx.Name != idxName {
			updated = append(updated, idx)
		}
	}
	c.saveSchemaEntry(tableName, updated)
	info("index %q dropped from %q", idxName, tableName)
}

func cmdIndexScan(c *ctx, args []string, limit int) {
	if len(args) < 2 {
		die("index scan requires <table> <name> [start [end]]")
	}
	var start, end []byte
	if len(args) > 2 {
		start = []byte(args[2])
	}
	if len(args) > 3 {
		end = []byte(args[3])
	}
	if c.remote != nil {
		it := c.remote.IndexScan(args[0], args[1], start, end, limit)
		defer it.Close()
		printIndexScan(c.ctx, it, limit)
		return
	}
	tbl := c.table(args[0])
	idx := tbl.Index(args[1])
	it := idx.Scan(start, end)
	defer it.Close()
	printIndexScan(c.ctx, it, limit)
}

func cmdIndexScanPrefix(c *ctx, args []string, limit int) {
	if len(args) != 3 {
		die("index scan prefix requires <table> <name> <prefix>")
	}
	if c.remote != nil {
		it := c.remote.IndexScanPrefix(args[0], args[1], []byte(args[2]), limit)
		defer it.Close()
		printIndexScan(c.ctx, it, limit)
		return
	}
	it := c.table(args[0]).Index(args[1]).ScanPrefix([]byte(args[2]))
	defer it.Close()
	printIndexScan(c.ctx, it, limit)
}

// ---- schema entity ---------------------------------------------------------

func cmdSchemaEntity(c *ctx, args []string) {
	if len(args) == 0 {
		die("schema requires a subcommand: dump, load")
	}
	switch args[0] {
	case "dump":
		cmdSchemaDump(c, args[1:])
	case "load":
		cmdSchemaLoad(c, args[1:])
	default:
		die("unknown schema subcommand %q (want: dump, load)", args[0])
	}
}

func cmdSchemaDump(c *ctx, args []string) {
	var schema Schema
	if c.remote != nil {
		schema = c.remote.SchemaDump()
	} else {
		schema = Schema{Version: 1}
		for _, name := range c.userTables() {
			st := SchemaTable{Name: name}
			if indexes, ok := c.schema[name]; ok {
				st.Indexes = indexes
			}
			schema.Tables = append(schema.Tables, st)
		}
	}

	var out *os.File
	if len(args) > 0 {
		f, err := os.Create(args[0])
		check(err, "create schema file")
		defer f.Close()
		out = f
	} else {
		out = os.Stdout
	}
	enc := json.NewEncoder(out)
	enc.SetIndent("", "  ")
	check(enc.Encode(schema), "encode schema")
}

func cmdSchemaLoad(c *ctx, args []string) {
	if len(args) == 0 {
		die("schema load requires <file>")
	}
	data, err := os.ReadFile(args[0])
	check(err, "read schema file")

	var schema Schema
	check(json.Unmarshal(data, &schema), "parse schema file")

	if c.remote != nil {
		c.remote.SchemaLoad(schema)
		return
	}

	for _, st := range schema.Tables {
		tbl := c.table(st.Name) // creates if absent
		existing := c.schema[st.Name]
		existingSet := map[string]bool{}
		for _, idx := range existing {
			existingSet[idx.Name] = true
		}

		for _, idx := range st.Indexes {
			fn, err := makeIndexer(idx)
			check(err, fmt.Sprintf("indexer %q on %q", idx.Name, st.Name))
			if existingSet[idx.Name] {
				// Index already exists; just re-register the function.
				check(tbl.RegisterIndex(c.ctx, idx.Name, fn), "register index")
			} else {
				// New index: create and populate from existing data.
				check(tbl.CreateIndex(c.ctx, idx.Name, fn), "create index")
			}
		}

		c.saveSchemaEntry(st.Name, st.Indexes)
	}
}

// ---- database entity -------------------------------------------------------

func cmdDatabaseEntity(c *ctx, args []string) {
	if len(args) == 0 {
		die("database requires a subcommand: stats, dump, load, batch")
	}
	switch args[0] {
	case "stats":
		cmdDatabaseStats(c)
	case "dump":
		cmdDatabaseDump(c, args[1:])
	case "load":
		cmdDatabaseLoad(c, args[1:])
	case "batch":
		cmdDatabaseBatch(c, args[1:])
	default:
		die("unknown database subcommand %q (want: stats, dump, load, batch)", args[0])
	}
}

func cmdDatabaseDump(c *ctx, args []string) {
	var out *os.File
	if len(args) > 0 {
		f, err := os.Create(args[0])
		check(err, "create dump file")
		defer f.Close()
		out = f
	} else {
		out = os.Stdout
	}

	if c.remote != nil {
		c.remote.DumpData(out)
		return
	}

	d := Dump{Version: 1}
	for _, name := range c.userTables() {
		tbl := c.table(name)
		dt := DumpTable{Name: name}
		it := tbl.Scan(nil, nil)
		for ; it.Valid(); it.Next() {
			dt.Entries = append(dt.Entries, DumpEntry{
				Key:   string(it.Key()),
				Value: string(it.Value()),
			})
		}
		it.Close()
		d.Tables = append(d.Tables, dt)
	}
	enc := json.NewEncoder(out)
	enc.SetIndent("", "  ")
	check(enc.Encode(d), "encode dump")
}

func cmdDatabaseLoad(c *ctx, args []string) {
	var r io.Reader
	if len(args) == 0 {
		r = os.Stdin
	} else {
		f, err := os.Open(args[0])
		check(err, "open dump file")
		defer f.Close()
		r = f
	}

	if c.remote != nil {
		c.remote.LoadData(r)
		return
	}

	data, err := io.ReadAll(r)
	check(err, "read dump file")

	var d Dump
	check(json.Unmarshal(data, &d), "parse dump file")

	for _, dt := range d.Tables {
		tbl := c.table(dt.Name)
		for _, e := range dt.Entries {
			check(tbl.Put(c.ctx, []byte(e.Key), []byte(e.Value)), "import put")
		}
	}
}

func cmdDatabaseBatch(c *ctx, args []string) {
	// "database batch show" — display queued ops.
	if len(args) == 1 && args[0] == "show" {
		cmdDatabaseBatchShow(c)
		return
	}
	// "database batch" with no args in REPL — enter interactive batch mode.
	if len(args) == 0 && replMode {
		if c.batch != nil {
			die("already in batch mode")
		}
		c.batch = &BatchFile{}
		info("Entering batch mode. 'value put', 'value delete', and 'check' will be queued.")
		info("Use 'commit' to apply the batch or 'abort' to discard it.")
		return
	}

	var data []byte
	var err error
	if len(args) == 0 {
		data, err = io.ReadAll(os.Stdin)
	} else {
		data, err = os.ReadFile(args[0])
	}
	check(err, "read batch file")

	var bf BatchFile
	check(json.Unmarshal(data, &bf), "parse batch file")

	applyBatch(c, &bf)
}

func cmdDatabaseBatchShow(c *ctx) {
	if c.batch == nil {
		die("no active batch")
	}
	bf := c.batch
	fmt.Printf("%d check(s), %d op(s) queued\n", len(bf.Checks), len(bf.Ops))
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	for _, chk := range bf.Checks {
		if chk.Expected == nil {
			fmt.Fprintf(w, "  check\t%s\t%s\t(must be absent)\n", chk.Table, chk.Key)
		} else {
			fmt.Fprintf(w, "  check\t%s\t%s\t== %q\n", chk.Table, chk.Key, *chk.Expected)
		}
	}
	for _, op := range bf.Ops {
		switch op.Op {
		case "put":
			fmt.Fprintf(w, "  put\t%s\t%s\t%s\n", op.Table, op.Key, op.Value)
		case "delete":
			fmt.Fprintf(w, "  delete\t%s\t%s\n", op.Table, op.Key)
		}
	}
	w.Flush()
}

func applyBatch(c *ctx, bf *BatchFile) {
	if c.remote != nil {
		if !c.remote.ApplyBatch(bf) {
			die("batch not committed")
		}
		return
	}

	// Open all referenced tables once.
	tables := map[string]*flexkv.Table{}
	refTable := func(name string) *flexkv.Table {
		if t, ok := tables[name]; ok {
			return t
		}
		t := c.table(name)
		tables[name] = t
		return t
	}

	// Build a flexkv.Batch: all checks + all ops as a single WAL record.
	batch := c.db.NewBatch()

	for _, chk := range bf.Checks {
		var expected []byte
		if chk.Expected != nil {
			expected = []byte(*chk.Expected)
		}
		batch.Check(refTable(chk.Table), []byte(chk.Key), expected)
	}

	for _, op := range bf.Ops {
		tbl := refTable(op.Table)
		switch op.Op {
		case "put":
			batch.Put(tbl, []byte(op.Key), []byte(op.Value))
		case "delete":
			batch.Delete(tbl, []byte(op.Key))
		default:
			die("unknown batch op %q (want: put, delete)", op.Op)
		}
	}

	committed, err := batch.Commit(c.ctx)
	check(err, "batch commit")
	if !committed {
		die("batch rejected: a pre-condition check failed")
	}
}

func cmdCommit(c *ctx) {
	if c.batch == nil {
		die("no active batch to commit")
	}
	applyBatch(c, c.batch)
	c.batch = nil
	info("batch committed")
}

func cmdAbort(c *ctx) {
	if c.batch == nil {
		die("no active batch to abort")
	}
	c.batch = nil
	info("batch aborted")
}

func cmdCheck(c *ctx, args []string) {
	if c.batch == nil {
		die("check can only be used inside a batch")
	}
	if len(args) < 2 || len(args) > 3 {
		die("check requires <table> <key> [expected-value]")
	}
	chk := BatchCheck{
		Table: args[0],
		Key:   args[1],
	}
	if len(args) == 3 {
		val := args[2]
		chk.Expected = &val
	}
	c.batch.Checks = append(c.batch.Checks, chk)
	info("queued check on %q", args[0])
}

// ---- index spec parsing ----------------------------------------------------

// parseIndexSpec parses an index name plus type+options tokens into a SchemaIndex.
//
//	exact
//	prefix <n>
//	suffix <n>
//	field  <n> [delim]
func parseIndexSpec(name string, args []string) (SchemaIndex, error) {
	if len(args) == 0 {
		return SchemaIndex{}, fmt.Errorf("index type required (exact|prefix|suffix|field)")
	}
	idx := SchemaIndex{Name: name, Type: args[0]}
	switch args[0] {
	case "exact":
		// no extra options
	case "prefix", "suffix":
		if len(args) < 2 {
			return SchemaIndex{}, fmt.Errorf("%s requires <length>", args[0])
		}
		n, err := strconv.Atoi(args[1])
		if err != nil || n <= 0 {
			return SchemaIndex{}, fmt.Errorf("invalid length %q: must be a positive integer", args[1])
		}
		idx.Length = n
	case "field":
		if len(args) < 2 {
			return SchemaIndex{}, fmt.Errorf("field requires <n>")
		}
		n, err := strconv.Atoi(args[1])
		if err != nil || n < 0 {
			return SchemaIndex{}, fmt.Errorf("invalid field number %q: must be a non-negative integer", args[1])
		}
		idx.Field = n
		if len(args) >= 3 {
			idx.Delim = args[2]
		}
	default:
		return SchemaIndex{}, fmt.Errorf("unknown index type %q (want: exact, prefix, suffix, field)", args[0])
	}
	return idx, nil
}

// ---- scan helpers ----------------------------------------------------------

func printIndexScan(ctx context.Context, it interface {
	Valid() bool
	Next()
	Value() []byte
	PrimaryKey() []byte
	GetRecord(context.Context) ([]byte, error)
	Close()
}, limit int) {
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	n := 0
	for ; it.Valid(); it.Next() {
		idxVal := it.Value()
		pk := it.PrimaryKey()
		rec, err := it.GetRecord(ctx)
		if err != nil {
			fmt.Fprintf(os.Stderr, "flexctl: GetRecord: %v\n", err)
			continue
		}
		fmt.Fprintf(w, "%s\t%s\t%s\n", idxVal, pk, rec)
		n++
		if limit > 0 && n >= limit {
			break
		}
	}
	w.Flush()
}

func printScan(it interface {
	Valid() bool
	Next()
	Key() []byte
	Value() []byte
}, limit int) {
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	n := 0
	for ; it.Valid(); it.Next() {
		fmt.Fprintf(w, "%s\t%s\n", it.Key(), it.Value())
		n++
		if limit > 0 && n >= limit {
			break
		}
	}
	w.Flush()
}

// ---- stats -----------------------------------------------------------------

// fmtBytes formats a byte count as a human-readable string.
func fmtBytes(n uint64) string {
	switch {
	case n >= 1<<30:
		return fmt.Sprintf("%.2f GiB", float64(n)/float64(1<<30))
	case n >= 1<<20:
		return fmt.Sprintf("%.2f MiB", float64(n)/float64(1<<20))
	case n >= 1<<10:
		return fmt.Sprintf("%.2f KiB", float64(n)/float64(1<<10))
	default:
		return fmt.Sprintf("%d B", n)
	}
}

// diskBytes returns the actual disk blocks used by a file (not apparent size).
func diskBytes(fi os.FileInfo) uint64 {
	if sys, ok := fi.Sys().(*syscall.Stat_t); ok {
		return uint64(sys.Blocks) * 512
	}
	return uint64(fi.Size()) // fallback: apparent size
}

// fileSize returns the actual disk bytes used by a file, or 0 on error.
func fileSize(path string) uint64 {
	fi, err := os.Stat(path)
	if err != nil {
		return 0
	}
	return diskBytes(fi)
}

// dirSize returns the actual disk bytes used by all regular files under root.
func dirSize(root string) uint64 {
	var total uint64
	filepath.Walk(root, func(_ string, fi os.FileInfo, err error) error {
		if err == nil && !fi.IsDir() {
			total += diskBytes(fi)
		}
		return nil
	})
	return total
}

func cmdDatabaseStats(c *ctx) {
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)

	if c.remote != nil {
		st := c.remote.DBStats()
		total := st.WAL0Bytes + st.WAL1Bytes + st.RegistryBytes + st.TablesDirBytes

		fmt.Fprintf(w, "=== Database ===\n")
		fmt.Fprintf(w, "  Server:\t%s\n", c.path)
		fmt.Fprintf(w, "  Write seq:\t%d\n", st.Seq)
		fmt.Fprintf(w, "  Open tables:\t%d (flexdb internal)\n", st.TableCount)
		fmt.Fprintf(w, "  Active memtable:\t%s\n", fmtBytes(uint64(st.ActiveMTBytes)))
		fmt.Fprintf(w, "  Inactive memtable:\t%s\n", fmtBytes(uint64(st.InactiveMTBytes)))
		w.Flush()

		fmt.Fprintf(w, "\n=== Disk Usage ===\n")
		fmt.Fprintf(w, "  MEMTABLE_LOG0:\t%s\n", fmtBytes(st.WAL0Bytes))
		fmt.Fprintf(w, "  MEMTABLE_LOG1:\t%s\n", fmtBytes(st.WAL1Bytes))
		fmt.Fprintf(w, "  TABLE_REGISTRY:\t%s\n", fmtBytes(st.RegistryBytes))
		fmt.Fprintf(w, "  tables/:\t%s\n", fmtBytes(st.TablesDirBytes))
		fmt.Fprintf(w, "  Total:\t%s\n", fmtBytes(total))
		w.Flush()

		tables := c.remote.TableList()
		if len(tables) == 0 {
			return
		}
		fmt.Fprintf(w, "\n=== Tables ===\n")
		for _, name := range tables {
			ts := c.remote.TableStats(name)
			if len(ts.Indexes) > 0 {
				idxNames := make([]string, len(ts.Indexes))
				for i, idx := range ts.Indexes {
					idxNames[i] = idx.Name
				}
				fmt.Fprintf(w, "  %s\t[indexes: %s]\n", name, strings.Join(idxNames, ", "))
			} else {
				fmt.Fprintf(w, "  %s\t[no indexes]\n", name)
			}
			fmt.Fprintf(w, "    data:\t%s\t%d anchors\tcache %s / %s\n",
				fmtBytes(ts.FileBytes), ts.AnchorCount,
				fmtBytes(ts.CacheUsed), fmtBytes(ts.CacheCap))
			for _, idx := range ts.Indexes {
				fmt.Fprintf(w, "    .%s:\t%s\t%d anchors\n",
					idx.Name, fmtBytes(idx.FileBytes), idx.AnchorCount)
			}
		}
		w.Flush()
		return
	}

	dbStats := c.db.RawDB().Stats()
	userTables := c.userTables()

	// --- DB overview ---
	fmt.Fprintf(w, "=== Database ===\n")
	fmt.Fprintf(w, "  Path:\t%s\n", c.path)
	fmt.Fprintf(w, "  Write seq:\t%d\n", dbStats.Seq)
	fmt.Fprintf(w, "  Open tables:\t%d (flexdb internal)\n", dbStats.TableCount)
	fmt.Fprintf(w, "  Active memtable:\t%s\n", fmtBytes(uint64(dbStats.ActiveMTBytes)))
	fmt.Fprintf(w, "  Inactive memtable:\t%s\n", fmtBytes(uint64(dbStats.InactiveMTBytes)))
	w.Flush()

	// --- Disk usage ---
	wal0 := fileSize(filepath.Join(c.path, "MEMTABLE_LOG0"))
	wal1 := fileSize(filepath.Join(c.path, "MEMTABLE_LOG1"))
	registry := fileSize(filepath.Join(c.path, "TABLE_REGISTRY"))
	tablesDir := dirSize(filepath.Join(c.path, "tables"))
	total := wal0 + wal1 + registry + tablesDir

	fmt.Fprintf(w, "\n=== Disk Usage ===\n")
	fmt.Fprintf(w, "  MEMTABLE_LOG0:\t%s\n", fmtBytes(wal0))
	fmt.Fprintf(w, "  MEMTABLE_LOG1:\t%s\n", fmtBytes(wal1))
	fmt.Fprintf(w, "  TABLE_REGISTRY:\t%s\n", fmtBytes(registry))
	fmt.Fprintf(w, "  tables/:\t%s\n", fmtBytes(tablesDir))
	fmt.Fprintf(w, "  Total:\t%s\n", fmtBytes(total))
	w.Flush()

	// --- Per-table stats ---
	if len(userTables) == 0 {
		return
	}
	fmt.Fprintf(w, "\n=== Tables ===\n")
	for _, name := range userTables {
		tbl := c.table(name)
		raw := tbl.RawDataTable()
		ts := raw.Stats()

		indexes := c.schema[name]
		if len(indexes) > 0 {
			idxNames := make([]string, len(indexes))
			for i, idx := range indexes {
				idxNames[i] = idx.Name
			}
			fmt.Fprintf(w, "  %s\t[indexes: %s]\n", name, strings.Join(idxNames, ", "))
		} else {
			fmt.Fprintf(w, "  %s\t[no indexes]\n", name)
		}
		fmt.Fprintf(w, "    data:\t%s\t%d anchors\tcache %s / %s\n",
			fmtBytes(ts.FileBytes), ts.AnchorCount,
			fmtBytes(ts.CacheUsed), fmtBytes(ts.CacheCap))

		for _, idx := range indexes {
			// Ensure the index table is open by re-registering the indexer.
			if fn, err := makeIndexer(idx); err == nil {
				tbl.RegisterIndex(c.ctx, idx.Name, fn) //nolint — opens indexFDB as a side effect
			}
			ft, ok := tbl.RawIndexTable(idx.Name)

			if !ok {
				fmt.Fprintf(w, "    .%s:\t(not open)\n", idx.Name)
				continue
			}
			its := ft.Stats()
			fmt.Fprintf(w, "    .%s:\t%s\t%d anchors\n", idx.Name, fmtBytes(its.FileBytes), its.AnchorCount)
		}
	}
	w.Flush()
}

// ---- REPL ------------------------------------------------------------------

// tokenize splits a line into shell-style tokens, respecting single and double
// quotes and backslash escapes inside double quotes.
func tokenize(s string) ([]string, error) {
	var tokens []string
	var cur strings.Builder
	inSingle, inDouble := false, false
	for i := 0; i < len(s); i++ {
		ch := s[i]
		switch {
		case inSingle:
			if ch == '\'' {
				inSingle = false
			} else {
				cur.WriteByte(ch)
			}
		case inDouble:
			if ch == '"' {
				inDouble = false
			} else if ch == '\\' && i+1 < len(s) {
				i++
				cur.WriteByte(s[i])
			} else {
				cur.WriteByte(ch)
			}
		case ch == '\'':
			inSingle = true
		case ch == '"':
			inDouble = true
		case ch == ' ' || ch == '\t':
			if cur.Len() > 0 {
				tokens = append(tokens, cur.String())
				cur.Reset()
			}
		default:
			cur.WriteByte(ch)
		}
	}
	if inSingle || inDouble {
		return nil, fmt.Errorf("unclosed quote")
	}
	if cur.Len() > 0 {
		tokens = append(tokens, cur.String())
	}
	return tokens, nil
}

// flexCompleter provides tab completion for the REPL.
// It is fully context-aware: subcommands, table names, index names, and key
// prefixes are all completed based on what has already been typed.
type flexCompleter struct{ c *ctx }

// maxKeyCompletions caps the number of key/index-value candidates from live scans.
const maxKeyCompletions = 50

// Do implements readline.AutoCompleter.
func (fc *flexCompleter) Do(line []rune, pos int) (newLine [][]rune, length int) {
	// Only complete up to the cursor position.
	input := string(line[:pos])
	tokens := strings.Fields(input)

	// If the line ends with whitespace, the next token is being started.
	trailingSpace := len(input) > 0 && (input[len(input)-1] == ' ' || input[len(input)-1] == '\t')
	var prefix string
	if !trailingSpace && len(tokens) > 0 {
		prefix = tokens[len(tokens)-1]
		tokens = tokens[:len(tokens)-1]
	}

	candidates := fc.candidates(tokens, prefix)
	var matches [][]rune
	for _, c := range candidates {
		if strings.HasPrefix(c, prefix) {
			matches = append(matches, []rune(c[len(prefix):]))
		}
	}
	return matches, len([]rune(prefix))
}

// candidates returns the set of completions valid after the given completed
// tokens. prefix is the partial token currently being typed; it is passed
// through to live key/index-value scans so they can use a prefix scan instead
// of a full scan.
func (fc *flexCompleter) candidates(tokens []string, prefix string) []string {
	topLevel := []string{
		"table", "value", "index", "schema", "database",
		"check", "commit", "abort", "help", "exit", "quit",
	}
	subCmds := map[string][]string{
		"table":    {"list", "create", "drop", "scan"},
		"value":    {"get", "put", "delete", "exists", "count"},
		"index":    {"list", "create", "drop", "scan"},
		"schema":   {"dump", "load"},
		"database": {"stats", "dump", "load", "batch"},
	}

	switch len(tokens) {
	case 0:
		return topLevel

	case 1:
		cmd := tokens[0]
		if subs, ok := subCmds[cmd]; ok {
			return subs
		}
		if cmd == "check" {
			return fc.tableNames()
		}

	case 2:
		// tokens: [entity, subcommand] — completing the third word.
		cmd, sub := tokens[0], tokens[1]
		switch cmd {
		case "table":
			switch sub {
			case "create", "drop":
				return fc.tableNames()
			case "scan":
				// Next word can be the "prefix" keyword or a table name.
				return append([]string{"prefix"}, fc.tableNames()...)
			}
		case "value":
			switch sub {
			case "get", "put", "delete", "exists", "count":
				return fc.tableNames()
			}
		case "index":
			switch sub {
			case "list", "create", "drop":
				return fc.tableNames()
			case "scan":
				return append([]string{"prefix"}, fc.tableNames()...)
			}
		case "check":
			// check <table> <KEY> — sub is the table name.
			return fc.tableKeys(sub, prefix)
		}

	case 3:
		// tokens: [entity, subcommand, arg1]
		cmd, sub, arg := tokens[0], tokens[1], tokens[2]
		switch cmd {
		case "table":
			if sub == "scan" {
				if arg == "prefix" {
					// table scan prefix <TABLE>
					return fc.tableNames()
				}
				// table scan <table> <START>
				return fc.tableKeys(arg, prefix)
			}
		case "value":
			switch sub {
			case "get", "delete", "exists", "put":
				// value <sub> <table> <KEY>
				return fc.tableKeys(arg, prefix)
			}
		case "index":
			switch sub {
			case "list", "create", "drop":
				return fc.indexNames(arg)
			case "scan":
				if arg == "prefix" {
					// index scan prefix <TABLE>
					return fc.tableNames()
				}
				// index scan <table> — offer index names and the "prefix" keyword.
				return append([]string{"prefix"}, fc.indexNames(arg)...)
			}
		}

	case 4:
		// tokens: [entity, subcommand, arg1, arg2]
		cmd, sub, arg2, arg3 := tokens[0], tokens[1], tokens[2], tokens[3]
		switch cmd {
		case "table":
			if sub == "scan" {
				if arg2 == "prefix" {
					// table scan prefix <table> <KEY_PREFIX>
					return fc.tableKeys(arg3, prefix)
				}
				// table scan <table> <start> <END>
				return fc.tableKeys(arg2, prefix)
			}
		case "index":
			if sub == "scan" {
				if arg2 == "prefix" {
					// index scan prefix <table> <INDEX>
					return fc.indexNames(arg3)
				}
				// index scan <table> <index> <START>
				return fc.indexValues(arg2, arg3, prefix)
			}
		}

	case 5:
		// tokens: [entity, subcommand, arg1, arg2, arg3]
		cmd, sub, arg2, arg3, arg4 := tokens[0], tokens[1], tokens[2], tokens[3], tokens[4]
		if cmd == "index" && sub == "scan" {
			if arg2 == "prefix" {
				// index scan prefix <table> <index> <KEY_PREFIX>
				return fc.indexValues(arg3, arg4, prefix)
			}
			// index scan <table> <index> <start> <END>
			return fc.indexValues(arg2, arg3, prefix)
		}
	}
	return nil
}

func (fc *flexCompleter) tableNames() []string {
	tables, err := fc.c.db.Tables()
	if err != nil {
		return nil
	}
	var out []string
	for _, t := range tables {
		if t != schemaTable {
			out = append(out, t)
		}
	}
	return out
}

func (fc *flexCompleter) indexNames(tableName string) []string {
	indexes := fc.c.schema[tableName]
	out := make([]string, len(indexes))
	for i, idx := range indexes {
		out[i] = idx.Name
	}
	return out
}

// tableKeys returns up to maxKeyCompletions primary keys from tableName whose
// string representation starts with prefix, skipping keys that contain
// non-printable bytes.
func (fc *flexCompleter) tableKeys(tableName, prefix string) []string {
	tbl, err := fc.c.db.Table(fc.c.ctx, tableName)
	if err != nil {
		return nil
	}
	it := tbl.ScanPrefix([]byte(prefix))
	defer it.Close()
	var keys []string
	for ; it.Valid() && len(keys) < maxKeyCompletions; it.Next() {
		k := string(it.Key())
		if isPrintableASCII(k) {
			keys = append(keys, k)
		}
	}
	return keys
}

// indexValues returns up to maxKeyCompletions index values from the named index
// on tableName that start with prefix, skipping values with non-printable bytes.
func (fc *flexCompleter) indexValues(tableName, indexName, prefix string) (vals []string) {
	tbl, err := fc.c.db.Table(fc.c.ctx, tableName)
	if err != nil {
		return nil
	}
	func() {
		defer func() { recover() }() // guard against an unregistered index
		it := tbl.Index(indexName).ScanPrefix([]byte(prefix))
		defer it.Close()
		for ; it.Valid() && len(vals) < maxKeyCompletions; it.Next() {
			v := string(it.Value())
			if isPrintableASCII(v) {
				vals = append(vals, v)
			}
		}
	}()
	return vals
}

// isPrintableASCII reports whether every byte in s is a printable ASCII character.
func isPrintableASCII(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] < 0x20 || s[i] > 0x7e {
			return false
		}
	}
	return true
}

// replBanner prints a human-readable startup banner to stderr.
// It is suppressed when stdin is not a terminal (pipe / script mode).
func replBanner(c *ctx) {
	fi, err := os.Stdin.Stat()
	if err != nil || fi.Mode()&os.ModeCharDevice == 0 {
		return
	}

	mode := "local"
	if c.remote != nil {
		mode = "remote"
	}
	fmt.Fprintf(os.Stderr, "flexctl %s  (%s: %s)\n", version, mode, c.path)

	// Table list — best-effort; skip gracefully on error.
	var tables []string
	if c.remote != nil {
		tables = c.remote.TableList()
	} else {
		tables = c.userTables()
	}
	switch len(tables) {
	case 0:
		fmt.Fprintln(os.Stderr, "  No tables yet.")
	case 1, 2, 3, 4, 5:
		fmt.Fprintf(os.Stderr, "  Tables (%d): %s\n", len(tables), strings.Join(tables, ", "))
	default:
		fmt.Fprintf(os.Stderr, "  Tables: %d\n", len(tables))
	}

	fmt.Fprintln(os.Stderr, "  Type 'help' for commands, 'exit' to quit.")
	fmt.Fprintln(os.Stderr)
}

// cmdRepl runs an interactive read-eval-print loop against c.
func cmdRepl(c *ctx) {
	replMode = true
	replBanner(c)

	prompt := func() string {
		if c.batch != nil {
			return "flexctl (batch)> "
		}
		return "flexctl> "
	}

	rl, err := readline.NewEx(&readline.Config{
		Prompt:          prompt(),
		HistoryLimit:    500,
		AutoComplete:    &flexCompleter{c: c},
		InterruptPrompt: "^C",
		EOFPrompt:       "exit",
	})
	if err != nil {
		// Fall back to a plain scanner if readline init fails.
		cmdReplPlain(c)
		return
	}
	defer rl.Close()

	for {
		rl.SetPrompt(prompt())
		line, err := rl.Readline()
		if err != nil { // io.EOF or readline.ErrInterrupt
			break
		}
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if line == "exit" || line == "quit" || line == ".quit" {
			break
		}
		if line == "help" || line == "?" {
			fmt.Print(replHelp)
			continue
		}

		tokens, err := tokenize(line)
		if err != nil {
			fmt.Fprintf(os.Stderr, "parse error: %v\n", err)
			continue
		}
		if len(tokens) == 0 {
			continue
		}
		func() {
			defer func() {
				if r := recover(); r != nil {
					if e, ok := r.(replError); ok {
						fmt.Fprintln(os.Stderr, string(e))
					} else {
						panic(r)
					}
				}
			}()
			runCommand(c, tokens[0], tokens[1:])
		}()
	}
}

// cmdReplPlain is a fallback REPL using a plain line scanner (no readline).
func cmdReplPlain(c *ctx) {
	interactive := func() bool {
		fi, err := os.Stdin.Stat()
		if err != nil {
			return false
		}
		return (fi.Mode() & os.ModeCharDevice) != 0
	}()

	printPrompt := func() {
		if !interactive {
			return
		}
		if c.batch != nil {
			fmt.Print("flexctl (batch)> ")
		} else {
			fmt.Print("flexctl> ")
		}
	}

	printPrompt()
	var buf strings.Builder
	tmp := make([]byte, 4096)
	for {
		n, err := os.Stdin.Read(tmp)
		if n > 0 {
			buf.Write(tmp[:n])
		}
		for {
			s := buf.String()
			idx := strings.IndexByte(s, '\n')
			if idx < 0 {
				break
			}
			line := strings.TrimSpace(s[:idx])
			buf.Reset()
			buf.WriteString(s[idx+1:])

			if line == "" || strings.HasPrefix(line, "#") {
				printPrompt()
				continue
			}
			if line == "exit" || line == "quit" || line == ".quit" {
				return
			}
			if line == "help" || line == "?" {
				fmt.Print(replHelp)
				printPrompt()
				continue
			}
			tokens, terr := tokenize(line)
			if terr != nil {
				fmt.Fprintf(os.Stderr, "parse error: %v\n", terr)
				printPrompt()
				continue
			}
			if len(tokens) == 0 {
				printPrompt()
				continue
			}
			func() {
				defer func() {
					if r := recover(); r != nil {
						if e, ok := r.(replError); ok {
							fmt.Fprintln(os.Stderr, string(e))
						} else {
							panic(r)
						}
					}
				}()
				runCommand(c, tokens[0], tokens[1:])
			}()
			printPrompt()
		}
		if err != nil {
			break
		}
	}
}

const replHelp = `Commands (omit db-path — it was given on startup):

  table list
  table create  <table>
  table drop    <table>
  table scan    <table> [start [end]] [--limit N]
  table scan prefix <table> <prefix> [--limit N]

  value get     <table> <key> [index]
  value put     <table> <key> <value>
  value delete  <table> <key>
  value exists  <table> <key>
  value count   <table> [index]

  index list    <table>
  index create  <table> <name> exact
  index create  <table> <name> prefix <n>
  index create  <table> <name> suffix <n>
  index create  <table> <name> field <n> [delim]
  index drop    <table> <name>
  index scan    <table> <name> [start [end]] [--limit N]
  index scan prefix <table> <name> <prefix> [--limit N]

  schema dump   [file]
  schema load   <file>

  database stats
  database dump  [file]
  database load  [file]
  database batch [file]
  exit / quit

Batch mode (start with 'database batch', then):
  value put     <table> <key> <value>
  value delete  <table> <key>
  check         <table> <key> [expected]
  database batch show
  commit
  abort
  (Other commands are disabled while a batch is active)
`

// ---- main ------------------------------------------------------------------

func usage() {
	fmt.Fprint(os.Stderr, `Usage: flexctl <db-path> <command> [args...]
       flexctl <db-path>              (starts interactive REPL)

Commands:
  table list                                              List all user tables
  table create  <table>                                   Create a table (no-op if exists)
  table drop    <table>                                   Drop table and all its indexes
  table scan    <table> [start [end]] [--limit N]         Range scan [start, end)
  table scan prefix <table> <prefix> [--limit N]          Prefix scan

  value get     <table> <key> [index]                     Print value; with index, print all index matches
  value put     <table> <key> <value>                     Insert or update a key-value pair
  value delete  <table> <key>                             Delete a key
  value exists  <table> <key>                             Exit 0 if present, 1 if absent
  value count   <table> [index]                           Count entries; with index, count index entries

  index list    <table>                                   List indexes on a table
  index create  <table> <name> <type> [opts]              Create a secondary index
  index drop    <table> <name>                            Drop a secondary index
  index scan    <table> <name> [start [end]] [--limit N]  Range scan on index
  index scan prefix <table> <name> <prefix> [--limit N]   Prefix scan on index

  schema dump   [file]                                    Export schema as JSON (stdout if omitted)
  schema load   <file>                                    Import and apply schema

  database stats                                          Show database and per-table statistics
  database dump  [file]                                   Dump all tables as JSON (stdout if omitted)
  database load  [file]                                   Import JSON dump (stdin if omitted)
  database batch [file]                                   Apply JSON batch (stdin if omitted)

All scan commands accept --limit N (or -n N) to stop after N results.
Confirmation messages go to stderr; data output goes to stdout.

Index types for index create:
  exact                    Entire value
  prefix <n>               First n bytes of value
  suffix <n>               Last n bytes of value
  field  <n> [delim]       Field n (0-based), split by delim (default ",")

Interactive batch mode (REPL only — start with 'database batch', then):
  check  <table> <key> [expected]    Queue a pre-condition check
  value put    <table> <key> <val>   Queue a put
  value delete <table> <key>         Queue a delete
  database batch show                Show queued ops
  commit                             Apply all queued ops atomically
  abort                              Discard all queued ops
`)
	os.Exit(1)
}

// runCommand dispatches a single command with the given arguments against c.
func runCommand(c *ctx, command string, args []string) {
	if c.batch != nil {
		switch command {
		case "value", "check", "commit", "abort", "database":
			// allowed in batch mode
		default:
			die("command %q not allowed in batch mode (use commit or abort first)", command)
		}
	}

	switch command {
	case "table":
		cmdTableEntity(c, args)
	case "value":
		cmdValueEntity(c, args)
	case "index":
		cmdIndexEntity(c, args)
	case "schema":
		cmdSchemaEntity(c, args)
	case "database":
		cmdDatabaseEntity(c, args)
	case "check":
		cmdCheck(c, args)
	case "commit":
		cmdCommit(c)
	case "abort":
		cmdAbort(c)
	default:
		die("unknown command %q", command)
	}
}

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	dbPath := os.Args[1]

	c := openCtx(dbPath)
	defer c.close()

	// No command → start REPL.
	if len(os.Args) == 2 || os.Args[2] == "repl" {
		cmdRepl(c)
		return
	}

	runCommand(c, os.Args[2], os.Args[3:])
}
