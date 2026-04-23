package main

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/brunoga/flexspace-go/flexkv"
)

// ---- roles -----------------------------------------------------------------

const (
	roleRead  = "read"
	roleWrite = "write"
	roleAdmin = "admin"
)

type contextKey int

const ctxRole contextKey = iota

// hasRole returns true when the request context carries at least the required
// privilege level (read ⊂ write ⊂ admin).
func hasRole(r *http.Request, required string) bool {
	role, _ := r.Context().Value(ctxRole).(string)
	switch required {
	case roleRead:
		return role == roleRead || role == roleWrite || role == roleAdmin
	case roleWrite:
		return role == roleWrite || role == roleAdmin
	case roleAdmin:
		return role == roleAdmin
	}
	return false
}

// ---- response helpers -------------------------------------------------------

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		slog.Error("writeJSON encode", "err", err)
	}
}

func apiError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

// ---- Server -----------------------------------------------------------------

// Server holds all shared state for the HTTP handlers.
type Server struct {
	db          *flexkv.DB
	ss          *SchemaStore
	cfg         *Config
	metrics     *Metrics
	tablesMu    sync.RWMutex
	tablesCache map[string]*flexkv.Table
}

func newServer(db *flexkv.DB, ss *SchemaStore, cfg *Config, initialTables map[string]*flexkv.Table) *Server {
	if initialTables == nil {
		initialTables = make(map[string]*flexkv.Table)
	}
	return &Server{db: db, ss: ss, cfg: cfg, metrics: newMetrics(), tablesCache: initialTables}
}

// table returns a cached *flexkv.Table handle, opening it on first use.
// Using the same handle across requests preserves registered indexers.
func (s *Server) table(ctx context.Context, name string) (*flexkv.Table, error) {
	s.tablesMu.RLock()
	tbl, ok := s.tablesCache[name]
	s.tablesMu.RUnlock()
	if ok {
		return tbl, nil
	}
	tbl, err := s.db.Table(ctx, name)
	if err != nil {
		return nil, err
	}
	s.tablesMu.Lock()
	s.tablesCache[name] = tbl
	s.tablesMu.Unlock()
	return tbl, nil
}

// statusWriter wraps http.ResponseWriter to capture the written status code.
type statusWriter struct {
	http.ResponseWriter
	status int
}

func (sw *statusWriter) WriteHeader(code int) {
	sw.status = code
	sw.ResponseWriter.WriteHeader(code)
}

func (sw *statusWriter) Write(b []byte) (int, error) {
	if sw.status == 0 {
		sw.status = http.StatusOK
	}
	return sw.ResponseWriter.Write(b)
}

// Unwrap allows http.ResponseController and other introspectors to reach the
// underlying ResponseWriter (needed for HTTP/2 flush, hijack, etc.).
func (sw *statusWriter) Unwrap() http.ResponseWriter { return sw.ResponseWriter }

// track wraps a handler to record latency and request counts in s.metrics and
// emit a structured log line for every request.
func (s *Server) track(pattern string, h http.HandlerFunc) (string, http.HandlerFunc) {
	rm := s.metrics.getOrCreate(pattern)
	return pattern, func(w http.ResponseWriter, r *http.Request) {
		sw := &statusWriter{ResponseWriter: w}
		rm.active.Add(1)
		start := time.Now()

		h(sw, r)

		dur := time.Since(start)
		rm.active.Add(-1)
		status := sw.status
		if status == 0 {
			status = http.StatusOK
		}
		s.metrics.record(pattern, status, dur)

		role, _ := r.Context().Value(ctxRole).(string)
		logRequest(r.Method, r.URL.Path, pattern, status, dur, role)
	}
}

// logRequest emits a single structured request log line at the appropriate level.
func logRequest(method, path, pattern string, status int, dur time.Duration, role string) {
	attrs := []any{
		"method", method,
		"path", path,
		"route", pattern,
		"status", status,
		"latency_ms", float64(dur.Nanoseconds()) / 1e6,
		"role", role,
	}
	switch {
	case status >= 500:
		slog.Error("request", attrs...)
	case status >= 400:
		slog.Warn("request", attrs...)
	default:
		slog.Debug("request", attrs...)
	}
}

// routes registers all HTTP handlers and returns the root handler wrapped with
// the authentication middleware.
func (s *Server) routes() http.Handler {
	mux := http.NewServeMux()

	// No auth required.
	mux.HandleFunc(s.track("GET /v1/ping", s.handlePing))

	// read role
	mux.HandleFunc(s.track("GET /v1/metrics", s.handleMetrics))
	mux.HandleFunc(s.track("GET /metrics", s.handlePrometheusMetrics))
	mux.HandleFunc(s.track("GET /v1/stats", s.handleStats))

	mux.HandleFunc(s.track("GET /v1/tables", s.handleListTables))
	mux.HandleFunc(s.track("GET /v1/tables/{table}/count", s.handleCount))
	mux.HandleFunc(s.track("GET /v1/tables/{table}/stats", s.handleTableStats))
	mux.HandleFunc(s.track("GET /v1/tables/{table}/keys/{key...}", s.handleGet))
	mux.HandleFunc(s.track("HEAD /v1/tables/{table}/keys/{key...}", s.handleExists))
	mux.HandleFunc(s.track("GET /v1/tables/{table}/scan", s.handleScan))
	mux.HandleFunc(s.track("GET /v1/tables/{table}/indexes", s.handleListIndexes))
	mux.HandleFunc(s.track("GET /v1/tables/{table}/indexes/{index}/count", s.handleIndexCount))
	mux.HandleFunc(s.track("GET /v1/tables/{table}/indexes/{index}/scan", s.handleIndexScan))
	mux.HandleFunc(s.track("GET /v1/schema", s.handleDumpSchema))
	mux.HandleFunc(s.track("GET /v1/dump", s.handleDumpData))

	// write role
	mux.HandleFunc(s.track("PUT /v1/tables/{table}/keys/{key...}", s.handlePut))
	mux.HandleFunc(s.track("DELETE /v1/tables/{table}/keys/{key...}", s.handleDeleteKey))
	mux.HandleFunc(s.track("POST /v1/batch", s.handleBatch))
	mux.HandleFunc(s.track("POST /v1/dump", s.handleLoadData))

	// admin role
	mux.HandleFunc(s.track("POST /v1/tables", s.handleCreateTable))
	mux.HandleFunc(s.track("DELETE /v1/tables/{table}", s.handleDropTable))
	mux.HandleFunc(s.track("POST /v1/tables/{table}/indexes", s.handleCreateIndex))
	mux.HandleFunc(s.track("DELETE /v1/tables/{table}/indexes/{index}", s.handleDropIndex))
	mux.HandleFunc(s.track("POST /v1/schema", s.handleLoadSchema))

	return s.authMiddleware(mux)
}

// authMiddleware validates the bearer token on every request except GET /v1/ping.
func (s *Server) authMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/ping" {
			next.ServeHTTP(w, r)
			return
		}
		auth := r.Header.Get("Authorization")
		if !strings.HasPrefix(auth, "Bearer ") {
			w.Header().Set("WWW-Authenticate", `Bearer realm="flexsrv"`)
			apiError(w, http.StatusUnauthorized, "missing Authorization: Bearer <key> header")
			return
		}
		token := auth[len("Bearer "):]
		role := s.cfg.authenticate(token)
		if role == "" {
			w.Header().Set("WWW-Authenticate", `Bearer realm="flexsrv"`)
			apiError(w, http.StatusUnauthorized, "invalid API key")
			return
		}
		ctx := context.WithValue(r.Context(), ctxRole, role)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// ---- system handlers --------------------------------------------------------

func (s *Server) handlePing(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	if !hasRole(r, roleRead) {
		apiError(w, http.StatusForbidden, "read role required")
		return
	}
	writeJSON(w, http.StatusOK, s.metrics.Snapshot())
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	if !hasRole(r, roleRead) {
		apiError(w, http.StatusForbidden, "read role required")
		return
	}
	raw := s.db.RawDB()
	st := raw.Stats()
	resp := map[string]any{
		"seq":               st.Seq,
		"table_count":       st.TableCount,
		"active_mt_bytes":   st.ActiveMTBytes,
		"inactive_mt_bytes": st.InactiveMTBytes,
		"wal0_bytes":        fileSize(filepath.Join(s.cfg.DBPath, "MEMTABLE_LOG0")),
		"wal1_bytes":        fileSize(filepath.Join(s.cfg.DBPath, "MEMTABLE_LOG1")),
		"registry_bytes":    fileSize(filepath.Join(s.cfg.DBPath, "TABLE_REGISTRY")),
		"tables_dir_bytes":  dirSize(filepath.Join(s.cfg.DBPath, "tables")),
	}
	writeJSON(w, http.StatusOK, resp)
}

func fileSize(path string) uint64 {
	fi, err := os.Stat(path)
	if err != nil {
		return 0
	}
	return uint64(fi.Size())
}

func dirSize(path string) uint64 {
	entries, err := os.ReadDir(path)
	if err != nil {
		return 0
	}
	var total uint64
	for _, e := range entries {
		if !e.IsDir() {
			if fi, err := e.Info(); err == nil {
				total += uint64(fi.Size())
			}
		}
	}
	return total
}

// ---- table handlers ---------------------------------------------------------

func (s *Server) handleListTables(w http.ResponseWriter, r *http.Request) {
	if !hasRole(r, roleRead) {
		apiError(w, http.StatusForbidden, "read role required")
		return
	}
	names, err := s.db.Tables()
	if err != nil {
		apiError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if names == nil {
		names = []string{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"tables": names})
}

func (s *Server) handleCreateTable(w http.ResponseWriter, r *http.Request) {
	if !hasRole(r, roleAdmin) {
		apiError(w, http.StatusForbidden, "admin role required")
		return
	}
	var req struct {
		Name string `json:"name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		apiError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}
	if req.Name == "" {
		apiError(w, http.StatusBadRequest, "name is required")
		return
	}
	if _, err := s.table(r.Context(), req.Name); err != nil { // opens and caches the handle
		apiError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if err := s.ss.RecordTable(req.Name); err != nil {

		slog.Warn("schema record table", "table", req.Name, "err", err)
	}
	writeJSON(w, http.StatusCreated, map[string]string{"name": req.Name})
}

func (s *Server) handleDropTable(w http.ResponseWriter, r *http.Request) {
	if !hasRole(r, roleAdmin) {
		apiError(w, http.StatusForbidden, "admin role required")
		return
	}
	table := r.PathValue("table")
	if err := s.db.DropTable(r.Context(), table); err != nil {
		apiError(w, http.StatusInternalServerError, err.Error())
		return
	}

	s.tablesMu.Lock()
	delete(s.tablesCache, table)
	s.tablesMu.Unlock()
	if err := s.ss.RemoveTable(table); err != nil {
		slog.Warn("schema remove table", "table", table, "err", err)
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleCount(w http.ResponseWriter, r *http.Request) {
	if !hasRole(r, roleRead) {
		apiError(w, http.StatusForbidden, "read role required")
		return
	}
	tbl, err := s.table(r.Context(), r.PathValue("table"))
	if err != nil {
		apiError(w, http.StatusInternalServerError, err.Error())
		return
	}
	it := tbl.Scan(nil, nil)
	defer it.Close()
	n := 0
	for ; it.Valid(); it.Next() {
		n++
	}
	writeJSON(w, http.StatusOK, map[string]int{"count": n})
}

func (s *Server) handleIndexCount(w http.ResponseWriter, r *http.Request) {
	if !hasRole(r, roleRead) {
		apiError(w, http.StatusForbidden, "read role required")
		return
	}
	tbl, err := s.table(r.Context(), r.PathValue("table"))
	if err != nil {
		apiError(w, http.StatusInternalServerError, err.Error())
		return
	}
	it := tbl.Index(r.PathValue("index")).Scan(nil, nil)
	defer it.Close()
	n := 0
	for ; it.Valid(); it.Next() {
		n++
	}
	writeJSON(w, http.StatusOK, map[string]int{"count": n})
}

type indexStatResult struct {
	Name        string `json:"name"`
	FileBytes   uint64 `json:"file_bytes"`
	AnchorCount int    `json:"anchor_count"`
}

func (s *Server) handleTableStats(w http.ResponseWriter, r *http.Request) {
	if !hasRole(r, roleRead) {
		apiError(w, http.StatusForbidden, "read role required")
		return
	}
	tableName := r.PathValue("table")
	tbl, err := s.table(r.Context(), tableName)
	if err != nil {
		apiError(w, http.StatusInternalServerError, err.Error())
		return
	}
	st := tbl.RawDataTable().Stats()

	// Collect per-index stats. We use the schema store to know which indexes
	// exist, and RegisterIndex to ensure their flexdb table is open.
	var idxStats []indexStatResult
	for _, idx := range s.ss.IndexesFor(tableName) {
		fn, err := makeIndexer(idx)
		if err != nil {
			continue
		}
		// RegisterIndex is idempotent and opens the underlying flexdb table.
		tbl.RegisterIndex(r.Context(), idx.Name, fn) //nolint:errcheck
		ft, ok := tbl.RawIndexTable(idx.Name)
		if !ok {
			continue
		}
		its := ft.Stats()
		idxStats = append(idxStats, indexStatResult{
			Name:        idx.Name,
			FileBytes:   its.FileBytes,
			AnchorCount: its.AnchorCount,
		})
	}
	if idxStats == nil {
		idxStats = []indexStatResult{}
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"file_bytes":   st.FileBytes,
		"anchor_count": st.AnchorCount,
		"cache_used":   st.CacheUsed,
		"cache_cap":    st.CacheCap,
		"indexes":      idxStats,
	})
}

// ---- key-value handlers -----------------------------------------------------

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	if !hasRole(r, roleRead) {
		apiError(w, http.StatusForbidden, "read role required")
		return
	}
	tbl, err := s.table(r.Context(), r.PathValue("table"))
	if err != nil {
		apiError(w, http.StatusInternalServerError, err.Error())
		return
	}
	val, err := tbl.Get(r.Context(), []byte(r.PathValue("key")))
	if err != nil {
		apiError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if val == nil {
		apiError(w, http.StatusNotFound, "key not found")
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.Itoa(len(val)))
	w.WriteHeader(http.StatusOK)
	w.Write(val) //nolint:errcheck
}

func (s *Server) handleExists(w http.ResponseWriter, r *http.Request) {
	if !hasRole(r, roleRead) {
		apiError(w, http.StatusForbidden, "read role required")
		return
	}
	tbl, err := s.table(r.Context(), r.PathValue("table"))
	if err != nil {
		apiError(w, http.StatusInternalServerError, err.Error())
		return
	}
	val, err := tbl.Get(r.Context(), []byte(r.PathValue("key")))
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	if val == nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Length", strconv.Itoa(len(val)))
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handlePut(w http.ResponseWriter, r *http.Request) {
	if !hasRole(r, roleWrite) {
		apiError(w, http.StatusForbidden, "write role required")
		return
	}
	limit := int64(s.cfg.Limits.MaxValueBytes) + 1
	body, err := io.ReadAll(io.LimitReader(r.Body, limit))
	if err != nil {
		apiError(w, http.StatusBadRequest, "read body: "+err.Error())
		return
	}
	if int64(len(body)) >= limit {
		apiError(w, http.StatusRequestEntityTooLarge,
			fmt.Sprintf("value exceeds max_value_bytes (%d)", s.cfg.Limits.MaxValueBytes))
		return
	}
	tbl, err := s.table(r.Context(), r.PathValue("table"))
	if err != nil {
		apiError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if err := tbl.Put(r.Context(), []byte(r.PathValue("key")), body); err != nil {
		apiError(w, http.StatusInternalServerError, err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleDeleteKey(w http.ResponseWriter, r *http.Request) {
	if !hasRole(r, roleWrite) {
		apiError(w, http.StatusForbidden, "write role required")
		return
	}
	tbl, err := s.table(r.Context(), r.PathValue("table"))
	if err != nil {
		apiError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if err := tbl.Delete(r.Context(), []byte(r.PathValue("key"))); err != nil {
		apiError(w, http.StatusInternalServerError, err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// handleScan streams NDJSON over [start,end) or a prefix.
//
//	?start=<str>&end=<str>&limit=N   range scan
//	?prefix=<str>&limit=N            prefix scan
//
// Each line: {"key":"...","value":"<base64>"}
func (s *Server) handleScan(w http.ResponseWriter, r *http.Request) {
	if !hasRole(r, roleRead) {
		apiError(w, http.StatusForbidden, "read role required")
		return
	}
	q := r.URL.Query()
	limit := s.cfg.Limits.MaxScanResults
	if l := q.Get("limit"); l != "" {
		if n, err := strconv.Atoi(l); err == nil && n > 0 && n < limit {
			limit = n
		}
	}

	tbl, err := s.table(r.Context(), r.PathValue("table"))
	if err != nil {
		apiError(w, http.StatusInternalServerError, err.Error())
		return
	}

	var it *flexkv.Iterator
	if prefix := q.Get("prefix"); prefix != "" {
		it = tbl.ScanPrefix([]byte(prefix))
	} else {
		var start, end []byte
		if v := q.Get("start"); v != "" {
			start = []byte(v)
		}
		if v := q.Get("end"); v != "" {
			end = []byte(v)
		}
		it = tbl.Scan(start, end)
	}
	defer it.Close()

	w.Header().Set("Content-Type", "application/x-ndjson")
	w.WriteHeader(http.StatusOK)
	bw := bufio.NewWriter(w)
	flusher, canFlush := w.(http.Flusher)

	enc := json.NewEncoder(bw)
	type row struct {
		Key   string `json:"key"`
		Value string `json:"value"` // base64-encoded
	}
	n := 0
	for ; it.Valid() && n < limit; it.Next() {
		r := row{
			Key:   string(it.Key()),
			Value: base64.StdEncoding.EncodeToString(it.Value()),
		}
		if err := enc.Encode(r); err != nil {
			break
		}
		n++
		if canFlush && n%100 == 0 {
			bw.Flush() //nolint:errcheck
			flusher.Flush()
		}
	}
	bw.Flush() //nolint:errcheck
	if canFlush {
		flusher.Flush()
	}
}

// ---- index handlers ---------------------------------------------------------

func (s *Server) handleListIndexes(w http.ResponseWriter, r *http.Request) {
	if !hasRole(r, roleRead) {
		apiError(w, http.StatusForbidden, "read role required")
		return
	}
	idxs := s.ss.IndexesFor(r.PathValue("table"))
	if idxs == nil {
		idxs = []SchemaIndex{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"indexes": idxs})
}

func (s *Server) handleCreateIndex(w http.ResponseWriter, r *http.Request) {
	if !hasRole(r, roleAdmin) {
		apiError(w, http.StatusForbidden, "admin role required")
		return
	}
	var spec SchemaIndex
	if err := json.NewDecoder(r.Body).Decode(&spec); err != nil {
		apiError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}
	if spec.Name == "" {
		apiError(w, http.StatusBadRequest, "name is required")
		return
	}
	fn, err := makeIndexer(spec)
	if err != nil {
		apiError(w, http.StatusBadRequest, err.Error())
		return
	}
	tableName := r.PathValue("table")
	tbl, err := s.table(r.Context(), tableName)
	if err != nil {
		apiError(w, http.StatusInternalServerError, err.Error())
		return
	}
	// CreateIndex scans existing data to populate the index — can be slow for
	// large tables, but it's correct and idempotent.
	if err := tbl.CreateIndex(r.Context(), spec.Name, fn); err != nil {
		apiError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if err := s.ss.RecordIndex(tableName, spec); err != nil {
		slog.Warn("schema record index", "table", tableName, "index", spec.Name, "err", err)
	}
	writeJSON(w, http.StatusCreated, spec)
}

func (s *Server) handleDropIndex(w http.ResponseWriter, r *http.Request) {
	if !hasRole(r, roleAdmin) {
		apiError(w, http.StatusForbidden, "admin role required")
		return
	}
	tableName := r.PathValue("table")
	indexName := r.PathValue("index")
	tbl, err := s.table(r.Context(), tableName)
	if err != nil {
		apiError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if err := tbl.DropIndex(r.Context(), indexName); err != nil {
		apiError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if err := s.ss.RemoveIndex(tableName, indexName); err != nil {
		slog.Warn("schema remove index", "table", tableName, "index", indexName, "err", err)
	}
	w.WriteHeader(http.StatusNoContent)
}

// handleIndexScan streams NDJSON from a secondary index.
//
//	?value=<str>                     exact match
//	?prefix=<str>&limit=N            prefix scan
//	?start=<str>&end=<str>&limit=N   range scan
//	&records=true                    include the full value ("record" field)
//
// Each line: {"indexed_value":"...","primary_key":"...","record":"<base64>"}
// When records=false (default) the "record" field is omitted.
func (s *Server) handleIndexScan(w http.ResponseWriter, r *http.Request) {
	if !hasRole(r, roleRead) {
		apiError(w, http.StatusForbidden, "read role required")
		return
	}
	q := r.URL.Query()
	limit := s.cfg.Limits.MaxScanResults
	if l := q.Get("limit"); l != "" {
		if n, err := strconv.Atoi(l); err == nil && n > 0 && n < limit {
			limit = n
		}
	}
	fetchRecords := q.Get("records") == "true"

	tableName := r.PathValue("table")
	indexName := r.PathValue("index")
	tbl, err := s.table(r.Context(), tableName)
	if err != nil {
		apiError(w, http.StatusInternalServerError, err.Error())
		return
	}
	idx := tbl.Index(indexName)

	var it *flexkv.IndexIterator
	if v := q.Get("value"); v != "" {
		it = idx.Get([]byte(v))
	} else if p := q.Get("prefix"); p != "" {
		it = idx.ScanPrefix([]byte(p))
	} else {
		var start, end []byte
		if v := q.Get("start"); v != "" {
			start = []byte(v)
		}
		if v := q.Get("end"); v != "" {
			end = []byte(v)
		}
		it = idx.Scan(start, end)
	}
	defer it.Close()

	w.Header().Set("Content-Type", "application/x-ndjson")
	w.WriteHeader(http.StatusOK)
	bw := bufio.NewWriter(w)
	flusher, canFlush := w.(http.Flusher)

	enc := json.NewEncoder(bw)
	type row struct {
		IndexedValue string `json:"indexed_value"`
		PrimaryKey   string `json:"primary_key"`
		Record       string `json:"record,omitempty"` // base64, only when records=true
	}
	n := 0
	for ; it.Valid() && n < limit; it.Next() {
		row := row{
			IndexedValue: string(it.Value()),
			PrimaryKey:   string(it.PrimaryKey()),
		}
		if fetchRecords {
			rec, err := it.GetRecord(r.Context())
			if err == nil {
				row.Record = base64.StdEncoding.EncodeToString(rec)
			}
		}

		if err := enc.Encode(row); err != nil {
			break
		}
		n++
		if canFlush && n%100 == 0 {
			bw.Flush() //nolint:errcheck
			flusher.Flush()
		}
	}
	bw.Flush() //nolint:errcheck
	if canFlush {
		flusher.Flush()
	}
}

// ---- batch handler ----------------------------------------------------------

// handleBatch executes an atomic batch of checks and operations.
//
// Request body (JSON):
//
//	{
//	  "checks": [{"table":"t","key":"k","expected":"v"}],  // optional
//	  "ops":    [{"op":"put","table":"t","key":"k","value":"v"},
//	             {"op":"delete","table":"t","key":"k2"}]
//	}
//
// Response:
//
//	{"committed": true}
//	{"committed": false, "reason": "check failed: ..."}
func (s *Server) handleBatch(w http.ResponseWriter, r *http.Request) {
	if !hasRole(r, roleWrite) {
		apiError(w, http.StatusForbidden, "write role required")
		return
	}

	type batchCheck struct {
		Table    string  `json:"table"`
		Key      string  `json:"key"`
		Expected *string `json:"expected"` // nil means key must be absent
	}
	type batchOp struct {
		Op    string `json:"op"` // "put" or "delete"
		Table string `json:"table"`
		Key   string `json:"key"`
		Value string `json:"value,omitempty"`
	}
	type batchReq struct {
		Checks []batchCheck `json:"checks,omitempty"`
		Ops    []batchOp    `json:"ops"`
	}

	var req batchReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		apiError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}
	if len(req.Ops) == 0 {
		apiError(w, http.StatusBadRequest, "ops must not be empty")
		return
	}

	// Resolve table handles up-front so we can return early on bad table names.
	tableCache := map[string]*flexkv.Table{}
	getTable := func(name string) (*flexkv.Table, error) {
		if t, ok := tableCache[name]; ok {
			return t, nil
		}
		t, err := s.table(r.Context(), name)
		if err != nil {
			return nil, err
		}
		tableCache[name] = t
		return t, nil
	}

	// Build a flexkv.Batch: checks + all ops committed as one WAL record.
	batch := s.db.NewBatch()

	for _, c := range req.Checks {
		tbl, err := getTable(c.Table)
		if err != nil {
			apiError(w, http.StatusInternalServerError, err.Error())
			return
		}
		var expected []byte
		if c.Expected != nil {
			expected = []byte(*c.Expected)
		}
		batch.Check(tbl, []byte(c.Key), expected)
	}

	for _, op := range req.Ops {
		tbl, err := getTable(op.Table)
		if err != nil {
			apiError(w, http.StatusInternalServerError, err.Error())
			return
		}
		switch op.Op {
		case "put":
			batch.Put(tbl, []byte(op.Key), []byte(op.Value))
		case "delete":
			batch.Delete(tbl, []byte(op.Key))
		default:
			apiError(w, http.StatusBadRequest, fmt.Sprintf("unknown op %q (want: put, delete)", op.Op))
			return
		}
	}

	committed, err := batch.Commit(r.Context())
	if err != nil {
		apiError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if !committed {
		// Identify the failing check for the error message (best-effort — we
		// report the first check whose condition is no longer satisfied).
		reason := "check failed"
		for _, c := range req.Checks {
			tbl, terr := getTable(c.Table)
			if terr != nil {
				break
			}
			cur, gerr := tbl.Get(r.Context(), []byte(c.Key))
			if gerr != nil {
				break
			}

			var match bool
			if c.Expected == nil {
				match = cur == nil
			} else {
				match = string(cur) == *c.Expected
			}
			if !match {
				reason = fmt.Sprintf("check failed: table=%q key=%q", c.Table, c.Key)
				break
			}
		}
		writeJSON(w, http.StatusOK, map[string]any{"committed": false, "reason": reason})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"committed": true})
}

// ---- schema handlers --------------------------------------------------------

func (s *Server) handleDumpSchema(w http.ResponseWriter, r *http.Request) {
	if !hasRole(r, roleRead) {
		apiError(w, http.StatusForbidden, "read role required")
		return
	}
	sc := s.ss.Get()
	if sc.Version == 0 {
		sc.Version = 1
	}
	writeJSON(w, http.StatusOK, sc)
}

// handleLoadSchema applies a schema: creates missing tables and indexes.
// Existing tables/indexes are left untouched (idempotent).
func (s *Server) handleLoadSchema(w http.ResponseWriter, r *http.Request) {
	if !hasRole(r, roleAdmin) {
		apiError(w, http.StatusForbidden, "admin role required")
		return
	}
	var sc Schema
	if err := json.NewDecoder(r.Body).Decode(&sc); err != nil {
		apiError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}

	existing := s.ss.Get()
	existingIdx := func(tableName, idxName string) bool {
		for _, t := range existing.Tables {
			if t.Name != tableName {
				continue
			}
			for _, idx := range t.Indexes {
				if idx.Name == idxName {
					return true
				}
			}
		}
		return false
	}

	for _, tblSpec := range sc.Tables {
		tbl, err := s.table(r.Context(), tblSpec.Name)
		if err != nil {
			apiError(w, http.StatusInternalServerError, "open table "+tblSpec.Name+": "+err.Error())
			return
		}
		if err := s.ss.RecordTable(tblSpec.Name); err != nil {
			slog.Warn("schema record table", "table", tblSpec.Name, "err", err)
		}
		for _, idxSpec := range tblSpec.Indexes {
			if existingIdx(tblSpec.Name, idxSpec.Name) {
				continue // already registered; leave it alone
			}
			fn, err := makeIndexer(idxSpec)
			if err != nil {
				apiError(w, http.StatusBadRequest, fmt.Sprintf("index %s.%s: %v", tblSpec.Name, idxSpec.Name, err))
				return
			}
			if err := tbl.CreateIndex(r.Context(), idxSpec.Name, fn); err != nil {
				apiError(w, http.StatusInternalServerError, fmt.Sprintf("create index %s.%s: %v", tblSpec.Name, idxSpec.Name, err))
				return
			}
			if err := s.ss.RecordIndex(tblSpec.Name, idxSpec); err != nil {
				slog.Warn("schema record index", "table", tblSpec.Name, "index", idxSpec.Name, "err", err)
			}
		}
	}

	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// ---- dump / load data -------------------------------------------------------

// handleDumpData streams all table data as NDJSON.
//
// Each line is one of:
//
//	{"type":"table","name":"<tableName>"}
//	{"type":"entry","key":"<key>","value":"<base64value>"}
//
// Tables are listed in sorted order; entries are in key order.
func (s *Server) handleDumpData(w http.ResponseWriter, r *http.Request) {
	if !hasRole(r, roleRead) {
		apiError(w, http.StatusForbidden, "read role required")
		return
	}
	names, err := s.db.Tables()
	if err != nil {
		apiError(w, http.StatusInternalServerError, err.Error())
		return
	}

	w.Header().Set("Content-Type", "application/x-ndjson")
	w.WriteHeader(http.StatusOK)
	bw := bufio.NewWriter(w)
	enc := json.NewEncoder(bw)
	flusher, canFlush := w.(http.Flusher)

	type header struct {
		Type string `json:"type"`
		Name string `json:"name"`
	}
	type entry struct {
		Type  string `json:"type"`
		Key   string `json:"key"`
		Value string `json:"value"` // base64
	}

	for _, name := range names {
		tbl, err := s.table(r.Context(), name)
		if err != nil {
			slog.Warn("dump: open table", "table", name, "err", err)
			continue
		}
		enc.Encode(header{Type: "table", Name: name}) //nolint:errcheck
		it := tbl.Scan(nil, nil)

		for ; it.Valid(); it.Next() {
			enc.Encode(entry{ //nolint:errcheck
				Type:  "entry",
				Key:   string(it.Key()),
				Value: base64.StdEncoding.EncodeToString(it.Value()),
			})
		}
		it.Close()
		if canFlush {
			bw.Flush() //nolint:errcheck
			flusher.Flush()
		}
	}
	bw.Flush() //nolint:errcheck
}

// handleLoadData imports data from the NDJSON format produced by handleDumpData.
func (s *Server) handleLoadData(w http.ResponseWriter, r *http.Request) {
	if !hasRole(r, roleWrite) {
		apiError(w, http.StatusForbidden, "write role required")
		return
	}

	type line struct {
		Type  string `json:"type"`
		Name  string `json:"name,omitempty"`
		Key   string `json:"key,omitempty"`
		Value string `json:"value,omitempty"`
	}

	var (
		curTbl   *flexkv.Table
		imported int
		errors   []string
	)

	scanner := bufio.NewScanner(r.Body)
	// Each NDJSON line holds a base64-encoded value (4/3 overhead) plus JSON envelope.
	// Start with a small buffer and let it grow up to the configured limit.
	scanner.Buffer(nil, s.cfg.Limits.MaxValueBytes*4/3+1024)
	for scanner.Scan() {
		var l line
		if err := json.Unmarshal(scanner.Bytes(), &l); err != nil {
			errors = append(errors, "parse: "+err.Error())
			continue
		}
		switch l.Type {
		case "table":
			t, err := s.table(r.Context(), l.Name)
			if err != nil {
				errors = append(errors, "open table "+l.Name+": "+err.Error())
				curTbl = nil
				continue
			}
			curTbl = t
			s.ss.RecordTable(l.Name) //nolint:errcheck
		case "entry":
			if curTbl == nil {
				errors = append(errors, "entry before table header")
				continue
			}
			val, err := base64.StdEncoding.DecodeString(l.Value)
			if err != nil {
				errors = append(errors, "decode value for key "+l.Key+": "+err.Error())
				continue
			}
			if err := curTbl.Put(r.Context(), []byte(l.Key), val); err != nil {
				errors = append(errors, "put "+l.Key+": "+err.Error())
			} else {
				imported++
			}
		}

	}
	if err := scanner.Err(); err != nil {
		errors = append(errors, "read body: "+err.Error())
	}

	resp := map[string]any{"imported": imported}
	if len(errors) > 0 {
		resp["errors"] = errors
	}
	writeJSON(w, http.StatusOK, resp)
}
func (s *Server) handlePrometheusMetrics(w http.ResponseWriter, r *http.Request) {
	// No auth for Prometheus typically, or handled via infra.
	// But let's require read role to be safe if a key is provided,
	// or allow if it's a known monitoring IP in a real deployment.
	// For v1, let's keep it simple: if auth is configured, use it.
	if !hasRole(r, roleRead) {
		apiError(w, http.StatusForbidden, "read role required")
		return
	}

	m := s.db.RawDB().Metrics()
	st := s.db.RawDB().Stats()

	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	w.WriteHeader(http.StatusOK)

	fmt.Fprintf(w, "# HELP flexdb_put_total Total number of Put operations\n")
	fmt.Fprintf(w, "# TYPE flexdb_put_total counter\n")
	fmt.Fprintf(w, "flexdb_put_total %d\n", m.PutCount)

	fmt.Fprintf(w, "# HELP flexdb_get_total Total number of Get operations\n")
	fmt.Fprintf(w, "# TYPE flexdb_get_total counter\n")
	fmt.Fprintf(w, "flexdb_get_total %d\n", m.GetCount)

	fmt.Fprintf(w, "# HELP flexdb_delete_total Total number of Delete operations\n")
	fmt.Fprintf(w, "# TYPE flexdb_delete_total counter\n")
	fmt.Fprintf(w, "flexdb_delete_total %d\n", m.DeleteCount)

	fmt.Fprintf(w, "# HELP flexdb_cache_hits_total Total number of cache hits\n")
	fmt.Fprintf(w, "# TYPE flexdb_cache_hits_total counter\n")
	fmt.Fprintf(w, "flexdb_cache_hits_total %d\n", m.CacheHitCount)

	fmt.Fprintf(w, "# HELP flexdb_cache_misses_total Total number of cache misses\n")
	fmt.Fprintf(w, "# TYPE flexdb_cache_misses_total counter\n")
	fmt.Fprintf(w, "flexdb_cache_misses_total %d\n", m.CacheMissCount)

	fmt.Fprintf(w, "# HELP flexdb_wal_writes_total Total number of WAL records written\n")
	fmt.Fprintf(w, "# TYPE flexdb_wal_writes_total counter\n")
	fmt.Fprintf(w, "flexdb_wal_writes_total %d\n", m.WALWriteCount)

	fmt.Fprintf(w, "# HELP flexdb_wal_flushes_total Total number of WAL flushes to disk\n")
	fmt.Fprintf(w, "# TYPE flexdb_wal_flushes_total counter\n")
	fmt.Fprintf(w, "flexdb_wal_flushes_total %d\n", m.WALFlushCount)

	fmt.Fprintf(w, "# HELP flexdb_flush_batch_total Total number of memtable flush batches\n")
	fmt.Fprintf(w, "# TYPE flexdb_flush_batch_total counter\n")
	fmt.Fprintf(w, "flexdb_flush_batch_total %d\n", m.FlushBatchCount)

	fmt.Fprintf(w, "# HELP flexdb_gc_reclaimed_bytes_total Total bytes reclaimed by GC\n")
	fmt.Fprintf(w, "# TYPE flexdb_gc_reclaimed_bytes_total counter\n")
	fmt.Fprintf(w, "flexdb_gc_reclaimed_bytes_total %d\n", m.GCReclaimedBytes)

	fmt.Fprintf(w, "# HELP flexdb_gc_moved_bytes_total Total bytes moved by GC\n")
	fmt.Fprintf(w, "# TYPE flexdb_gc_moved_bytes_total counter\n")
	fmt.Fprintf(w, "flexdb_gc_moved_bytes_total %d\n", m.GCMovedBytes)

	fmt.Fprintf(w, "# HELP flexdb_active_memtable_bytes Current size of active memtable\n")
	fmt.Fprintf(w, "# TYPE flexdb_active_memtable_bytes gauge\n")
	fmt.Fprintf(w, "flexdb_active_memtable_bytes %d\n", st.ActiveMTBytes)

	fmt.Fprintf(w, "# HELP flexdb_inactive_memtable_bytes Current size of inactive memtable\n")
	fmt.Fprintf(w, "# TYPE flexdb_inactive_memtable_bytes gauge\n")
	fmt.Fprintf(w, "flexdb_inactive_memtable_bytes %d\n", st.InactiveMTBytes)

	fmt.Fprintf(w, "# HELP flexdb_table_count Total number of open tables\n")
	fmt.Fprintf(w, "# TYPE flexdb_table_count gauge\n")
	fmt.Fprintf(w, "flexdb_table_count %d\n", st.TableCount)
}
