package main

// remote.go — HTTP client for flexsrv.
//
// When flexctl's first argument starts with "http://" or "https://", openCtx
// populates ctx.remote with a RemoteClient and leaves ctx.db nil.  Each
// cmd* function that supports remote mode begins with:
//
//	if c.remote != nil { c.remote.DoThing(...); return }
//
// Authentication: set the FLEXSRV_KEY environment variable to the bearer token
// printed by `flexsrv keygen`.  Pass --insecure to accept self-signed certs.

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

// RemoteClient holds an HTTP client configured for one flexsrv instance.
type RemoteClient struct {
	base string       // e.g. "https://host:7700"
	key  string       // bearer token (empty = unauthenticated)
	hc   *http.Client // shared HTTP/2 client
}

func newRemoteClient(addr, key string, insecure bool) *RemoteClient {
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: insecure}, //nolint:gosec
	}
	return &RemoteClient{
		base: strings.TrimRight(addr, "/"),
		key:  key,
		hc:   &http.Client{Timeout: 60 * time.Second, Transport: tr},
	}
}

// ---- low-level HTTP helpers -------------------------------------------------

func (rc *RemoteClient) url(path string) string {
	return rc.base + "/v1" + path
}

func (rc *RemoteClient) newReq(method, path string, body io.Reader, ct string) *http.Request {
	req, err := http.NewRequest(method, rc.url(path), body)
	if err != nil {
		die("build request: %v", err)
	}
	if rc.key != "" {
		req.Header.Set("Authorization", "Bearer "+rc.key)
	}
	if ct != "" {
		req.Header.Set("Content-Type", ct)
	}
	return req
}

// doJSON sends a JSON request and decodes the JSON response into out (may be nil).
func (rc *RemoteClient) doJSON(method, path string, in, out any) {
	var body io.Reader
	if in != nil {
		data, err := json.Marshal(in)
		if err != nil {
			die("marshal request: %v", err)
		}
		body = bytes.NewReader(data)
	}
	req := rc.newReq(method, path, body, "application/json")
	resp, err := rc.hc.Do(req)
	if err != nil {
		die("%s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		var e struct{ Error string `json:"error"` }
		json.NewDecoder(resp.Body).Decode(&e) //nolint:errcheck
		if e.Error != "" {
			die("%s %s: %s", method, path, e.Error)
		}
		die("%s %s: HTTP %d", method, path, resp.StatusCode)
	}
	if out != nil {
		if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
			die("decode response: %v", err)
		}
	}
}

// doStream opens a streaming GET and returns the response for the caller to
// read and close.
func (rc *RemoteClient) doStream(path string) *http.Response {
	req := rc.newReq("GET", path, nil, "")
	resp, err := rc.hc.Do(req)
	if err != nil {
		die("GET %s: %v", path, err)
	}
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		die("GET %s: HTTP %d", path, resp.StatusCode)
	}
	return resp
}

// ---- table management -------------------------------------------------------

func (rc *RemoteClient) TableList() []string {
	var r struct{ Tables []string `json:"tables"` }
	rc.doJSON("GET", "/tables", nil, &r)
	if r.Tables == nil {
		return []string{}
	}
	return r.Tables
}

func (rc *RemoteClient) TableCreate(name string) {
	rc.doJSON("POST", "/tables", map[string]string{"name": name}, nil)
}

func (rc *RemoteClient) TableDrop(name string) {
	rc.doJSON("DELETE", "/tables/"+url.PathEscape(name), nil, nil)
}

// ---- KV ops -----------------------------------------------------------------

// Get returns the value for key in table, or nil if not found.
func (rc *RemoteClient) Get(table, key string) []byte {
	req := rc.newReq("GET",
		"/tables/"+url.PathEscape(table)+"/keys/"+url.PathEscape(key),
		nil, "")
	resp, err := rc.hc.Do(req)
	if err != nil {
		die("get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return nil
	}
	if resp.StatusCode != http.StatusOK {
		die("get: HTTP %d", resp.StatusCode)
	}
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		die("get read: %v", err)
	}
	return data
}

func (rc *RemoteClient) Put(table, key string, value []byte) {
	req := rc.newReq("PUT",
		"/tables/"+url.PathEscape(table)+"/keys/"+url.PathEscape(key),
		bytes.NewReader(value), "application/octet-stream")
	resp, err := rc.hc.Do(req)
	if err != nil {
		die("put: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		die("put: HTTP %d", resp.StatusCode)
	}
}

func (rc *RemoteClient) Delete(table, key string) {
	req := rc.newReq("DELETE",
		"/tables/"+url.PathEscape(table)+"/keys/"+url.PathEscape(key),
		nil, "")
	resp, err := rc.hc.Do(req)
	if err != nil {
		die("delete: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		die("delete: HTTP %d", resp.StatusCode)
	}
}

func (rc *RemoteClient) Exists(table, key string) bool {
	req := rc.newReq("HEAD",
		"/tables/"+url.PathEscape(table)+"/keys/"+url.PathEscape(key),
		nil, "")
	resp, err := rc.hc.Do(req)
	if err != nil {
		die("exists: %v", err)
	}
	resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

func (rc *RemoteClient) Count(table string) int {
	var r struct{ Count int `json:"count"` }
	rc.doJSON("GET", "/tables/"+url.PathEscape(table)+"/count", nil, &r)
	return r.Count
}

func (rc *RemoteClient) IndexCount(table, index string) int {
	var r struct{ Count int `json:"count"` }
	rc.doJSON("GET", "/tables/"+url.PathEscape(table)+"/indexes/"+url.PathEscape(index)+"/count", nil, &r)
	return r.Count
}

// ---- scan iterators ---------------------------------------------------------

// remoteScanIter streams NDJSON from a GET /v1/tables/{table}/scan response.
// It satisfies the same structural interface as *flexkv.Iterator so it can
// be passed to printScan.
type remoteScanIter struct {
	resp    *http.Response
	scanner *bufio.Scanner
	key     []byte
	val     []byte
	valid   bool
}

func (rc *RemoteClient) Scan(table string, start, end []byte, limit int) *remoteScanIter {
	q := url.Values{}
	if len(start) > 0 {
		q.Set("start", string(start))
	}
	if len(end) > 0 {
		q.Set("end", string(end))
	}
	if limit > 0 {
		q.Set("limit", fmt.Sprintf("%d", limit))
	}
	path := "/tables/" + url.PathEscape(table) + "/scan?" + q.Encode()
	resp := rc.doStream(path)
	it := &remoteScanIter{resp: resp, scanner: bufio.NewScanner(resp.Body)}
	it.advance()
	return it
}

func (rc *RemoteClient) ScanPrefix(table string, prefix []byte, limit int) *remoteScanIter {
	q := url.Values{"prefix": []string{string(prefix)}}
	if limit > 0 {
		q.Set("limit", fmt.Sprintf("%d", limit))
	}
	path := "/tables/" + url.PathEscape(table) + "/scan?" + q.Encode()
	resp := rc.doStream(path)
	it := &remoteScanIter{resp: resp, scanner: bufio.NewScanner(resp.Body)}
	it.advance()
	return it
}

func (it *remoteScanIter) advance() {
	if !it.scanner.Scan() {
		it.valid = false
		return
	}
	var row struct {
		Key   string `json:"key"`
		Value string `json:"value"` // base64
	}
	if err := json.Unmarshal(it.scanner.Bytes(), &row); err != nil {
		it.valid = false
		return
	}
	it.key = []byte(row.Key)
	it.val, _ = base64.StdEncoding.DecodeString(row.Value)
	it.valid = true
}

func (it *remoteScanIter) Valid() bool   { return it.valid }
func (it *remoteScanIter) Next()         { it.advance() }
func (it *remoteScanIter) Key() []byte   { return it.key }
func (it *remoteScanIter) Value() []byte { return it.val }
func (it *remoteScanIter) Close()        { it.resp.Body.Close() }

// ---- index ops --------------------------------------------------------------

func (rc *RemoteClient) IndexList(table string) []SchemaIndex {
	var r struct{ Indexes []SchemaIndex `json:"indexes"` }
	rc.doJSON("GET", "/tables/"+url.PathEscape(table)+"/indexes", nil, &r)
	if r.Indexes == nil {
		return []SchemaIndex{}
	}
	return r.Indexes
}

func (rc *RemoteClient) IndexCreate(table string, spec SchemaIndex) {
	rc.doJSON("POST", "/tables/"+url.PathEscape(table)+"/indexes", spec, nil)
}

func (rc *RemoteClient) IndexDrop(table, name string) {
	rc.doJSON("DELETE",
		"/tables/"+url.PathEscape(table)+"/indexes/"+url.PathEscape(name),
		nil, nil)
}

// remoteIndexIter streams NDJSON from an index scan response.
// It satisfies the same structural interface as *flexkv.IndexIterator.
type remoteIndexIter struct {
	resp    *http.Response
	scanner *bufio.Scanner
	idxVal  []byte
	pk      []byte
	record  []byte
	valid   bool
}

func (rc *RemoteClient) indexScan(table, index string, q url.Values) *remoteIndexIter {
	q.Set("records", "true") // always fetch full records
	path := "/tables/" + url.PathEscape(table) +
		"/indexes/" + url.PathEscape(index) +
		"/scan?" + q.Encode()
	resp := rc.doStream(path)
	it := &remoteIndexIter{resp: resp, scanner: bufio.NewScanner(resp.Body)}
	it.advance()
	return it
}

func (rc *RemoteClient) IndexScan(table, index string, start, end []byte, limit int) *remoteIndexIter {
	q := url.Values{}
	if len(start) > 0 {
		q.Set("start", string(start))
	}
	if len(end) > 0 {
		q.Set("end", string(end))
	}
	if limit > 0 {
		q.Set("limit", fmt.Sprintf("%d", limit))
	}
	return rc.indexScan(table, index, q)
}

func (rc *RemoteClient) IndexScanPrefix(table, index string, prefix []byte, limit int) *remoteIndexIter {
	q := url.Values{}
	q.Set("prefix", string(prefix))
	if limit > 0 {
		q.Set("limit", fmt.Sprintf("%d", limit))
	}
	return rc.indexScan(table, index, q)
}

func (it *remoteIndexIter) advance() {
	if !it.scanner.Scan() {
		it.valid = false
		return
	}
	var row struct {
		IndexedValue string `json:"indexed_value"`
		PrimaryKey   string `json:"primary_key"`
		Record       string `json:"record"` // base64
	}
	if err := json.Unmarshal(it.scanner.Bytes(), &row); err != nil {
		it.valid = false
		return
	}
	it.idxVal = []byte(row.IndexedValue)
	it.pk = []byte(row.PrimaryKey)
	it.record, _ = base64.StdEncoding.DecodeString(row.Record)
	it.valid = true
}

func (it *remoteIndexIter) Valid() bool                    { return it.valid }
func (it *remoteIndexIter) Next()                          { it.advance() }
func (it *remoteIndexIter) Value() []byte                  { return it.idxVal }
func (it *remoteIndexIter) PrimaryKey() []byte             { return it.pk }
func (it *remoteIndexIter) GetRecord() ([]byte, error)     { return it.record, nil }
func (it *remoteIndexIter) Close()                         { it.resp.Body.Close() }

// ---- batch ------------------------------------------------------------------

// ApplyBatch POSTs a batch to the server and returns whether it committed.
func (rc *RemoteClient) ApplyBatch(bf *BatchFile) bool {
	var r struct {
		Committed bool   `json:"committed"`
		Reason    string `json:"reason,omitempty"`
	}
	rc.doJSON("POST", "/batch", bf, &r)
	if !r.Committed {
		fmt.Fprintf(os.Stderr, "flexctl: batch rejected: %s\n", r.Reason)
	}
	return r.Committed
}

// ---- schema -----------------------------------------------------------------

func (rc *RemoteClient) SchemaDump() Schema {
	var sc Schema
	rc.doJSON("GET", "/schema", nil, &sc)
	return sc
}

func (rc *RemoteClient) SchemaLoad(sc Schema) {
	rc.doJSON("POST", "/schema", sc, nil)
}

// ---- data dump / load -------------------------------------------------------

// DumpData fetches all data from the server and writes it in flexctl's dump
// format to w.
//
// The server sends NDJSON with base64-encoded values; we reconstruct the
// flexctl JSON structure (Dump) and write it as indented JSON.
func (rc *RemoteClient) DumpData(w io.Writer) {
	resp := rc.doStream("/dump")
	defer resp.Body.Close()

	var dump Dump
	dump.Version = 1
	var curTable *DumpTable

	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 256*1024), 256*1024)
	for scanner.Scan() {
		var line struct {
			Type  string `json:"type"`
			Name  string `json:"name,omitempty"`
			Key   string `json:"key,omitempty"`
			Value string `json:"value,omitempty"` // base64
		}
		if err := json.Unmarshal(scanner.Bytes(), &line); err != nil {
			continue
		}
		switch line.Type {
		case "table":
			dump.Tables = append(dump.Tables, DumpTable{Name: line.Name})
			curTable = &dump.Tables[len(dump.Tables)-1]
		case "entry":
			if curTable == nil {
				continue
			}
			val, _ := base64.StdEncoding.DecodeString(line.Value)
			curTable.Entries = append(curTable.Entries, DumpEntry{
				Key:   line.Key,
				Value: string(val),
			})
		}
	}
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	enc.Encode(dump) //nolint:errcheck
}

// LoadData reads flexctl's JSON dump format from r and uploads it to the server.
func (rc *RemoteClient) LoadData(r io.Reader) {
	data, err := io.ReadAll(r)
	if err != nil {
		die("read dump: %v", err)
	}
	var dump Dump
	if err := json.Unmarshal(data, &dump); err != nil {
		die("parse dump: %v", err)
	}

	// Convert to NDJSON and POST to /v1/dump.
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	for _, tbl := range dump.Tables {
		enc.Encode(map[string]string{"type": "table", "name": tbl.Name}) //nolint:errcheck
		for _, entry := range tbl.Entries {
			enc.Encode(map[string]string{ //nolint:errcheck
				"type":  "entry",
				"key":   entry.Key,
				"value": base64.StdEncoding.EncodeToString([]byte(entry.Value)),
			})
		}
	}

	req := rc.newReq("POST", "/dump", &buf, "application/x-ndjson")
	resp, err := rc.hc.Do(req)
	if err != nil {
		die("load data: %v", err)
	}
	defer resp.Body.Close()
	var result struct {
		Imported int      `json:"imported"`
		Errors   []string `json:"errors,omitempty"`
	}
	json.NewDecoder(resp.Body).Decode(&result) //nolint:errcheck
	info("imported %d entries", result.Imported)
	for _, e := range result.Errors {
		fmt.Fprintf(os.Stderr, "flexctl: load error: %s\n", e)
	}
}

// ---- stats ------------------------------------------------------------------

type remoteDBStats struct {
	Seq             uint64 `json:"seq"`
	TableCount      uint32 `json:"table_count"`
	ActiveMTBytes   int64  `json:"active_mt_bytes"`
	InactiveMTBytes int64  `json:"inactive_mt_bytes"`
	WAL0Bytes       uint64 `json:"wal0_bytes"`
	WAL1Bytes       uint64 `json:"wal1_bytes"`
	RegistryBytes   uint64 `json:"registry_bytes"`
	TablesDirBytes  uint64 `json:"tables_dir_bytes"`
}

type remoteTableStats struct {
	FileBytes   uint64              `json:"file_bytes"`
	AnchorCount int                 `json:"anchor_count"`
	CacheUsed   uint64              `json:"cache_used"`
	CacheCap    uint64              `json:"cache_cap"`
	Indexes     []remoteIndexStats  `json:"indexes"`
}

type remoteIndexStats struct {
	Name        string `json:"name"`
	FileBytes   uint64 `json:"file_bytes"`
	AnchorCount int    `json:"anchor_count"`
}

func (rc *RemoteClient) DBStats() remoteDBStats {
	var r remoteDBStats
	rc.doJSON("GET", "/stats", nil, &r)
	return r
}

func (rc *RemoteClient) TableStats(table string) remoteTableStats {
	var r remoteTableStats
	rc.doJSON("GET", "/tables/"+url.PathEscape(table)+"/stats", nil, &r)
	return r
}
