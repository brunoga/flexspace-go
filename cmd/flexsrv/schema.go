package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/brunoga/flexspace-go/flexkv"
)

const schemaFile = "flexsrv_schema.json"

// SchemaStore manages the persisted index schema and handles re-registration
// of indexer functions across restarts. It is safe for concurrent use.
type SchemaStore struct {
	mu     sync.RWMutex
	db     *flexkv.DB
	path   string // absolute path to flexsrv_schema.json
	schema Schema
}

// loadAndRegisterSchema opens (or creates) the schema file and re-registers
// all indexers on the already-open database. It returns the SchemaStore and a
// map of the table handles it opened, which the Server uses as its initial
// cache so indexers survive across requests.
func loadAndRegisterSchema(db *flexkv.DB, dbPath string) (*SchemaStore, map[string]*flexkv.Table, error) {
	ss := &SchemaStore{
		db:   db,
		path: filepath.Join(dbPath, schemaFile),
	}

	data, err := os.ReadFile(ss.path)
	if err != nil && !os.IsNotExist(err) {
		return nil, nil, fmt.Errorf("read %s: %w", ss.path, err)
	}
	if len(data) > 0 {
		if err := json.Unmarshal(data, &ss.schema); err != nil {
			return nil, nil, fmt.Errorf("parse %s: %w", ss.path, err)
		}
	}

	// Re-register every known indexer. We use RegisterIndex (not CreateIndex)
	// because the index data is already on disk; we just need to reattach the
	// in-memory function so that future writes keep the index current.
	tables := make(map[string]*flexkv.Table)
	for _, tbl := range ss.schema.Tables {
		t, err := db.Table(tbl.Name)
		if err != nil {
			return nil, nil, fmt.Errorf("open table %q: %w", tbl.Name, err)
		}
		tables[tbl.Name] = t
		for _, idx := range tbl.Indexes {
			fn, err := makeIndexer(idx)
			if err != nil {
				return nil, nil, fmt.Errorf("index %s.%s: %w", tbl.Name, idx.Name, err)
			}
			if err := t.RegisterIndex(idx.Name, fn); err != nil {
				return nil, nil, fmt.Errorf("register index %s.%s: %w", tbl.Name, idx.Name, err)
			}
		}
	}

	return ss, tables, nil
}

// Get returns a snapshot of the current schema.
func (ss *SchemaStore) Get() Schema {
	ss.mu.RLock()
	defer ss.mu.RUnlock()
	return ss.schema
}

// IndexesFor returns the index specs registered for tableName.
func (ss *SchemaStore) IndexesFor(tableName string) []SchemaIndex {
	ss.mu.RLock()
	defer ss.mu.RUnlock()
	for _, t := range ss.schema.Tables {
		if t.Name == tableName {
			// Return a copy to avoid races.
			out := make([]SchemaIndex, len(t.Indexes))
			copy(out, t.Indexes)
			return out
		}
	}
	return nil
}

// RecordTable ensures tableName appears in the schema and saves to disk.
func (ss *SchemaStore) RecordTable(name string) error {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	for _, t := range ss.schema.Tables {
		if t.Name == name {
			return nil // already recorded
		}
	}
	ss.schema.Tables = append(ss.schema.Tables, SchemaTable{Name: name})
	return ss.save()
}

// RemoveTable removes tableName from the schema and saves to disk.
func (ss *SchemaStore) RemoveTable(name string) error {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	tables := ss.schema.Tables[:0]
	for _, t := range ss.schema.Tables {
		if t.Name != name {
			tables = append(tables, t)
		}
	}
	ss.schema.Tables = tables
	return ss.save()
}

// RecordIndex appends idx to tableName's index list and saves to disk.
// If tableName is not yet in the schema it is added automatically.
func (ss *SchemaStore) RecordIndex(tableName string, idx SchemaIndex) error {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	for i := range ss.schema.Tables {
		if ss.schema.Tables[i].Name == tableName {
			ss.schema.Tables[i].Indexes = append(ss.schema.Tables[i].Indexes, idx)
			return ss.save()
		}
	}
	ss.schema.Tables = append(ss.schema.Tables, SchemaTable{
		Name:    tableName,
		Indexes: []SchemaIndex{idx},
	})
	return ss.save()
}

// RemoveIndex removes the named index from tableName's entry and saves.
func (ss *SchemaStore) RemoveIndex(tableName, indexName string) error {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	for i := range ss.schema.Tables {
		if ss.schema.Tables[i].Name != tableName {
			continue
		}
		idxs := ss.schema.Tables[i].Indexes[:0]
		for _, idx := range ss.schema.Tables[i].Indexes {
			if idx.Name != indexName {
				idxs = append(idxs, idx)
			}
		}
		ss.schema.Tables[i].Indexes = idxs
		return ss.save()
	}
	return nil
}

// Replace atomically replaces the entire schema and saves to disk.
// It does NOT call CreateIndex/RegisterIndex — callers must do that themselves.
func (ss *SchemaStore) Replace(s Schema) error {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	ss.schema = s
	return ss.save()
}

// save writes ss.schema to disk. Caller must hold ss.mu for write.
func (ss *SchemaStore) save() error {
	data, err := json.MarshalIndent(ss.schema, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(ss.path, data, 0o600)
}
