package main

import (
	"fmt"
	"strings"

	"github.com/brunoga/flexspace-go/flexkv"
)

// SchemaIndex describes a single secondary index in declarative form.
// This is intentionally identical to the flexctl type so that schema files
// produced by flexctl can be loaded by flexsrv without conversion.
//
// Built-in indexer types:
//
//	"exact"  — the entire value
//	"prefix" — first Length bytes of the value
//	"suffix" — last Length bytes of the value
//	"field"  — field number Field (0-based) split by Delim (default ",")
type SchemaIndex struct {
	Name   string `json:"name"`
	Type   string `json:"type"`
	Length int    `json:"length,omitempty"`
	Delim  string `json:"delim,omitempty"`
	Field  int    `json:"field,omitempty"`
}

// SchemaTable describes one user table and its indexes.
type SchemaTable struct {
	Name    string        `json:"name"`
	Indexes []SchemaIndex `json:"indexes,omitempty"`
}

// Schema is the root of a schema document, compatible with flexctl's format.
type Schema struct {
	Version int           `json:"version"`
	Tables  []SchemaTable `json:"tables"`
}

// makeIndexer returns a flexkv.Indexer for the given SchemaIndex spec.
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
		n := idx.Field
		return func(_, value []byte) [][]byte {
			parts := strings.Split(string(value), delim)
			if n >= len(parts) {
				return nil
			}
			f := strings.TrimSpace(parts[n])
			if f == "" {
				return nil
			}
			return [][]byte{[]byte(f)}
		}, nil

	default:
		return nil, fmt.Errorf("unknown indexer type %q (want: exact, prefix, suffix, field)", idx.Type)
	}
}
