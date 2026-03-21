package flexdb

import (
	"bytes"
	"hash/crc32"
)

var crc32cTable = crc32.MakeTable(crc32.Castagnoli)

// MaxKVSize is the maximum combined key+value size for inline storage (4 KiB).
// Entries that exceed this limit are automatically stored as blobs: the value
// is written to a per-table append-only blob file and a 16-byte sentinel is
// kept in the main KV store. Keys alone are always ≤ MaxKVSize.
const MaxKVSize = 4 << 10

// KV represents a key-value pair. A zero-length Value is a tombstone (deletion).
type KV struct {
	Key   []byte
	Value []byte
}

// IsTombstone reports whether this KV represents a deletion. A zero-length
// Value is the tombstone sentinel; the key is retained so iterators can
// suppress it during merges.
func (kv *KV) IsTombstone() bool {
	return len(kv.Value) == 0
}

// hash32 returns a 32-bit CRC32C hash of the key.
// Uses hardware acceleration (SSE4.2 / ARMv8 CRC) when available,
// matching the C implementation which also uses crc32c.
func hash32(key []byte) uint32 {
	return crc32.Checksum(key, crc32cTable)
}

// fp16 folds a 32-bit hash into a 16-bit fingerprint.
func fp16(h uint32) uint16 {
	return uint16(h) ^ uint16(h>>16)
}

// vi128Size returns the number of bytes needed to encode v in varint-128 format.
func vi128Size(v uint32) int {
	switch {
	case v < (1 << 7):
		return 1
	case v < (1 << 14):
		return 2
	case v < (1 << 21):
		return 3
	case v < (1 << 28):
		return 4
	default:
		return 5
	}
}

// putVI128 encodes v into buf and returns the number of bytes written.
func putVI128(buf []byte, v uint32) int {
	i := 0
	for v >= 0x80 {
		buf[i] = byte(v) | 0x80
		v >>= 7
		i++
	}
	buf[i] = byte(v)
	return i + 1
}

// getVI128 decodes a varint-128 from buf, returning the value and bytes consumed.
func getVI128(buf []byte) (uint32, int) {
	var v uint32
	var shift uint
	for i, b := range buf {
		v |= uint32(b&0x7F) << shift
		if b < 0x80 {
			return v, i + 1
		}
		shift += 7
	}
	return 0, 0 // truncated
}

// kv128EncodedSize returns the exact encoded byte count for the given key/value lengths.
func kv128EncodedSize(klen, vlen int) int {
	return vi128Size(uint32(klen)) + vi128Size(uint32(vlen)) + klen + vlen
}

// encodeKV128 encodes kv into buf and returns the number of bytes written.
func encodeKV128(buf []byte, kv *KV) int {
	return encodeKV128Data(buf, kv.Key, kv.Value)
}

// encodeKV128Data encodes key and value into buf and returns the number of bytes written.
func encodeKV128Data(buf []byte, key, value []byte) int {
	n := putVI128(buf, uint32(len(key)))
	n += putVI128(buf[n:], uint32(len(value)))
	copy(buf[n:], key)
	n += len(key)
	copy(buf[n:], value)
	n += len(value)
	return n
}

// decodeKV128 decodes one KV from buf and returns the KV and bytes consumed.
// Key and value are backed by a single allocation to reduce GC pressure.
func decodeKV128(buf []byte) (*KV, int) {
	klen, n1 := getVI128(buf)
	if n1 == 0 {
		return nil, 0
	}
	vlen, n2 := getVI128(buf[n1:])
	if n2 == 0 {
		return nil, 0
	}
	n := n1 + n2
	total := n + int(klen) + int(vlen)
	if total > len(buf) {
		return nil, 0
	}
	combined := make([]byte, int(klen)+int(vlen))
	copy(combined[:klen], buf[n:])
	copy(combined[klen:], buf[n+int(klen):])
	return &KV{Key: combined[:klen:klen], Value: combined[klen:]}, total
}

// dupKV copies kv into a single allocation, reducing GC pressure.
// Key and value share one []byte backing buffer; the *KV struct is a second.
// This replaces the 3-alloc pattern &KV{Key: dupKey(k), Value: dupKey(v)}.
func dupKV(kv *KV) *KV {
	buf := make([]byte, len(kv.Key)+len(kv.Value))
	klen := copy(buf, kv.Key)
	copy(buf[klen:], kv.Value)
	return &KV{Key: buf[:klen:klen], Value: buf[klen:]}
}

// compareKeys compares two byte-slice keys lexicographically.
func compareKeys(a, b []byte) int {
	return bytes.Compare(a, b)
}
