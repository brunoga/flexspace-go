package flexdb

import (
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// Memtable constants matching the C implementation.
const (
	memtableFlushBatch = 1024
	memtableLogBufCap  = 4 << 20 // 4 MB
)

// memtable is one of the two double-buffered ordered in-memory tables.
type memtable struct {
	db     *DB
	m      *skipList // ordered map; protected by the caller's MT lock
	logBuf []byte
	logOff int
	logFd  *os.File

	mu     sync.Mutex // protects logBuf
	size   atomic.Int64
	hidden atomic.Bool // true = immutable (being flushed / just cleared)

	// readers tracks concurrent readers of this memtable.
	// Used to safely clear the memtable after flush.
	readers atomic.Int32
}

func newMemtable(db *DB, logPath string) (*memtable, error) {
	f, err := os.OpenFile(logPath, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {

		return nil, err
	}
	mt := &memtable{
		db:     db,
		m:      newSkipList(),
		logBuf: make([]byte, memtableLogBufCap),
		logFd:  f,
	}
	return mt, nil
}

func (mt *memtable) close() error {
	flushErr := mt.flushLog()
	closeErr := mt.logFd.Close()
	if flushErr != nil {
		return flushErr
	}
	return closeErr
}

// put upserts a KV into the memtable and appends it to the WAL.
func (mt *memtable) put(key, value []byte, seq uint64) error {
	if err := mt.logAppend(key, value, seq); err != nil {
		return err
	}
	sz := int64(kv128EncodedSize(len(key), len(value)))
	mt.m.Set(key, value)
	mt.size.Add(sz)
	return nil
}

// get returns the value for key, or (nil, false) if not present.
func (mt *memtable) get(key []byte) ([]byte, bool) {
	mt.readers.Add(1)
	defer mt.readers.Add(-1)
	return mt.m.Get(key)
}

// probe returns: 0=not found, 1=tombstone, 2=valid value
func (mt *memtable) probe(key []byte) int {
	mt.readers.Add(1)
	defer mt.readers.Add(-1)
	v, ok := mt.m.Get(key)
	if !ok {
		return 0
	}
	if len(v) == 0 {
		return 1
	}
	return 2
}

// walBatchOp is a single op within a batch WAL record.
type walBatchOp struct {
	pkey  []byte // tableID-prefixed key
	value []byte // nil = tombstone
}

// logAppend encodes kv into the WAL buffer; flushes buffer when full.
// Format: [sz:8][seq:8][KV(sz bytes)]
func (mt *memtable) logAppend(key, value []byte, seq uint64) error {
	sz := kv128EncodedSize(len(key), len(value))
	needed := 8 + 8 + sz // sz + seq + kv
	mt.mu.Lock()
	if mt.logOff+needed > len(mt.logBuf) {
		if err := mt.flushLogLocked(); err != nil {
			mt.mu.Unlock()
			return err
		}
	}
	binary.LittleEndian.PutUint64(mt.logBuf[mt.logOff:], uint64(sz))
	binary.LittleEndian.PutUint64(mt.logBuf[mt.logOff+8:], seq)
	n := encodeKV128Data(mt.logBuf[mt.logOff+16:], key, value)
	mt.logOff += 8 + 8 + n
	mt.mu.Unlock()
	return nil
}

// logAppendBatch writes all ops as a single atomic WAL record.
//
// Format (size=0 sentinel distinguishes it from single-op records):
//
//	[0:8]                 size=0 sentinel
//	[payload_size:8]      byte count of what follows
//	[seq:8]               sequence number
//	[op_count:4]
//	for each op:
//	  [kv_size:8]         VI128-encoded size
//	  [VI128 KV...]       tableID-prefixed key + value
//
// Callers must hold all rwMT write locks to guarantee atomicity.
func (mt *memtable) logAppendBatch(ops []walBatchOp, seq uint64) error {
	// Compute payload size: seq(8) + op_count(4) + for each op: kv_size(8) + kv_bytes
	payloadSize := 8 + 4
	for _, op := range ops {
		payloadSize += 8 + kv128EncodedSize(len(op.pkey), len(op.value))
	}
	needed := 8 + 8 + payloadSize // sentinel + payload_size + payload

	writeBatch := func(buf []byte) {
		off := 0
		binary.LittleEndian.PutUint64(buf[off:], 0) // sentinel
		off += 8
		binary.LittleEndian.PutUint64(buf[off:], uint64(payloadSize))
		off += 8
		binary.LittleEndian.PutUint64(buf[off:], seq)
		off += 8
		binary.LittleEndian.PutUint32(buf[off:], uint32(len(ops)))
		off += 4
		for _, op := range ops {
			kvSize := kv128EncodedSize(len(op.pkey), len(op.value))
			binary.LittleEndian.PutUint64(buf[off:], uint64(kvSize))
			off += 8
			n := encodeKV128Data(buf[off:], op.pkey, op.value)
			off += n
		}
	}

	mt.mu.Lock()
	if mt.logOff+needed > len(mt.logBuf) {
		if err := mt.flushLogLocked(); err != nil {
			mt.mu.Unlock()
			return err
		}
	}
	if needed > len(mt.logBuf) {
		// Too large for the buffer; write directly to file.
		buf := make([]byte, needed)
		writeBatch(buf)
		_, err := mt.logFd.Write(buf)
		mt.mu.Unlock()
		if err != nil {
			return fmt.Errorf("flexdb: WAL write failed: %w", err)
		}
		return nil
	}
	writeBatch(mt.logBuf[mt.logOff : mt.logOff+needed])
	mt.logOff += needed
	mt.mu.Unlock()
	return nil
}

// flushLog writes the in-memory log buffer to disk.
func (mt *memtable) flushLog() error {
	mt.mu.Lock()
	err := mt.flushLogLocked()
	mt.mu.Unlock()
	return err
}

func (mt *memtable) flushLogLocked() error {
	if mt.logOff == 0 {
		return nil
	}
	if _, err := mt.logFd.Write(mt.logBuf[:mt.logOff]); err != nil {
		return fmt.Errorf("flexdb: WAL write failed: %w", err)
	}
	mt.logOff = 0
	return nil
}

// truncateLog resets the log file and writes a fresh header:
// [timestamp:8][seq:8]
// The seq is persisted so that DB.seqNum can be restored after a reopen
// even when the WAL body has been truncated (i.e. all data was flushed).
func (mt *memtable) truncateLog() error {
	if err := mt.flushLog(); err != nil {
		return err
	}
	if err := mt.logFd.Truncate(0); err != nil {
		return err
	}
	if _, err := mt.logFd.Seek(0, io.SeekStart); err != nil {
		return err
	}
	var hdr [16]byte
	binary.LittleEndian.PutUint64(hdr[:8], uint64(time.Now().Unix()))
	binary.LittleEndian.PutUint64(hdr[8:], mt.db.seqNum.Load())
	if _, err := mt.logFd.Write(hdr[:]); err != nil {
		return err
	}
	return nil
}

// redoLog replays a WAL file into this memtable (skipping the 8-byte header).
// Returns the maximum sequence number seen across all replayed records.
func (mt *memtable) redoLog(path string) (maxSeq uint64, err error) {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return 0, err
	}
	if info.Size() <= 8 {
		return 0, nil
	}

	buf := make([]byte, info.Size())
	if _, err := io.ReadFull(f, buf); err != nil {
		return 0, err
	}
	// Header: [timestamp:8][seq:8] (seq field added for reopen recovery).
	// Older logs have only an 8-byte timestamp; treat their seq as 0.
	off := 8
	if len(buf) >= 16 {
		if seq := binary.LittleEndian.Uint64(buf[8:]); seq > maxSeq {
			maxSeq = seq
		}
		off = 16
	}
	for off+8 <= len(buf) {
		sz := int(binary.LittleEndian.Uint64(buf[off:]))
		off += 8

		if sz == 0 {
			// Batch record: [payload_size:8][seq:8][op_count:4][ops...]
			// Atomicity: if truncated, skip the entire batch.
			if off+8 > len(buf) {
				break
			}
			payloadSize := int(binary.LittleEndian.Uint64(buf[off:]))
			off += 8
			if off+payloadSize > len(buf) {
				break // batch truncated — skip (atomicity guarantee)
			}
			batchEnd := off + payloadSize
			if off+8+4 > batchEnd {
				off = batchEnd
				continue
			}
			seq := binary.LittleEndian.Uint64(buf[off:])
			off += 8
			if seq > maxSeq {
				maxSeq = seq
			}
			opCount := int(binary.LittleEndian.Uint32(buf[off:]))
			off += 4
			for op := 0; op < opCount && off+8 <= batchEnd; op++ {
				kvSize := int(binary.LittleEndian.Uint64(buf[off:]))
				off += 8
				if off+kvSize > batchEnd {
					break
				}
				kv, n := decodeKV128(buf[off:])
				if kv == nil || n != kvSize {
					break
				}
				mt.m.Set(kv.Key, kv.Value)
				off += kvSize
			}
			off = batchEnd
		} else {
			// Single-op record: [sz:8 already consumed][seq:8][KV(sz bytes)]
			if off+8 > len(buf) {
				break
			}
			seq := binary.LittleEndian.Uint64(buf[off:])
			off += 8
			if seq > maxSeq {
				maxSeq = seq
			}
			if off+sz > len(buf) {
				slog.Error("flexdb: WAL entry truncated", "offset", off)
				break
			}
			kv, n := decodeKV128(buf[off:])
			if kv == nil || n != sz {
				slog.Error("flexdb: WAL decode failed", "offset", off)
				break
			}

			mt.m.Set(kv.Key, kv.Value)
			off += sz
		}
	}
	return maxSeq, nil
}

// isFull returns true when the memtable has grown past the capacity threshold.
func (mt *memtable) isFull() bool {
	return mt.size.Load() >= mt.db.memtableCap
}

// reset replaces the memtable map and size counter (called after flush completes).
// Spins until all readers have finished using the old map.
func (mt *memtable) reset() {
	for mt.readers.Load() > 0 {
		time.Sleep(1 * time.Microsecond)
	}
	mt.m = newSkipList()
	mt.size.Store(0)
}

// ---- flush worker ----

type flushWorker struct {
	db   *DB
	quit chan struct{}
	done chan struct{}

	stopOnce sync.Once

	immediateWork atomic.Bool
}

func (w *flushWorker) start() {
	go w.run()
}

func (w *flushWorker) stop() {
	w.stopOnce.Do(func() {
		close(w.quit)
	})
	<-w.done
}

func (w *flushWorker) triggerImmediate() {
	w.immediateWork.Store(true)
}

func (w *flushWorker) run() {
	defer close(w.done)
	defer func() {
		if r := recover(); r != nil {
			slog.Error("flexdb: flush worker panicked", "error", r)
		}
	}()

	msTimer := time.NewTicker(time.Millisecond)
	defer msTimer.Stop()

	lastFlush := time.Now()

	for {
		select {
		case <-w.quit:
			// Flush both memtables on shutdown.
			w.db.flushActiveMT()
			w.db.flushActiveMT()
			return
		case <-msTimer.C:
			active := w.db.activeMT()
			shouldFlush := w.immediateWork.Swap(false) ||
				active.isFull() ||
				time.Since(lastFlush) >= w.db.flushInter

			if shouldFlush && !active.hidden.Load() {
				w.db.swapAndFlush()
				lastFlush = time.Now()
			}
		}
	}
}
