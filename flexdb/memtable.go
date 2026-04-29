package flexdb

import (
	"encoding/binary"
	"io"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

const (
	// memtableLogBufCap is the write-ahead log buffer size (4 MiB).
	memtableLogBufCap = 4 << 20

	// batchSentinel is used in the WAL to identify a multi-operation record.
	batchSentinel = 0
)

// memtable provides a high-performance in-memory write buffer for the database.
// It uses a skip-list for ordered key storage and is backed by a write-ahead log (WAL).
type memtable struct {
	db   *DB
	m    *skipList
	size atomic.Uint64

	// Readers counter for the current skip-list.
	readers atomic.Int64

	// Hidden from Get() while being replayed or flushed.
	hidden atomic.Bool

	// Write-ahead log state.
	mu     sync.Mutex
	logFd  *os.File
	logOff int
	logBuf []byte
}

func newMemtable(db *DB, logPath string) (*memtable, error) {
	f, err := os.OpenFile(logPath, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return nil, err
	}
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		f.Close()
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
	mt.mu.Lock()
	defer mt.mu.Unlock()
	if err := mt.flushLogLocked(); err != nil {
		mt.logFd.Close()
		return err
	}
	return mt.logFd.Close()
}

// put inserts or updates a key in the memtable and logs it to the WAL.
func (mt *memtable) put(pkey, value []byte, seq uint64) error {
	if err := mt.logAppend(pkey, value, seq); err != nil {
		return err
	}
	mt.m.Set(pkey, value)
	mt.size.Add(uint64(kv128EncodedSize(len(pkey), len(value))))
	return nil
}

// get retrieves a key from the memtable. Returns (value, true) if found,
// (nil, true) if key is deleted (tombstone), or (nil, false) if not present.
func (mt *memtable) get(pkey []byte) ([]byte, bool) {
	mt.readers.Add(1)
	defer mt.readers.Add(-1)
	return mt.m.Get(pkey)
}

// probe reports 0: not found, 1: found value, 2: found tombstone.
func (mt *memtable) probe(pkey []byte) int {
	mt.readers.Add(1)
	defer mt.readers.Add(-1)
	val, ok := mt.m.Get(pkey)
	if !ok {
		return 0
	}
	if val == nil {
		return 2
	}
	return 1
}

func (mt *memtable) logAppend(pkey, value []byte, seq uint64) error {
	kvSz := uint32(kv128EncodedSize(len(pkey), len(value)))
	needed := 4 + 8 + int(kvSz) // sz:4 + seq:8 + KV

	mt.mu.Lock()
	if mt.logOff+needed > len(mt.logBuf) {
		if err := mt.flushLogLocked(); err != nil {
			mt.mu.Unlock()
			return err
		}
	}

	binary.LittleEndian.PutUint32(mt.logBuf[mt.logOff:], kvSz)
	binary.LittleEndian.PutUint64(mt.logBuf[mt.logOff+4:], seq)
	encodeKV128Data(mt.logBuf[mt.logOff+12:], pkey, value)
	mt.logOff += needed
	mt.mu.Unlock()
	return nil
}

type walBatchOp struct {
	pkey  []byte
	value []byte
}

// logAppendBatch writes all ops as a single atomic WAL record.
//
// Format (size-prefixed):
//
//	  [sentinel:4]       (0 identifies a batch)
//	  [payload_size:8]
//	  [seq:8]            sequence number of the batch
//	  [op_count:4]       number of ops in the batch
//	  for each op:
//		  [kv_size:4]         size of the encoded KV
//		  [VI128 KV...]       tableID-prefixed key + value
//
// Callers must hold all rwMT write locks to guarantee atomicity.
func (mt *memtable) logAppendBatch(ops []walBatchOp, seq uint64) error {
	payloadSize := 8 + 4
	for _, op := range ops {
		kvSz := uint32(kv128EncodedSize(len(op.pkey), len(op.value)))
		payloadSize += 4 + int(kvSz)
	}
	needed := 4 + 8 + payloadSize // sentinel + payload_size + payload

	mt.mu.Lock()
	if mt.logOff+needed > len(mt.logBuf) {
		if err := mt.flushLogLocked(); err != nil {
			mt.mu.Unlock()
			return err
		}
	}

	buf := mt.logBuf[mt.logOff:]
	binary.LittleEndian.PutUint32(buf[0:4], batchSentinel)
	binary.LittleEndian.PutUint64(buf[4:12], uint64(payloadSize))
	binary.LittleEndian.PutUint64(buf[12:20], seq)
	binary.LittleEndian.PutUint32(buf[20:24], uint32(len(ops)))

	off := 24
	for _, op := range ops {
		kvSz := uint32(kv128EncodedSize(len(op.pkey), len(op.value)))
		binary.LittleEndian.PutUint32(buf[off:], kvSz)
		off += 4
		encodeKV128Data(buf[off:], op.pkey, op.value)
		off += int(kvSz)
	}

	mt.logOff += needed
	mt.mu.Unlock()
	return nil
}

func (mt *memtable) flushLog() error {
	mt.mu.Lock()
	defer mt.mu.Unlock()
	return mt.flushLogLocked()
}

func (mt *memtable) flushLogLocked() error {
	if mt.logOff == 0 {
		return nil
	}
	if _, err := mt.logFd.Write(mt.logBuf[:mt.logOff]); err != nil {
		return err
	}
	mt.logOff = 0
	// Use fdatasync for performance.
	if _, _, errno := syscall.Syscall(syscall.SYS_FDATASYNC, mt.logFd.Fd(), 0, 0); errno != 0 {
		return errno
	}
	return nil
}

func (mt *memtable) truncateLog() error {
	mt.mu.Lock()
	defer mt.mu.Unlock()
	if err := mt.logFd.Truncate(0); err != nil {
		return err
	}
	if _, err := mt.logFd.Seek(0, io.SeekStart); err != nil {
		return err
	}
	mt.logOff = 0
	// Header: [timestamp:8][seq:8]
	var h [16]byte
	binary.LittleEndian.PutUint64(h[0:8], uint64(time.Now().Unix()))
	binary.LittleEndian.PutUint64(h[8:16], mt.db.seqNum.Load())
	if _, err := mt.logFd.Write(h[:]); err != nil {
		return err
	}
	return mt.logFd.Sync()
}

func (mt *memtable) redoLog(path string) (uint64, error) {
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

	// Header: [timestamp:8][seq:8]; older logs may have only 8 bytes (timestamp only).
	off := 8
	var maxSeq uint64
	if len(buf) >= 16 {
		maxSeq = binary.LittleEndian.Uint64(buf[8:16])
		off = 16
	}

	for off+4 <= len(buf) {
		sz := int(binary.LittleEndian.Uint32(buf[off:]))
		off += 4

		if sz == 0 {
			// Batch record: [payload_size:8][seq:8][op_count:4][ops...]
			if off+8 > len(buf) {
				break
			}
			payloadSize := int(binary.LittleEndian.Uint64(buf[off:]))
			off += 8
			if off+payloadSize > len(buf) {
				break
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

			var batchOps []walBatchOp
			corrupted := false
			for op := 0; op < opCount; op++ {
				if off+4 > batchEnd {
					corrupted = true
					break
				}
				kvSize := int(binary.LittleEndian.Uint32(buf[off:]))
				off += 4
				if off+kvSize > batchEnd {
					corrupted = true
					break
				}
				kv, n := decodeKV128(buf[off : off+kvSize])
				if kv == nil || n != kvSize {
					corrupted = true
					break
				}
				batchOps = append(batchOps, walBatchOp{pkey: kv.Key, value: kv.Value})
				off += kvSize
			}
			if !corrupted && len(batchOps) == opCount {
				for _, op := range batchOps {
					mt.m.Set(op.pkey, op.value)
					mt.size.Add(uint64(kv128EncodedSize(len(op.pkey), len(op.value))))
				}
			}
			off = batchEnd
		} else {
			// Single-op record: [sz:4 already consumed][seq:8][KV(sz bytes)]
			if off+8 > len(buf) {
				break
			}
			seq := binary.LittleEndian.Uint64(buf[off:])
			off += 8
			if seq > maxSeq {
				maxSeq = seq
			}
			if off+sz > len(buf) {
				break
			}
			kv, n := decodeKV128(buf[off : off+sz])
			if kv == nil || n != sz {
				break
			}
			mt.m.Set(kv.Key, kv.Value)
			mt.size.Add(uint64(kv128EncodedSize(len(kv.Key), len(kv.Value))))
			off += sz
		}
	}
	return maxSeq, nil
}

// isFull returns true when the memtable has grown past the capacity threshold.
func (mt *memtable) isFull() bool {
	return mt.size.Load() >= uint64(mt.db.memtableCap)
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
			shouldFlush := active.isFull() ||
				time.Since(lastFlush) >= w.db.flushInter

			if shouldFlush && !active.hidden.Load() {
				w.db.swapAndFlush()
				lastFlush = time.Now()
			}
		}
	}
}
