package merger

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
)

// Block-level WAL for the decode-free gorilla path.
//
// Each received ASAPFRG1 frame is appended VERBATIM (no sample decode) as a
// length+crc-prefixed record and fsync'd before the ingest handler ack's the
// agent (HTTP 200), so an accepted batch is durable across a crash. On restart,
// replay reads the frames back, re-buckets them by window via the same
// windowBuffer, and re-flushes any window not yet persisted as a block —
// without ever re-decoding samples. This replaces the per-sample tsdb WAL for
// the gorilla path.
//
// A WAL segment is finalized (the file is renamed from <n>.active to <n>.seg)
// once the windows it covers have all been flushed into blocks, so replay only
// needs to read the still-active segment plus any unfinalized ones. To keep the
// implementation simple and robust we use ONE active segment file and truncate
// it (start a fresh segment) after a successful flush checkpoint.

const (
	walRecordHeaderSize = 8 // uint32 length + uint32 crc
	walActiveName       = "wal.active"
)

var walCRCTable = crc32.MakeTable(crc32.Castagnoli)

// WAL is an append-only log of raw fragment frames.
type WAL struct {
	dir string

	mu  sync.Mutex
	f   *os.File
	seq int
}

// OpenWAL opens (creating if needed) the WAL directory and the active segment
// for appending. Existing finalized/active segments are left in place for
// Replay to read first.
func OpenWAL(dir string) (*WAL, error) {
	if dir == "" {
		return nil, fmt.Errorf("wal: dir is required")
	}
	if err := os.MkdirAll(dir, 0o777); err != nil {
		return nil, fmt.Errorf("wal: mkdir %q: %w", dir, err)
	}
	w := &WAL{dir: dir}
	if err := w.openActive(); err != nil {
		return nil, err
	}
	return w, nil
}

func (w *WAL) openActive() error {
	f, err := os.OpenFile(w.activePath(), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o666)
	if err != nil {
		return fmt.Errorf("wal: open active segment: %w", err)
	}
	w.f = f
	return nil
}

func (w *WAL) activePath() string { return filepath.Join(w.dir, walActiveName) }

// Append writes one frame as a length+crc-prefixed record and fsyncs it. It
// returns only after the record is durable, so the caller can safely ack.
func (w *WAL) Append(frame []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.f == nil {
		if err := w.openActive(); err != nil {
			return err
		}
	}
	var hdr [walRecordHeaderSize]byte
	binary.BigEndian.PutUint32(hdr[0:4], uint32(len(frame)))
	binary.BigEndian.PutUint32(hdr[4:8], crc32.Checksum(frame, walCRCTable))
	if _, err := w.f.Write(hdr[:]); err != nil {
		return fmt.Errorf("wal: write header: %w", err)
	}
	if _, err := w.f.Write(frame); err != nil {
		return fmt.Errorf("wal: write frame: %w", err)
	}
	if err := w.f.Sync(); err != nil {
		return fmt.Errorf("wal: fsync: %w", err)
	}
	return nil
}

// Checkpoint finalizes the current active segment by renaming it to a numbered
// finalized segment and starting a fresh active segment. Call this AFTER the
// windows covered by the just-finalized records have been persisted as blocks.
// Finalized segments are then garbage so we delete them immediately: their data
// is now durable in the on-disk blocks.
//
// (A more granular per-window truncation is possible but a checkpoint-then-prune
// after each successful flush cycle is simpler and bounds WAL size to roughly
// one flush interval of in-flight fragments.)
func (w *WAL) Checkpoint() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.f == nil {
		return nil
	}
	if err := w.f.Close(); err != nil {
		return fmt.Errorf("wal: close active for checkpoint: %w", err)
	}
	w.f = nil
	// The active segment's records are now durably represented as blocks, so
	// remove it outright and start fresh.
	if err := os.Remove(w.activePath()); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("wal: remove checkpointed segment: %w", err)
	}
	return w.openActive()
}

// Close flushes and closes the active segment.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.f == nil {
		return nil
	}
	err := w.f.Close()
	w.f = nil
	return err
}

// ReplayWAL reads every WAL segment in the directory (finalized + active) in
// order and calls fn for each intact frame. A torn final record (truncated
// header/body, or a CRC mismatch on the last record) is tolerated: replay stops
// at the first bad record in a segment, which is the expected shape of a crash
// mid-append. The active segment is read last.
func ReplayWAL(dir string, fn func(frame []byte) error) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("wal: read dir %q: %w", dir, err)
	}
	type seg struct {
		path string
		n    int
		act  bool
	}
	var segs []seg
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		switch {
		case name == walActiveName:
			segs = append(segs, seg{path: filepath.Join(dir, name), act: true})
		default:
			// Finalized segments named <n>.seg (kept for forward-compat; current
			// Checkpoint deletes them, but Replay still honors any present).
			base := name
			if ext := filepath.Ext(name); ext == ".seg" {
				if n, perr := strconv.Atoi(base[:len(base)-len(ext)]); perr == nil {
					segs = append(segs, seg{path: filepath.Join(dir, name), n: n})
				}
			}
		}
	}
	// Finalized segments first (by number), then the active segment.
	sort.Slice(segs, func(i, j int) bool {
		if segs[i].act != segs[j].act {
			return !segs[i].act // non-active (finalized) before active
		}
		return segs[i].n < segs[j].n
	})

	for _, s := range segs {
		if err := replaySegment(s.path, fn); err != nil {
			return err
		}
	}
	return nil
}

func replaySegment(path string, fn func(frame []byte) error) error {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("wal: open segment %q: %w", path, err)
	}
	defer f.Close()

	var hdr [walRecordHeaderSize]byte
	for {
		if _, err := io.ReadFull(f, hdr[:]); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return nil // clean end or torn header → stop
			}
			return fmt.Errorf("wal: read header in %q: %w", path, err)
		}
		n := binary.BigEndian.Uint32(hdr[0:4])
		want := binary.BigEndian.Uint32(hdr[4:8])
		frame := make([]byte, n)
		if _, err := io.ReadFull(f, frame); err != nil {
			// Torn body on the last record after a crash: stop cleanly.
			return nil
		}
		if crc32.Checksum(frame, walCRCTable) != want {
			// Corrupt/torn record: stop replaying this segment.
			return nil
		}
		if err := fn(frame); err != nil {
			return err
		}
	}
}
