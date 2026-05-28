package merger

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	gorilla "github.com/ProjectASAP/asap-gorilla-go"
)

const (
	// defaultWindowMs is the buffering/close window for the decode-free path. A
	// closed window becomes one directly-built (pending) block, queryable within
	// ~window+grace+flush-tick — NOT the 2h block base, because a 2h window would
	// hide the freshest ~2h of data from the StoreAPI. The background compactor
	// later fuses adjacent windows up to the (decoupled) 2h compaction span and
	// re-chunks for ratio before they ship. 2m keeps recent data visible fast.
	defaultWindowMs = int64(2 * 60 * 1000)
	// defaultReorderGraceMs is how long after a window's end we keep accepting
	// late/out-of-order fragments for it before flushing.
	defaultReorderGraceMs = int64(60 * 1000)
)

// Manager owns the decode-free gorilla ingest pipeline: it buffers raw chunks
// per window (windowBuffer), durably logs each received frame (WAL), flushes
// closed windows into directly-built blocks (buildBlock -> BlockStore), and lets
// the BlockStore serve them to the StoreAPI. It replaces the per-sample tsdb
// Appender path.
type Manager struct {
	buf      *windowBuffer
	wal      *WAL
	store    *BlockStore
	windowMs int64
	graceMs  int64
	logger   *slog.Logger
}

// ManagerOptions configures the Manager.
type ManagerOptions struct {
	// PendingDir is where the per-window L1 blocks are directly built. Served by
	// the BlockStore but NOT watched by the shipper; the compactor reads its
	// sources from here.
	PendingDir string
	// ShippedDir is where the compactor writes merged + re-chunked L2 blocks.
	// Served by the BlockStore AND watched by the shipper.
	ShippedDir string
	// WALDir is where the block-level fragment WAL lives.
	WALDir string
	// WindowMs is the window/close size in ms (defaults to 2m).
	WindowMs int64
	// ReorderGraceMs is the post-window grace for late fragments (defaults 60s).
	ReorderGraceMs int64
	Logger         *slog.Logger
}

// NewManager wires the buffer, WAL and BlockStore, and replays the WAL so any
// fragments accepted-but-not-yet-flushed before a restart are re-buffered.
func NewManager(opts ManagerOptions) (*Manager, error) {
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}
	windowMs := opts.WindowMs
	if windowMs <= 0 {
		windowMs = defaultWindowMs
	}
	graceMs := opts.ReorderGraceMs
	if graceMs < 0 {
		graceMs = defaultReorderGraceMs
	}
	if opts.PendingDir == "" || opts.ShippedDir == "" {
		return nil, fmt.Errorf("manager: PendingDir and ShippedDir are required")
	}
	if opts.WALDir == "" {
		return nil, fmt.Errorf("manager: WALDir is required")
	}

	store, err := NewBlockStore(opts.PendingDir, opts.ShippedDir, logger)
	if err != nil {
		return nil, err
	}
	wal, err := OpenWAL(opts.WALDir)
	if err != nil {
		return nil, err
	}

	m := &Manager{
		buf:      newWindowBuffer(windowMs),
		wal:      wal,
		store:    store,
		windowMs: windowMs,
		graceMs:  graceMs,
		logger:   logger,
	}

	// Replay the WAL: re-bucket any accepted-but-unflushed fragments. Decode-free
	// — we only re-parse the frame structure, never the samples. A fragment
	// whose window was ALREADY persisted as a block before a crash is skipped
	// (idempotent replay), so a partially-flushed WAL never produces duplicate
	// blocks on restart.
	replayed := 0
	if rerr := ReplayWAL(opts.WALDir, func(frame []byte) error {
		n, derr := m.bufferFrameReplay(frame)
		if derr != nil {
			// A corrupt replayed frame is logged and skipped, not fatal.
			logger.Warn("manager: skip corrupt WAL frame on replay", "err", derr)
			return nil
		}
		replayed += n
		return nil
	}); rerr != nil {
		_ = wal.Close()
		_ = store.Close()
		return nil, fmt.Errorf("manager: WAL replay: %w", rerr)
	}
	if replayed > 0 {
		logger.Info("manager: replayed WAL fragments", "fragments", replayed)
	}

	return m, nil
}

// Store exposes the BlockStore (the StoreAPI query backend).
func (m *Manager) Store() *BlockStore { return m.store }

// WAL exposes the WAL (used by the flush loop to checkpoint, and tests).
func (m *Manager) WAL() *WAL { return m.wal }

// Append durably logs the frame, then buffers its raw chunks. It returns the
// number of fragments buffered. The WAL fsync happens BEFORE returning, so the
// caller may ack the agent once Append returns nil.
func (m *Manager) Append(frame []byte) (int, error) {
	if err := m.wal.Append(frame); err != nil {
		return 0, err
	}
	return m.bufferFrame(frame)
}

// bufferFrame decodes the ASAPFRG1 frame STRUCTURE (not samples) and buffers
// each fragment's raw chunk bytes into the window buffer.
func (m *Manager) bufferFrame(frame []byte) (int, error) {
	return m.bufferFrameFiltered(frame, false)
}

// bufferFrameReplay is bufferFrame for WAL replay: it skips any fragment whose
// MaxTime already falls inside a persisted block (idempotent replay).
func (m *Manager) bufferFrameReplay(frame []byte) (int, error) {
	return m.bufferFrameFiltered(frame, true)
}

func (m *Manager) bufferFrameFiltered(frame []byte, skipPersisted bool) (int, error) {
	frags, err := gorilla.DecodeFragmentBatch(frame)
	if err != nil {
		return 0, fmt.Errorf("decode fragment batch: %w", err)
	}
	buffered := 0
	for fi := range frags {
		f := &frags[fi]
		if f.Count == 0 || len(f.Data) == 0 {
			continue
		}
		if f.Encoding != "" && f.Encoding != "xor" {
			return buffered, fmt.Errorf("fragment %d: unsupported encoding %q", fi, f.Encoding)
		}
		if skipPersisted && m.store.CoversTime(f.MaxTime) {
			// This fragment's window was already flushed to a block before the
			// crash; re-buffering it would build a duplicate (overlapping) block.
			continue
		}
		ls := labelsFor(f.MetricName, f.Attributes)
		m.buf.add(ls, bufferedChunk{
			MinTime:    f.MinTime,
			MaxTime:    f.MaxTime,
			NumSamples: f.Count,
			Data:       f.Data,
		})
		buffered++
	}
	return buffered, nil
}

// FlushClosed flushes every window that is closable as of now (window end +
// grace <= now), building one block per window and making it queryable. It then
// checkpoints the WAL so the durably-blocked fragments are pruned. Returns the
// number of blocks built.
func (m *Manager) FlushClosed(now int64) (int, error) {
	closable := m.buf.closableWindows(now, m.graceMs)
	built := 0
	for _, wStart := range closable {
		ok, err := m.flushWindow(wStart)
		if err != nil {
			return built, err
		}
		if ok {
			built++
		}
	}
	if built > 0 {
		if rerr := m.store.Reload(); rerr != nil {
			return built, fmt.Errorf("flush: reload store: %w", rerr)
		}
		// The flushed windows are now durable as blocks; checkpoint (prune) the
		// WAL. Fragments for still-open windows were re-logged? No — they remain
		// only in the buffer + the (now-pruned) WAL. To avoid losing still-open
		// windows on a crash after checkpoint, we ONLY checkpoint when nothing is
		// left buffered. Otherwise we keep the WAL until the buffer drains.
		if m.buf.empty() {
			if cerr := m.wal.Checkpoint(); cerr != nil {
				return built, fmt.Errorf("flush: wal checkpoint: %w", cerr)
			}
		}
	}
	return built, nil
}

// FlushAll flushes EVERY buffered window regardless of grace (used on graceful
// shutdown so no accepted data is left only in the WAL).
func (m *Manager) FlushAll() (int, error) {
	all := m.buf.allWindows()
	built := 0
	for _, wStart := range all {
		ok, err := m.flushWindow(wStart)
		if err != nil {
			return built, err
		}
		if ok {
			built++
		}
	}
	if built > 0 {
		if rerr := m.store.Reload(); rerr != nil {
			return built, fmt.Errorf("flushAll: reload store: %w", rerr)
		}
		if m.buf.empty() {
			if cerr := m.wal.Checkpoint(); cerr != nil {
				return built, fmt.Errorf("flushAll: wal checkpoint: %w", cerr)
			}
		}
	}
	return built, nil
}

// flushWindow takes the buffered series for one window and builds a block.
// Returns ok=false (no error) when the window had nothing to write.
func (m *Manager) flushWindow(wStart int64) (bool, error) {
	series := m.buf.take(wStart)
	if len(series) == 0 {
		return false, nil
	}
	// Per-window L1 blocks land in the pending dir: served immediately by the
	// BlockStore (so recent data is queryable fast) but NOT shipped — the
	// compactor merges + re-chunks them into the shipped dir later.
	dir, err := buildBlock(m.store.PendingDir(), series)
	if err != nil {
		return false, fmt.Errorf("flush window %d: build block: %w", wStart, err)
	}
	m.logger.Info("built pending block from closed window",
		"window_start", wStart, "series", len(series), "dir", dir)
	return true, nil
}

// RunFlush drives FlushClosed on a ticker until ctx is cancelled, then flushes
// everything remaining on shutdown.
func (m *Manager) RunFlush(ctx context.Context, interval time.Duration) error {
	if interval <= 0 {
		interval = time.Minute
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			if _, err := m.FlushAll(); err != nil {
				m.logger.Warn("flush-all on shutdown failed", "err", err)
			}
			return ctx.Err()
		case <-t.C:
			if _, err := m.FlushClosed(time.Now().UnixMilli()); err != nil {
				m.logger.Warn("flush pass failed", "err", err)
			}
		}
	}
}

// Close flushes everything still buffered, then closes the WAL and BlockStore.
func (m *Manager) Close() error {
	if _, err := m.FlushAll(); err != nil {
		m.logger.Warn("flush-all on close failed", "err", err)
	}
	var firstErr error
	if err := m.wal.Close(); err != nil {
		firstErr = err
	}
	if err := m.store.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}
