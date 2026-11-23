package flashdb

import (
	"compress/gzip"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/arriqaaq/art"
)

const (
	snapshotFile     = "snapshot.rdb"
	snapshotTempFile = "snapshot.rdb.tmp"
	snapshotMagic    = "FLDBSNAP"
	snapshotVersion  = 1
)

// snapshotMeta records the point in the append-only log at which the snapshot
// was taken. On recovery every entry in segment firstSegment or later must be
// replayed; the segments before it are fully covered by the snapshot.
type snapshotMeta struct {
	Magic         string `json:"magic"`
	Version       uint16 `json:"version"`
	FirstSegment  uint64 `json:"first_segment"`
	RecordCount   uint64 `json:"record_count"`
	TakenUnixNano int64  `json:"taken_unix_nano"`
}

// startSnapshotScheduler triggers a Snapshot on a fixed interval until Close.
func (db *FlashDB) startSnapshotScheduler(interval time.Duration) {
	db.snapshotStop = make(chan struct{})
	db.snapshotDone = make(chan struct{})
	go func() {
		defer close(db.snapshotDone)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				_ = db.Snapshot()
			case <-db.snapshotStop:
				return
			}
		}
	}()
}

// Snapshot persists a compressed, point-in-time copy of the live data and
// truncates the append-only log entries that the snapshot now covers.
//
// Writes that were acknowledged before the snapshot starts are included in it;
// writes committed while the snapshot is being written remain in the log and
// are replayed on top of the snapshot during recovery, so no acknowledged
// write is ever lost.
func (db *FlashDB) Snapshot() error {
	if db.log == nil {
		return nil
	}

	db.snapshotMu.Lock()
	defer db.snapshotMu.Unlock()

	// Block new write transactions while the in-memory state is serialized.
	// Disk I/O (compression, fsync, log truncation) happens after the lock is
	// released, so the write path is only paused for the in-memory scan.
	db.mu.Lock()
	if db.closed {
		db.mu.Unlock()
		return ErrDatabaseClosed
	}

	firstSegment, err := db.logTailSegment()
	if err != nil {
		db.mu.Unlock()
		return err
	}

	records, err := db.snapshotRecords()
	if err != nil {
		db.mu.Unlock()
		return err
	}
	db.mu.Unlock()

	meta := snapshotMeta{
		Magic:         snapshotMagic,
		Version:       snapshotVersion,
		FirstSegment:  firstSegment,
		RecordCount:   uint64(len(records)),
		TakenUnixNano: time.Now().UnixNano(),
	}

	if err := db.writeSnapshotFile(meta, records); err != nil {
		return err
	}

	return db.truncateLogBefore(firstSegment)
}

// snapshotRecords walks every store and re-encodes the live entries using the
// same record format as the append-only log, so recovery reuses the existing
// build*Record replay logic.
func (db *FlashDB) snapshotRecords() ([]*record, error) {
	records := make([]*record, 0)

	// Strings
	db.strStore.Each(func(node *art.Node) {
		if !node.IsLeaf() {
			return
		}
		key := string(node.Key())
		value, _ := node.Value().(string)
		records = append(records, newRecord(node.Key(), []byte(value), StringRecord, StringSet))
		if ttl := db.getTTL(String, key); ttl != nil {
			records = append(records, newRecordWithExpire([]byte(key), nil, ttl.(int64), StringRecord, StringExpire))
		}
	})

	// Hashes
	for _, key := range db.hashStore.Keys() {
		fields := db.hashStore.HGetAll(key)
		for i := 0; i+1 < len(fields); i += 2 {
			member := fields[i].(string)
			value := fields[i+1].(string)
			records = append(records, newRecordWithValue([]byte(key), []byte(member), []byte(value), HashRecord, HashHSet))
		}
		if ttl := db.getTTL(Hash, key); ttl != nil {
			records = append(records, newRecordWithExpire([]byte(key), nil, ttl.(int64), HashRecord, HashHExpire))
		}
	}

	// Sets
	for _, key := range db.setStore.Keys() {
		for _, member := range db.setStore.SMembers(key) {
			records = append(records, newRecord([]byte(key), []byte(member.(string)), SetRecord, SetSAdd))
		}
		if ttl := db.getTTL(Set, key); ttl != nil {
			records = append(records, newRecordWithExpire([]byte(key), nil, ttl.(int64), SetRecord, SetSExpire))
		}
	}

	// Sorted sets
	for _, key := range db.zsetStore.Keys() {
		card := db.zsetStore.ZCard(key)
		pairs := db.zsetStore.ZRangeWithScores(key, 0, card-1)
		for i := 0; i+1 < len(pairs); i += 2 {
			member := pairs[i].(string)
			score := pairs[i+1].(float64)
			records = append(records, newRecordWithValue(
				[]byte(key), []byte(member), []byte(float64ToStr(score)), ZSetRecord, ZSetZAdd))
		}
		if ttl := db.getTTL(ZSet, key); ttl != nil {
			records = append(records, newRecordWithExpire([]byte(key), nil, ttl.(int64), ZSetRecord, ZSetZExpire))
		}
	}

	return records, nil
}

// writeSnapshotFile writes the snapshot to a temp file, fsyncs it and then
// atomically renames it into place. A crash at any point leaves either the
// previous snapshot or no snapshot behind, never a partial one.
func (db *FlashDB) writeSnapshotFile(meta snapshotMeta, records []*record) error {
	tmpPath := filepath.Join(db.config.Path, snapshotTempFile)
	finalPath := filepath.Join(db.config.Path, snapshotFile)

	f, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0640)
	if err != nil {
		return err
	}

	gw, err := gzip.NewWriterLevel(f, gzip.BestSpeed)
	if err != nil {
		_ = f.Close()
		return err
	}

	header, err := json.Marshal(meta)
	if err != nil {
		_ = f.Close()
		return err
	}
	if err := binary.Write(gw, binary.BigEndian, uint32(len(header))); err != nil {
		_ = f.Close()
		return err
	}
	if _, err := gw.Write(header); err != nil {
		_ = f.Close()
		return err
	}

	for _, r := range records {
		enc, err := r.encode()
		if err != nil {
			_ = f.Close()
			return err
		}
		if err := binary.Write(gw, binary.BigEndian, uint32(len(enc))); err != nil {
			_ = f.Close()
			return err
		}
		if _, err := gw.Write(enc); err != nil {
			_ = f.Close()
			return err
		}
	}

	if err := gw.Close(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}

	return os.Rename(tmpPath, finalPath)
}

// loadSnapshot reads the snapshot (if present) and replays its records into
// the in-memory stores. It returns the first log segment that still has to be
// replayed; 0 means no snapshot was found and the whole log must be replayed.
func (db *FlashDB) loadSnapshot() (uint64, error) {
	path := filepath.Join(db.config.Path, snapshotFile)
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	defer f.Close()

	gr, err := gzip.NewReader(f)
	if err != nil {
		return 0, err
	}
	defer gr.Close()

	var headerLen uint32
	if err := binary.Read(gr, binary.BigEndian, &headerLen); err != nil {
		return 0, err
	}
	header := make([]byte, headerLen)
	if _, err := io.ReadFull(gr, header); err != nil {
		return 0, err
	}

	var meta snapshotMeta
	if err := json.Unmarshal(header, &meta); err != nil {
		return 0, err
	}
	if meta.Magic != snapshotMagic || meta.Version != snapshotVersion {
		return 0, fmt.Errorf("incompatible snapshot: %+v", meta)
	}

	for i := uint64(0); i < meta.RecordCount; i++ {
		var recLen uint32
		if err := binary.Read(gr, binary.BigEndian, &recLen); err != nil {
			return 0, err
		}
		buf := make([]byte, recLen)
		if _, err := io.ReadFull(gr, buf); err != nil {
			return 0, err
		}
		r, err := decode(buf)
		if err != nil {
			return 0, err
		}
		if len(r.meta.key) > 0 {
			if err := db.loadRecord(r); err != nil {
				return 0, err
			}
		}
	}

	return meta.FirstSegment, nil
}

// logTailSegment returns the numeric index of the current (appendable) log
// segment. Segments are named with a zero-padded 20-digit index.
func (db *FlashDB) logTailSegment() (uint64, error) {
	segments, err := listLogSegments(db.config.Path)
	if err != nil {
		return 0, err
	}
	if len(segments) == 0 {
		return 0, fmt.Errorf("no append-only log segments found in %s", db.config.Path)
	}
	return segments[len(segments)-1], nil
}

// listLogSegments returns the numeric segment indexes on disk in ascending
// order, ignoring snapshot and unrelated files.
func listLogSegments(dir string) ([]uint64, error) {
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	segments := make([]uint64, 0, len(files))
	for _, file := range files {
		name := file.Name()
		if file.IsDir() || len(name) != 20 {
			continue
		}
		index, err := strconv.ParseUint(name, 10, 64)
		if err != nil || index == 0 {
			continue
		}
		segments = append(segments, index)
	}
	return segments, nil
}

// truncateLogBefore removes every log segment strictly older than firstSegment.
// The snapshot is already durable on disk when this runs, so the removed
// entries remain recoverable through it. The tail segment is never deleted:
// entries written after the snapshot live there and must stay replayable.
func (db *FlashDB) truncateLogBefore(firstSegment uint64) error {
	segments, err := listLogSegments(db.config.Path)
	if err != nil {
		return err
	}
	for _, index := range segments {
		if index >= firstSegment {
			continue
		}
		if err := os.Remove(filepath.Join(db.config.Path, fmt.Sprintf("%020d", index))); err != nil &&
			!os.IsNotExist(err) {
			return err
		}
	}
	return nil
}
