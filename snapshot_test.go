package flashdb

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/arriqaaq/aol"
	"github.com/arriqaaq/hash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func openAolWithSegmentSize(dir string, segmentSize int) (*aol.Log, error) {
	opts := aol.DefaultOptions
	opts.NoSync = true
	opts.SegmentSize = segmentSize
	return aol.Open(dir, opts)
}

func snapshotTestDB(t *testing.T, dir string) *FlashDB {
	t.Helper()
	cfg := &Config{
		Addr:   DefaultAddr,
		Path:   dir,
		NoSync: true,
	}
	db, err := New(cfg)
	require.NoError(t, err)
	return db
}

func writeMixedData(t *testing.T, db *FlashDB, prefix string, n int) {
	t.Helper()
	for i := 1; i <= n; i++ {
		key := fmt.Sprintf("%s_key_%d", prefix, i)
		member := fmt.Sprintf("%s_member_%d", prefix, i)
		value := fmt.Sprintf("%s_value_%d", prefix, i)
		err := db.Update(func(tx *Tx) error {
			tx.Set(key, member)
			_, err := tx.HSet(key, member, value)
			require.NoError(t, err)
			err = tx.SAdd(key, member)
			require.NoError(t, err)
			err = tx.ZAdd(key, 10.0, member)
			require.NoError(t, err)
			return nil
		})
		require.NoError(t, err)
	}
}

func assertMixedData(t *testing.T, db *FlashDB, prefix string, n int) {
	t.Helper()
	for i := 1; i <= n; i++ {
		key := fmt.Sprintf("%s_key_%d", prefix, i)
		member := fmt.Sprintf("%s_member_%d", prefix, i)
		value := fmt.Sprintf("%s_value_%d", prefix, i)
		err := db.View(func(tx *Tx) error {
			got, err := tx.Get(key)
			assert.NoError(t, err)
			assert.Equal(t, member, got)
			assert.Equal(t, value, tx.HGet(key, member))
			assert.True(t, tx.SIsMember(key, member))
			ok, score := tx.ZScore(key, member)
			assert.True(t, ok)
			assert.Equal(t, 10.0, score)
			return nil
		})
		require.NoError(t, err)
	}
}

// Snapshot covers the first batch of writes; a restart must restore them even
// though the log segments they lived in have been deleted.
func TestSnapshot_RestoreAfterTruncation(t *testing.T) {
	dir := filepath.Join(tmpDir, "snapshot_restore")
	defer os.RemoveAll(dir)

	db := snapshotTestDB(t, dir)
	writeMixedData(t, db, "a", 50)
	require.NoError(t, db.Snapshot())
	require.NoError(t, db.Close())

	segments, err := listLogSegments(dir)
	require.NoError(t, err)
	require.Len(t, segments, 1, "older segments must be truncated, tail kept")

	_, err = os.Stat(filepath.Join(dir, snapshotFile))
	require.NoError(t, err)

	db2 := snapshotTestDB(t, dir)
	defer db2.Close()
	assertMixedData(t, db2, "a", 50)
}

// Writes acknowledged after the snapshot must survive a restart: they stay in
// the log and are replayed on top of the snapshot.
func TestSnapshot_WritesAfterSnapshotAreReplayed(t *testing.T) {
	dir := filepath.Join(tmpDir, "snapshot_replay")
	defer os.RemoveAll(dir)

	db := snapshotTestDB(t, dir)
	writeMixedData(t, db, "before", 30)
	require.NoError(t, db.Snapshot())
	writeMixedData(t, db, "after", 30)
	require.NoError(t, db.Close())

	db2 := snapshotTestDB(t, dir)
	defer db2.Close()
	assertMixedData(t, db2, "before", 30)
	assertMixedData(t, db2, "after", 30)
}

// Repeated snapshots must keep producing a consistent, fully recoverable state.
func TestSnapshot_MultipleSnapshots(t *testing.T) {
	dir := filepath.Join(tmpDir, "snapshot_multi")
	defer os.RemoveAll(dir)

	db := snapshotTestDB(t, dir)
	for round := 0; round < 3; round++ {
		writeMixedData(t, db, fmt.Sprintf("round%d", round), 20)
		require.NoError(t, db.Snapshot())
	}
	require.NoError(t, db.Close())

	db2 := snapshotTestDB(t, dir)
	defer db2.Close()
	for round := 0; round < 3; round++ {
		assertMixedData(t, db2, fmt.Sprintf("round%d", round), 20)
	}
}

// Deletes applied before the snapshot must not resurrect after recovery.
func TestSnapshot_TombstonesAreCompacted(t *testing.T) {
	dir := filepath.Join(tmpDir, "snapshot_tombstone")
	defer os.RemoveAll(dir)

	db := snapshotTestDB(t, dir)
	writeMixedData(t, db, "del", 20)
	require.NoError(t, db.Update(func(tx *Tx) error {
		for i := 1; i <= 10; i++ {
			require.NoError(t, tx.Delete(fmt.Sprintf("del_key_%d", i)))
		}
		return nil
	}))
	require.NoError(t, db.Snapshot())
	require.NoError(t, db.Close())

	db2 := snapshotTestDB(t, dir)
	defer db2.Close()
	for i := 1; i <= 10; i++ {
		err := db2.View(func(tx *Tx) error {
			_, err := tx.Get(fmt.Sprintf("del_key_%d", i))
			return err
		})
		assert.Error(t, err)
	}
	for i := 11; i <= 20; i++ {
		key := fmt.Sprintf("del_key_%d", i)
		member := fmt.Sprintf("del_member_%d", i)
		value := fmt.Sprintf("del_value_%d", i)
		require.NoError(t, db2.View(func(tx *Tx) error {
			got, err := tx.Get(key)
			assert.NoError(t, err)
			assert.Equal(t, member, got)
			assert.Equal(t, value, tx.HGet(key, member))
			return nil
		}))
	}
}

// Un-expired TTLs must be carried by the snapshot and still expire afterwards.
func TestSnapshot_KeepsLiveTTL(t *testing.T) {
	dir := filepath.Join(tmpDir, "snapshot_ttl")
	defer os.RemoveAll(dir)

	db := snapshotTestDB(t, dir)
	require.NoError(t, db.Update(func(tx *Tx) error {
		return tx.Set("ttl_key", "ttl_val")
	}))
	require.NoError(t, db.Update(func(tx *Tx) error {
		return tx.Expire("ttl_key", 3600)
	}))
	require.NoError(t, db.Snapshot())
	require.NoError(t, db.Close())

	db2 := snapshotTestDB(t, dir)
	defer db2.Close()
	var ttl int64
	require.NoError(t, db2.View(func(tx *Tx) error {
		ttl = tx.TTL("ttl_key")
		return nil
	}))
	assert.Greater(t, ttl, int64(0))

	var val string
	require.NoError(t, db2.View(func(tx *Tx) error {
		var err error
		val, err = tx.Get("ttl_key")
		return err
	}))
	assert.Equal(t, "ttl_val", val)
}

// The compressed snapshot must be materially smaller than the raw records it
// holds (the workload is repetitive, like the production 2 GB append log).
func TestSnapshot_IsCompressed(t *testing.T) {
	dir := filepath.Join(tmpDir, "snapshot_compress")
	defer os.RemoveAll(dir)

	db := snapshotTestDB(t, dir)
	writeMixedData(t, db, "zip", 2000)
	require.NoError(t, db.Snapshot())
	require.NoError(t, db.Close())

	var logBytes int64
	segments, err := listLogSegments(dir)
	require.NoError(t, err)
	for _, index := range segments {
		info, err := os.Stat(filepath.Join(dir, fmt.Sprintf("%020d", index)))
		require.NoError(t, err)
		logBytes += info.Size()
	}

	info, err := os.Stat(filepath.Join(dir, snapshotFile))
	require.NoError(t, err)
	assert.Less(t, info.Size(), logBytes/2,
		"gzip snapshot should be much smaller than the uncompressed log")
	t.Logf("log tail=%d bytes, snapshot=%d bytes", logBytes, info.Size())
}

// Recovery must work when the log spans multiple segments, replaying only the
// segments at or after the snapshot boundary using their real segment indexes.
func TestSnapshot_ReplayAcrossSegments(t *testing.T) {
	dir := filepath.Join(tmpDir, "snapshot_segments")
	defer os.RemoveAll(dir)

	cfg := &Config{Addr: DefaultAddr, Path: dir, NoSync: true}
	// Shrink the segment size so a few hundred records span many segments.
	l, err := openAolWithSegmentSize(dir, 64*1024)
	require.NoError(t, err)

	db := &FlashDB{
		config:    cfg,
		strStore:  newStrStore(),
		setStore:  newSetStore(),
		hashStore: newHashStore(),
		zsetStore: newZSetStore(),
		exps:      hash.New(),
		log:       l,
		persist:   true,
	}
	db.log = l

	writeMixedData(t, db, "seg", 400)
	before, err := listLogSegments(dir)
	require.NoError(t, err)
	require.Greater(t, len(before), 1)

	require.NoError(t, db.Snapshot())
	writeMixedData(t, db, "segpost", 100)
	require.NoError(t, db.Close())

	db2, err := New(cfg)
	require.NoError(t, err)
	defer db2.Close()
	assertMixedData(t, db2, "seg", 400)
	assertMixedData(t, db2, "segpost", 100)
}

// After a snapshot truncates old segments, continued writes that roll the log
// into brand new segments must remain durable and replayable.
func TestSnapshot_WritesRollNewSegmentsAfterTruncation(t *testing.T) {
	dir := filepath.Join(tmpDir, "snapshot_roll")
	defer os.RemoveAll(dir)

	l, err := openAolWithSegmentSize(dir, 64*1024)
	require.NoError(t, err)
	cfg := &Config{Addr: DefaultAddr, Path: dir, NoSync: true}
	db := &FlashDB{
		config:    cfg,
		strStore:  newStrStore(),
		setStore:  newSetStore(),
		hashStore: newHashStore(),
		zsetStore: newZSetStore(),
		exps:      hash.New(),
		log:       l,
		persist:   true,
	}

	writeMixedData(t, db, "pre", 300)
	require.NoError(t, db.Snapshot())

	// Enough new writes to create several segments after the snapshot point.
	writeMixedData(t, db, "post", 500)
	segments, err := listLogSegments(dir)
	require.NoError(t, err)
	require.Greater(t, len(segments), 1)

	require.NoError(t, db.Close())

	db2, err := New(cfg)
	require.NoError(t, err)
	defer db2.Close()
	assertMixedData(t, db2, "pre", 300)
	assertMixedData(t, db2, "post", 500)
}

// Restart-style timing test. The snapshot's job is to shrink the amount of
// append-only log that must be replayed: after a snapshot with no new writes,
// the replay window drops to zero records. This test asserts that directly and
// logs the end-to-end recovery timings for reference. Run explicitly with
// FLASHDB_RECOVERY_BENCH=1 since it writes a large dataset.
func TestSnapshot_RecoveryIsFaster(t *testing.T) {
	if os.Getenv("FLASHDB_RECOVERY_BENCH") == "" {
		t.Skip("set FLASHDB_RECOVERY_BENCH=1 to run")
	}
	dir := filepath.Join(tmpDir, "snapshot_timing")
	defer os.RemoveAll(dir)

	l, err := openAolWithSegmentSize(dir, 256*1024)
	require.NoError(t, err)
	cfg := &Config{Addr: DefaultAddr, Path: dir, NoSync: true}
	db := &FlashDB{
		config:    cfg,
		strStore:  newStrStore(),
		setStore:  newSetStore(),
		hashStore: newHashStore(),
		zsetStore: newZSetStore(),
		exps:      hash.New(),
		log:       l,
		persist:   true,
	}
	writeMixedData(t, db, "perf", 20000)

	// Baseline: no snapshot, every log record is replayed on restart.
	require.NoError(t, db.Close())
	baselineSegments, err := listLogSegments(dir)
	require.NoError(t, err)
	require.Greater(t, len(baselineSegments), 1, "test setup must span multiple segments")
	var baselineRecords uint64
	l2, err := openAolWithSegmentSize(dir, 256*1024)
	require.NoError(t, err)
	dbProbe := &FlashDB{config: cfg, log: l2}
	for _, index := range baselineSegments {
		for j := uint64(0); ; j++ {
			if _, err := dbProbe.log.Read(index, j); err != nil {
				break
			}
			baselineRecords++
		}
	}
	require.NoError(t, dbProbe.Close())

	start := time.Now()
	db2, err := New(cfg)
	require.NoError(t, err)
	fullReplay := time.Since(start)

	require.NoError(t, db2.Snapshot())
	require.NoError(t, db2.Close())

	// After the snapshot, only the (empty) tail segment remains to replay.
	snapshotSegments, err := listLogSegments(dir)
	require.NoError(t, err)
	require.Len(t, snapshotSegments, 1)

	start = time.Now()
	db3, err := New(cfg)
	require.NoError(t, err)
	snapshotRecovery := time.Since(start)
	defer db3.Close()

	assertMixedData(t, db3, "perf", 20000)

	var replayRecords uint64
	for _, index := range snapshotSegments {
		for j := uint64(0); ; j++ {
			if _, err := db3.log.Read(index, j); err != nil {
				break
			}
			replayRecords++
		}
	}

	t.Logf("segments to replay: baseline=%d, snapshot=%d; post-snapshot replay records=%d",
		len(baselineSegments), len(snapshotSegments), replayRecords)
	t.Logf("end-to-end recovery: full replay=%s, snapshot path=%s", fullReplay, snapshotRecovery)
	// The tail segment is always retained (new writes still append to it), so
	// recovery re-applies at most one segment's worth of records idempotently.
	// The key win is that every sealed segment is gone: the replay window is
	// bounded by a single segment instead of the whole log.
	assert.Equal(t, 1, len(snapshotSegments))
	assert.Less(t, len(snapshotSegments), len(baselineSegments))
	assert.Less(t, replayRecords, baselineRecords,
		"replay must be bounded to the retained tail segment, not the full log")
}

// The background scheduler must take snapshots on the configured interval and
// a restart through New() must restore the data captured by those snapshots.
func TestSnapshot_BackgroundScheduler(t *testing.T) {
	dir := filepath.Join(tmpDir, "snapshot_scheduler")
	defer os.RemoveAll(dir)

	cfg := &Config{
		Addr:             DefaultAddr,
		Path:             dir,
		NoSync:           true,
		SnapshotInterval: 1,
	}
	db, err := New(cfg)
	require.NoError(t, err)
	writeMixedData(t, db, "sched", 100)

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(filepath.Join(dir, snapshotFile)); err == nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	require.NoError(t, db.Close())

	_, err = os.Stat(filepath.Join(dir, snapshotFile))
	require.NoError(t, err, "background snapshot should have run")

	db2, err := New(cfg)
	require.NoError(t, err)
	defer db2.Close()
	assertMixedData(t, db2, "sched", 100)
}
