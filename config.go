package flashdb

import (
	"strconv"
	"time"
)

const (
	DefaultAddr         = "127.0.0.1:8000"
	DefaultMaxKeySize   = uint32(1 * 1024)
	DefaultMaxValueSize = uint32(8 * 1024)
)

type Config struct {
	Addr             string `json:"addr" toml:"addr"`
	Path             string `json:"path" toml:"path"`                           // dir path for append-only logs
	EvictionInterval int    `json:"eviction_interval" toml:"eviction_interval"` // in seconds
	// SnapshotInterval is the period between automatic snapshots, in seconds.
	// When > 0, FlashDB periodically persists a compressed snapshot of the live
	// data and truncates the append-only log up to the snapshot point, so a
	// restart only replays the log entries written after the snapshot.
	// When 0 (the default) automatic snapshots are disabled.
	SnapshotInterval int `json:"snapshot_interval" toml:"snapshot_interval"`
	// NoSync disables fsync after writes. This is less durable and puts the
	// log at risk of data loss when there's a server crash.
	NoSync bool
}

func (c *Config) validate() {
	if c.Addr == "" {
		c.Addr = DefaultAddr
	}
}

func (c *Config) evictionInterval() time.Duration {
	return time.Duration(c.EvictionInterval) * time.Second
}

func (c *Config) snapshotInterval() time.Duration {
	return time.Duration(c.SnapshotInterval) * time.Second
}

func DefaultConfig() *Config {
	return &Config{
		Addr:             DefaultAddr,
		Path:             "/tmp/flashdb",
		EvictionInterval: 10,
	}
}

func float64ToStr(val float64) string {
	return strconv.FormatFloat(val, 'f', -1, 64)
}

func strToFloat64(val string) (float64, error) {
	return strconv.ParseFloat(val, 64)
}
