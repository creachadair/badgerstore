// Copyright 2019 Michael J. Fromberger. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package badgerstore implements the [blob.Store] interface using BadgerDB.
package badgerstore

import (
	"context"
	"errors"
	"net/url"
	"path/filepath"
	"strconv"
	"time"

	"github.com/creachadair/ffs/blob"
	"github.com/creachadair/ffs/storage/dbkey"
	"github.com/creachadair/taskgroup"
	badger "github.com/dgraph-io/badger/v4"
)

// Opener constructs a filestore from an address comprising a URL, for use with
// the store package. The host and path of the URL give the path of the
// database directory.
//
// Optional query parameters include:
//
//	base_size=n      : base table size in MiB (default 2)
//	compact_on_close : do a L0 compaction on close (default true)
//	index_cache=m    : index cache size in MiB (default 50)
//	read_only        : open the database in read-only mode (default false)
//	auto_sync        : automatically sync writes when GCing (default false)
func Opener(_ context.Context, addr string) (blob.StoreCloser, error) {
	opts, err := parseOptions(addr)
	if err != nil {
		return nil, err
	}
	return New(opts)
}

// Options are optional settings for a [Store] or a [KV].
type Options struct {
	// Badger are the options to use for creating or opening a BadgerDB.
	// At least the Dir field must be set.
	Badger badger.Options

	// AutoSync, if true, enables automatic periodic sync to disk.
	AutoSync bool

	// KeyPrefix, if non-empty, is automatically prepended to all keys, and
	// scopes the resulting access to only keys having that prefix.
	KeyPrefix string
}

func parseOptions(addr string) (Options, error) {
	u, err := url.Parse(addr)
	if err != nil {
		return Options{}, err
	}
	filePath := filepath.Join(u.Host, filepath.FromSlash(u.Path))
	readOnly := parseBool(u, "read_only", false)
	badgerOpts := badger.DefaultOptions(filePath).
		WithNumVersionsToKeep(1).
		WithCompactL0OnClose(parseBool(u, "compact_on_close", !readOnly)).
		WithBaseTableSize(parseInt(u, "base_size", 2) << 20).
		WithIndexCacheSize(parseInt(u, "index_cache", 50) << 20).
		WithLogger(nil).
		WithReadOnly(readOnly)
	return Options{
		Badger:   badgerOpts,
		AutoSync: parseBool(u, "auto_sync", false),
	}, nil
}

func parseBool(u *url.URL, key string, dflt bool) bool {
	v := u.Query().Get(key)
	if v == "" {
		return dflt
	}
	ok, err := strconv.ParseBool(v)
	if err != nil {
		return dflt
	}
	return ok
}

func parseInt(u *url.URL, key string, dflt int64) int64 {
	v := u.Query().Get(key)
	if v == "" {
		return dflt
	}
	z, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return dflt
	}
	return z
}

// dbMonitor is a shared wrapper around an underlying badger DB instance that
// maintains the plumbing for automatic compactions and shutdown.  It is safe
// for multiple goroutines to share a single *dbMonitor d, and to access the
// methods of d.DB, without a lock.
type dbMonitor struct {
	DB     *badger.DB
	stopGC context.CancelFunc
	gc     *taskgroup.Single[error]
}

func (m *dbMonitor) isClosed() bool { return m.DB.IsClosed() }

func (m *dbMonitor) stopAndWait() { m.stopGC(); m.gc.Wait() }

func (m *dbMonitor) closeDB() error { return m.DB.Close() }

// Store implements the [blob.Store] interface using a BadgerDB instance.
type Store struct {
	mon    *dbMonitor
	prefix dbkey.Prefix
}

// New constructs a Store by opening or creating a BadgerDB instance with the
// specified options.
func New(opts Options) (Store, error) {
	mon, err := newMonitor(opts)
	if err != nil {
		return Store{}, err
	}
	return Store{mon: mon, prefix: dbkey.Prefix(opts.KeyPrefix)}, nil
}

// Keyspace satisfies part of the [blob.Store] interface. The values returned
// have concrete type [badgerstore.KV].
func (s Store) Keyspace(_ context.Context, name string) (blob.KV, error) {
	return KV{mon: s.mon, prefix: s.prefix.Keyspace(name)}, nil
}

// Sub satisfies part of the [blob.Store] interface.
func (s Store) Sub(_ context.Context, name string) (blob.Store, error) {
	return Store{mon: s.mon, prefix: s.prefix.Sub(name)}, nil
}

// Close satisfies part of the [blob.StoreCloser] interface.
func (s Store) Close(_ context.Context) error {
	if !s.mon.isClosed() {
		s.mon.stopAndWait()
	}
	return s.mon.closeDB()
}

// KV implements the [blob.KV] interface using a Badger key-value store.
type KV struct {
	mon *dbMonitor

	// The prefix of the key space belonging to this KV.  If empty, this refers
	// to the whole database.
	prefix dbkey.Prefix
}

var errClosed = errors.New("database is closed")

// NewKV creates a [KV] by opening the Badger database specified by opts.
func NewKV(opts Options) (KV, error) {
	mon, err := newMonitor(opts)
	if err != nil {
		return KV{}, err
	}
	return KV{mon: mon, prefix: dbkey.Prefix(opts.KeyPrefix)}, nil
}

func newMonitor(opts Options) (*dbMonitor, error) {
	db, err := badger.Open(opts.Badger)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	gc := taskgroup.Run(func() {
		t := time.NewTicker(time.Minute)
		defer t.Stop()

		// Run the GC once at startup to prime the state.
		var lastSize int64
		var lastRun time.Time
		rungc := func() {
			if db.RunValueLogGC(0.5) == nil {
				db.RunValueLogGC(0.5)
			}
			_, lastSize = db.Size()
			lastRun = time.Now()
		}

		rungc()
		for {
			_, curSize := db.Size()
			delta := max(curSize, lastSize) - min(curSize, lastSize)
			if delta > 512<<20 || time.Since(lastRun) >= 10*time.Minute {
				rungc()
			}
			if opts.AutoSync {
				db.Sync()
			}
			select {
			case <-ctx.Done():
				return
			case <-t.C:
			}
		}
	})
	return &dbMonitor{
		DB:     db,
		stopGC: cancel,
		gc:     gc,
	}, nil
}

// Close implements part of the [blob.KV] interface. It closes the underlying
// database instance and reports its result.
func (s KV) Close(_ context.Context) error {
	if !s.mon.isClosed() {
		s.mon.stopAndWait()
	}
	return s.mon.closeDB()
}

// Get implements part of [blob.KV].
func (s KV) Get(_ context.Context, key string) (data []byte, err error) {
	if s.mon.isClosed() {
		return nil, errClosed
	}
	realKey := s.prefix.Add(key)
	err = s.mon.DB.View(func(txn *badger.Txn) error {
		itm, err := txn.Get([]byte(realKey))
		if err == nil {
			data, err = itm.ValueCopy(data)
		}
		return err
	})
	if err == badger.ErrKeyNotFound || err == badger.ErrEmptyKey {
		err = blob.KeyNotFound(key)
	}
	return
}

// Put implements part of [blob.KV].
func (s KV) Put(_ context.Context, opts blob.PutOptions) error {
	if s.mon.isClosed() {
		return errClosed
	}
	realKey := []byte(s.prefix.Add(opts.Key))
	for {
		err := s.mon.DB.Update(func(txn *badger.Txn) error {
			_, gerr := txn.Get(realKey)
			if !opts.Replace {
				if gerr == nil {
					return blob.KeyExists(opts.Key)
				} else if gerr != badger.ErrKeyNotFound {
					return gerr
				}
			}
			return txn.Set(realKey, opts.Data)
		})
		if !errors.Is(err, badger.ErrConflict) {
			return err // including nil
		}
	}
}

// Delete implements part of [blob.KV].
func (s KV) Delete(_ context.Context, key string) error {
	if s.mon.isClosed() {
		return errClosed
	} else if key == "" {
		return blob.KeyNotFound(key) // badger cannot store empty keys
	}
	realKey := []byte(s.prefix.Add(key))
	for {
		err := s.mon.DB.Update(func(txn *badger.Txn) error {
			_, err := txn.Get(realKey)
			if err == nil {
				return txn.Delete(realKey)
			} else if err == badger.ErrKeyNotFound {
				return blob.KeyNotFound(key)
			}
			return err
		})
		if !errors.Is(err, badger.ErrConflict) {
			return err // including nil
		}
	}
}

// List implements part of [blob.KV].
func (s KV) List(ctx context.Context, start string, f func(string) error) error {
	if s.mon.isClosed() {
		return errClosed
	}
	fullPrefix := s.prefix.Add(start)
	return s.mon.DB.View(func(txn *badger.Txn) error {
		// N.B. The default prefetches values too.
		it := txn.NewIterator(badger.IteratorOptions{
			PrefetchValues: false, // faster, since we only want the keys

			// Note we do not use fullPrefix here, because start is not itself a
			// prefix but a point in the order.
			Prefix: []byte(s.prefix),
		})
		defer it.Close()

		for it.Seek([]byte(fullPrefix)); it.Valid(); it.Next() {
			fullKey := it.Item().Key()
			key := s.prefix.Remove(string(fullKey))
			if err := f(key); errors.Is(err, blob.ErrStopListing) {
				return nil
			} else if err != nil {
				return err
			} else if err := ctx.Err(); err != nil {
				return err
			}
		}
		return nil
	})
}

// Len implements part of [blob.KV].
func (s KV) Len(ctx context.Context) (int64, error) {
	if s.mon.isClosed() {
		return 0, errClosed
	}

	// Reaching here, we don't know the size.
	// Compute and store it.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	g := taskgroup.New(cancel)
	var size int64
	c := taskgroup.Gather(g.Go, func(v int64) { size += v })

	for i := range 256 {
		pfx := s.prefix.Add(string(byte(i)))
		c.Call(func() (int64, error) {
			var size int64
			err := s.mon.DB.View(func(txn *badger.Txn) error {
				it := txn.NewIterator(badger.IteratorOptions{
					Prefix: []byte(pfx),
				})
				defer it.Close()

				for it.Rewind(); it.Valid(); it.Next() {
					select {
					case <-ctx.Done():
						return ctx.Err()
					default:
						size++
					}
				}
				return nil
			})
			return size, err
		})
	}
	if err := g.Wait(); err != nil {
		return 0, err
	}
	return size, nil
}
