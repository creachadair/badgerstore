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

// Package badgerstore implements the blob.Store interface using Badger.
package badgerstore

import (
	"context"
	"encoding/binary"
	"errors"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/creachadair/atomicfile"
	"github.com/creachadair/ffs/blob"
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
func Opener(_ context.Context, addr string) (blob.Store, error) {
	opts, err := parseOptions(addr)
	if err != nil {
		return nil, err
	}
	return New(opts)
}

// Options are optional settings for a Store.
type Options struct {
	Badger   badger.Options // native options for BadgerDB
	AutoSync bool           // enable auto-sync when GCing
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

// Store implements the blob.Store interface using a Badger key-value store.
type Store struct {
	db     *badger.DB
	stopGC context.CancelFunc
	gc     *taskgroup.Single[error]

	μ        sync.Mutex
	size     int64  // number of keys (-1 means unknown)
	sizeFile string // file path (in database directory)
}

var errClosed = errors.New("database is closed")

// New creates a Store by opening the Badger database specified by opts.
func New(opts Options) (*Store, error) {
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
	sizeFile := filepath.Join(opts.Badger.Dir, "__dbsize.bin")
	return &Store{
		db:     db,
		stopGC: cancel,
		gc:     gc,

		size:     loadSize(sizeFile),
		sizeFile: sizeFile,
	}, nil
}

// Close implements part of the blob.Store interface. It closes the underlying
// database instance and reports its result.
func (s *Store) Close(_ context.Context) error {
	if !s.db.IsClosed() {
		s.stopGC()
		s.gc.Wait()
	}
	return s.db.Close()
}

// Get implements part of blob.Store.
func (s *Store) Get(_ context.Context, key string) (data []byte, err error) {
	if s.db.IsClosed() {
		return nil, errClosed
	}
	err = s.db.View(func(txn *badger.Txn) error {
		itm, err := txn.Get([]byte(key))
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

// Put implements part of blob.Store.
func (s *Store) Put(_ context.Context, opts blob.PutOptions) error {
	if s.db.IsClosed() {
		return errClosed
	}
	key := []byte(opts.Key)
	for {
		var add bool
		err := s.db.Update(func(txn *badger.Txn) error {
			_, gerr := txn.Get(key)
			add = gerr != nil // probably
			if !opts.Replace {
				if gerr == nil {
					return blob.KeyExists(opts.Key)
				} else if gerr != badger.ErrKeyNotFound {
					return gerr
				}
			}
			return txn.Set(key, opts.Data)
		})
		if err == nil {
			if add {
				s.addSize(1)
			}
			return nil
		}
		if !errors.Is(err, badger.ErrConflict) {
			return err
		}
	}
}

// Delete implements part of blob.Store.
func (s *Store) Delete(_ context.Context, key string) error {
	if s.db.IsClosed() {
		return errClosed
	} else if key == "" {
		return blob.KeyNotFound(key) // badger cannot store empty keys
	}
	for {
		err := s.db.Update(func(txn *badger.Txn) error {
			byteKey := []byte(key)
			_, err := txn.Get(byteKey)
			if err == nil {
				return txn.Delete(byteKey)
			} else if err == badger.ErrKeyNotFound {
				return blob.KeyNotFound(key)
			}
			return err
		})
		if err == nil {
			s.addSize(-1)
			return nil
		} else if !errors.Is(err, badger.ErrConflict) {
			return err
		}
	}
}

// List implements part of blob.Store.
func (s *Store) List(ctx context.Context, start string, f func(string) error) error {
	if s.db.IsClosed() {
		return errClosed
	}
	return s.db.View(func(txn *badger.Txn) error {
		// N.B. We don't use the default here, which prefetches the values.
		it := txn.NewIterator(badger.IteratorOptions{})
		defer it.Close()

		for it.Seek([]byte(start)); it.Valid(); it.Next() {
			key := it.Item().Key()
			err := f(string(key))
			if err == blob.ErrStopListing {
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

// Len implements part of blob.Store.
func (s *Store) Len(ctx context.Context) (int64, error) {
	if s.db.IsClosed() {
		return 0, errClosed
	}

	s.μ.Lock()
	defer s.μ.Unlock()
	if s.size >= 0 {
		return s.size, nil
	}

	// Reaching here, we don't know the size.
	// Compute and store it.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	g := taskgroup.New(cancel)
	var size int64
	c := taskgroup.Gather(g.Go, func(v int64) { size += v })

	for i := range 256 {
		pfx := []byte{byte(i)}
		c.Call(func() (int64, error) {
			var size int64
			err := s.db.View(func(txn *badger.Txn) error {
				it := txn.NewIterator(badger.IteratorOptions{
					Prefix: pfx,
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
	s.size = size
	saveSize(s.sizeFile, size)
	return s.size, nil
}

func (s *Store) addSize(v int64) {
	s.μ.Lock()
	defer s.μ.Unlock()

	s.size += v
	saveSize(s.sizeFile, s.size)
}

func loadSize(path string) int64 {
	sz, err := os.ReadFile(path)
	if err != nil {
		return -1
	} else if len(sz) != 8 {
		return -1
	}
	return int64(binary.BigEndian.Uint64(sz))
}

func saveSize(path string, size int64) {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(size))
	atomicfile.WriteData(path, buf[:], 0644)
}
