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
	"errors"
	"net/url"
	"path/filepath"
	"strconv"
	"sync/atomic"

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
//	base_size        : base table size in MiB (default 2)
//	compact_on_close : do a L0 compaction on close (default true)
//	index_cache      : index cache size in MiB (default 50)
//	read_only        : open the database in read-only mode (default false)
func Opener(_ context.Context, addr string) (blob.Store, error) {
	opts, err := parseOptions(addr)
	if err != nil {
		return nil, err
	}
	return New(opts)
}

func parseOptions(addr string) (badger.Options, error) {
	u, err := url.Parse(addr)
	if err != nil {
		return badger.Options{}, err
	}
	filePath := filepath.Join(u.Host, filepath.FromSlash(u.Path))
	opts := badger.DefaultOptions(filePath).
		WithNumVersionsToKeep(1).
		WithCompactL0OnClose(parseBool(u, "compact_on_close", true)).
		WithBaseTableSize(parseInt(u, "base_size", 2) << 20).
		WithIndexCacheSize(parseInt(u, "index_cache", 50) << 20).
		WithLogger(nil).
		WithReadOnly(parseBool(u, "read_only", false))
	return opts, nil
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
	closed atomic.Bool
}

var errClosed = errors.New("database is closed")

// New creates a Store by opening the Badger database specified by opts.
func New(opts badger.Options) (*Store, error) {
	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}
	return &Store{db: db}, nil
}

// NewPath creates a Store by opening a Badger database with default options at
// the specified path.
func NewPath(path string) (*Store, error) {
	return New(badger.DefaultOptions(path).WithLogger(nil))
}

// NewPathReadOnly creates a Store around a read-only Badger instance with
// default options at the specified path.
func NewPathReadOnly(path string) (*Store, error) {
	return New(badger.DefaultOptions(path).WithLogger(nil).WithReadOnly(true))
}

// Close implements part of the blob.Store interface. It closes the underlying
// database instance and reports its result.
func (s *Store) Close(_ context.Context) error {
	if s.closed.CompareAndSwap(false, true) {
		return s.db.Close()
	}
	return nil // fine, it's already closed
}

// Get implements part of blob.Store.
func (s *Store) Get(_ context.Context, key string) (data []byte, err error) {
	if s.closed.Load() {
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
	if s.closed.Load() {
		return errClosed
	}
	key := []byte(opts.Key)
	for {
		err := s.db.Update(func(txn *badger.Txn) error {
			if !opts.Replace {
				_, err := txn.Get(key)
				if err == nil {
					return blob.KeyExists(opts.Key)
				} else if err != badger.ErrKeyNotFound {
					return err
				}
			}
			return txn.Set(key, opts.Data)
		})
		if !errors.Is(err, badger.ErrConflict) {
			return err
		}
	}
}

// Delete implements part of blob.Store.
func (s *Store) Delete(_ context.Context, key string) error {
	if s.closed.Load() {
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
		if !errors.Is(err, badger.ErrConflict) {
			return err
		}
	}
}

// List implements part of blob.Store.
func (s *Store) List(ctx context.Context, start string, f func(string) error) error {
	if s.closed.Load() {
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
	if s.closed.Load() {
		return 0, errClosed
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	g := taskgroup.New(taskgroup.Trigger(cancel))

	sizes := make([]int64, 256)
	for i := 0; i < 256; i++ {
		pfx, i := []byte{byte(i)}, i

		g.Go(func() error {
			return s.db.View(func(txn *badger.Txn) error {
				it := txn.NewIterator(badger.IteratorOptions{
					Prefix: pfx,
				})
				defer it.Close()

				for it.Rewind(); it.Valid(); it.Next() {
					select {
					case <-ctx.Done():
						return ctx.Err()
					default:
						sizes[i]++
					}
				}
				return nil
			})
		})
	}
	if err := g.Wait(); err != nil {
		return 0, err
	}
	var total int64
	for _, size := range sizes {
		total += size
	}
	return total, nil
}
