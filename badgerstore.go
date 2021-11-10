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

	"github.com/creachadair/ffs/blob"
	"github.com/creachadair/taskgroup"
	badger "github.com/dgraph-io/badger/v3"
)

// Opener constructs a filestore from an address comprising a URL, for use with
// the store package. The host and path of the URL give the path of the
// database directory. Optional query parameters include:
//
//   read_only   : open the database in read-only mode
//
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
	opts := badger.DefaultOptions(filePath).WithLogger(nil)
	_, ro := u.Query()["read_only"]
	opts.ReadOnly = ro
	return opts, nil
}

// Store implements the blob.Store interface using a Badger key-value store.
type Store struct {
	db *badger.DB
}

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

// Close implements the io.Closer interface. It closes the underlying database
// instance and reports its result.
func (s *Store) Close() error { return s.db.Close() }

// Get implements part of blob.Store.
func (s *Store) Get(_ context.Context, key string) (data []byte, err error) {
	err = s.db.View(func(txn *badger.Txn) error {
		itm, err := txn.Get([]byte(key))
		if err == nil {
			data, err = itm.ValueCopy(data)
		}
		return err
	})
	if err == badger.ErrKeyNotFound {
		err = blob.KeyNotFound(key)
	}
	return
}

// Put implements part of blob.Store.
func (s *Store) Put(_ context.Context, opts blob.PutOptions) error {
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

// Size implements part of blob.Store.
func (s *Store) Size(_ context.Context, key string) (size int64, err error) {
	if key == "" {
		return 0, blob.KeyNotFound(key) // badger cannot store empty keys
	}
	err = s.db.View(func(txn *badger.Txn) error {
		itm, err := txn.Get([]byte(key))
		if err == nil {
			size = itm.ValueSize()
		}
		return err
	})
	if err == badger.ErrKeyNotFound {
		err = blob.KeyNotFound(key)
	}
	return
}

// Delete implements part of blob.Store.
func (s *Store) Delete(_ context.Context, key string) error {
	if key == "" {
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
func (s *Store) List(_ context.Context, start string, f func(string) error) error {
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
			}
		}
		return nil
	})
}

// Len implements part of blob.Store.
func (s *Store) Len(ctx context.Context) (int64, error) {
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
