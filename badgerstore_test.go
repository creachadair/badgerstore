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

package badgerstore_test

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/creachadair/badgerstore"
	"github.com/creachadair/ffs/blob"
	"github.com/creachadair/ffs/blob/storetest"
	badger "github.com/dgraph-io/badger/v4"
)

func TestStore(t *testing.T) {
	dir := t.TempDir()

	t.Logf("Test store: %s", dir)
	s, err := badgerstore.Opener(context.Background(), dir+"?auto_sync=1")
	if err != nil {
		t.Fatalf("Creating store in %q: %v", dir, err)
	}
	storetest.Run(t, s)
	if err := s.Close(context.Background()); err != nil {
		t.Errorf("Closing store: %v", err)
	}
}

func TestKeyPrefix(t *testing.T) {
	dir, err := os.MkdirTemp("", "fafoprefix")
	if err != nil {
		t.Fatal(err)
	}
	kv, err := badgerstore.New(badgerstore.Options{
		Badger:    badger.DefaultOptions(dir).WithLogger(nil),
		KeyPrefix: ":wibble:",
	})
	if err != nil {
		t.Fatalf("Creating store in %q: %v", dir, err)
	}
	storetest.Run(t, kv)
	if err := kv.Close(context.Background()); err != nil {
		t.Errorf("Closing store: %v", err)
	}
}

func TestListCancel(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	t.Logf("Test store: %s", dir)
	s, err := badgerstore.Opener(ctx, dir)
	if err != nil {
		t.Fatalf("Creating store in %q: %v", dir, err)
	}
	if err := s.Put(ctx, blob.PutOptions{
		Key:  "test key 1",
		Data: []byte("ok boomer"),
	}); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	err = s.List(ctx, "", func(string) error {
		time.Sleep(1 * time.Second)
		return nil
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Wrong error: got %v, want %v", err, context.DeadlineExceeded)
	}

	if err := s.Close(context.Background()); err != nil {
		t.Errorf("Closing store: %v", err)
	}
}
