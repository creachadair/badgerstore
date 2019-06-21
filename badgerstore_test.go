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
	"flag"
	"io/ioutil"
	"os"
	"testing"

	"github.com/creachadair/badgerstore"
	"github.com/creachadair/ffs/blob/storetest"
	"github.com/dgraph-io/badger/v2"
)

var keepOutput = flag.Bool("keep", false, "Keep test output after running")

func TestStore(t *testing.T) {
	dir, err := ioutil.TempDir("", "filestore")
	if err != nil {
		t.Fatalf("Creating temp directory: %v", err)
	}
	t.Logf("Test store: %s", dir)
	if !*keepOutput {
		defer os.RemoveAll(dir) // best effort cleanup
	}

	opts := badger.DefaultOptions
	opts.Dir = dir
	opts.ValueDir = dir
	s, err := badgerstore.New(opts)
	if err != nil {
		t.Fatalf("Creating store in %q: %v", dir, err)
	}
	storetest.Run(t, s)
	if err := s.Close(); err != nil {
		t.Errorf("Closing store: %v", err)
	}
}