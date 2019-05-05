// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package metamorphic

import (
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
)

type test struct {
	// The list of ops to execute. The ops refer to slots in the batches, iters,
	// readers, snapshots, and writers slices.
	ops []op
	idx int
	// The DB the test is run on.
	db *pebble.DB
	// The slots for the batches, iterators, readers, snapshots, and
	// writers. These are read and written by the ops to pass state from one op
	// to another.
	batches   []*pebble.Batch
	iters     []*pebble.Iterator
	readers   []pebble.Reader
	snapshots []*pebble.Snapshot
	writers   []pebble.Writer
}

func (t *test) init(h *history) error {
	// TODO(peter): Should probably leave initialization of the DB to the
	// caller. We'll want to use on-disk databases and each run should use a
	// different directory.
	db, err := pebble.Open("", &pebble.Options{
		FS: vfs.NewMem(),
	})
	if err != nil {
		return err
	}
	h.recordf("open -> %v\n", err)

	t.db = db
	t.readers[0] = db
	t.writers[0] = db
	return nil
}

func (t *test) step(h *history) bool {
	if t.idx >= len(t.ops) {
		return false
	}
	t.ops[t.idx].run(t, h)
	t.idx++
	return true
}

func (t *test) finish(h *history) {
	db := t.db
	t.db = nil
	h.recordf("close -> %v\n", db.Close())
}
