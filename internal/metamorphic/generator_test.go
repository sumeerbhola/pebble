// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package metamorphic

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGenerator(t *testing.T) {
	g := newGenerator()

	g.newBatch()
	g.newBatch()
	g.newBatch()
	require.EqualValues(t, 3, len(g.liveBatches))
	require.EqualValues(t, 3, len(g.batches))

	g.newIndexedBatch()
	g.newIndexedBatch()
	g.newIndexedBatch()
	require.EqualValues(t, 6, len(g.liveBatches))
	require.EqualValues(t, 6, len(g.batches))
	require.EqualValues(t, 3, len(g.readers))

	g.newIter()
	g.newIter()
	g.newIter()
	require.EqualValues(t, 3, len(g.liveIters))

	g.batchAbort()
	g.batchAbort()
	g.batchAbort()
	g.batchAbort()
	g.batchAbort()
	g.batchAbort()

	require.EqualValues(t, 0, len(g.liveBatches))
	require.EqualValues(t, 0, len(g.batches))
	require.EqualValues(t, 0, len(g.iters))
	require.EqualValues(t, 1, len(g.liveReaders))
	require.EqualValues(t, 0, len(g.readers))
	require.EqualValues(t, 0, len(g.liveSnapshots))
	require.EqualValues(t, 0, len(g.snapshots))
	require.EqualValues(t, 1, len(g.liveWriters))

	g.iterClose()
	g.iterClose()
	g.iterClose()
	require.EqualValues(t, 0, len(g.liveIters))

	if testing.Verbose() {
		t.Logf("%s\n", g)
	}

	g = newGenerator()

	g.newSnapshot()
	g.newSnapshot()
	g.newSnapshot()
	require.EqualValues(t, 3, len(g.liveSnapshots))
	require.EqualValues(t, 3, len(g.snapshots))

	g.newIter()
	g.newIter()
	g.newIter()
	g.snapshotClose()
	g.snapshotClose()
	g.snapshotClose()

	require.EqualValues(t, 0, len(g.liveBatches))
	require.EqualValues(t, 0, len(g.batches))
	require.EqualValues(t, 0, len(g.iters))
	require.EqualValues(t, 1, len(g.liveReaders))
	require.EqualValues(t, 0, len(g.readers))
	require.EqualValues(t, 0, len(g.liveSnapshots))
	require.EqualValues(t, 0, len(g.snapshots))
	require.EqualValues(t, 1, len(g.liveWriters))

	g.iterClose()
	g.iterClose()
	g.iterClose()
	require.EqualValues(t, 0, len(g.liveIters))

	if testing.Verbose() {
		t.Logf("%s\n", g)
	}

	g = newGenerator()

	g.newIndexedBatch()
	g.newIndexedBatch()
	g.newIndexedBatch()
	g.newIter()
	g.newIter()
	g.newIter()
	g.writerApply()
	g.writerApply()
	g.writerApply()
	g.iterClose()
	g.iterClose()
	g.iterClose()

	require.EqualValues(t, 0, len(g.liveBatches))
	require.EqualValues(t, 0, len(g.batches))
	require.EqualValues(t, 0, len(g.liveIters))
	require.EqualValues(t, 0, len(g.iters))
	require.EqualValues(t, 1, len(g.liveReaders))
	require.EqualValues(t, 0, len(g.readers))
	require.EqualValues(t, 0, len(g.liveSnapshots))
	require.EqualValues(t, 0, len(g.snapshots))
	require.EqualValues(t, 1, len(g.liveWriters))

	if testing.Verbose() {
		t.Logf("%s\n", g)
	}
}

func TestGeneratorRandom(t *testing.T) {
	g := newGenerator()
	g.makeOps(5000, 10000)
	if testing.Verbose() {
		t.Logf("%s", g)
	}
}
