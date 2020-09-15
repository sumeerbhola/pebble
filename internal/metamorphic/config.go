// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package metamorphic

type opType int

const (
	batchAbort opType = iota
	batchCommit
	dbCheckpoint
	dbClose
	dbCompact
	dbFlush
	dbRestart
	iterClose
	iterFirst
	iterLast
	iterNext
	iterPrev
	iterSeekGE
	iterSeekLT
	iterSeekPrefixGE
	iterSetBounds
	newBatch
	newIndexedBatch
	newIter
	newSnapshot
	readerGet
	snapshotClose
	writerApply
	writerDelete
	writerDeleteRange
	writerIngest
	writerMerge
	writerSet
)

type config struct {
	// Weights for the operation mix to generate. ops[i] corresponds to the
	// weight for opType(i).
	ops []int

	// TODO(peter): unimplemented
	// keyDist        randvar.Dynamic
	// keySizeDist    randvar.Static
	// valueSizeDist  randvar.Static
	// updateFrac     float64
	// lowerBoundFrac float64
	// upperBoundFrac float64
}

var defaultConfig = config{
	// dbClose is not in this list since it is deterministically generated once, at the end of the test.
	ops: []int{
		batchAbort:        0,
		batchCommit:       5,
		dbCheckpoint:      0,
		dbCompact:         1,
		dbFlush:           2,
		dbRestart:         2,
		iterClose:         2,
		iterFirst:         100,
		iterLast:          100,
		iterNext:          100,
		iterPrev:          100,
		iterSeekGE:        10,
		iterSeekLT:        10,
		iterSeekPrefixGE:  100,
		iterSetBounds:     500,
		newBatch:          5,
		newIndexedBatch:   5,
		newIter:           10,
		newSnapshot:       1,
		readerGet:         1,
		snapshotClose:     1,
		writerApply:       10,
		writerDelete:      100,
		writerDeleteRange: 50,
		writerIngest:      1,
		writerMerge:       1,
		writerSet:         100,
	},
}
