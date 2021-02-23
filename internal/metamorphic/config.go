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
	newIterUsingClone
	newSnapshot
	readerGet
	snapshotClose
	writerApply
	writerDelete
	writerDeleteRange
	writerIngest
	writerMerge
	writerSet
	writerSingleDelete
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
		batchAbort:         2,
		batchCommit:        5,
		dbCheckpoint:       1,
		dbCompact:          0,
		dbFlush:            0,
		dbRestart:          4,
		iterClose:          5,
		iterFirst:          1,
		iterLast:           1,
		iterNext:           200,
		iterPrev:           200,
		iterSeekGE:         400,
		iterSeekLT:         400,
		iterSeekPrefixGE:   10,
		iterSetBounds:      10,
		newBatch:           5,
		newIndexedBatch:    5,
		newIter:            10,
		newIterUsingClone:  30,
		newSnapshot:        10,
		readerGet:          1,
		snapshotClose:      10,
		writerApply:        10,
		writerDelete:       100,
		writerDeleteRange:  50,
		writerIngest:       100,
		writerMerge:        100,
		writerSet:          100,
		writerSingleDelete: 50,
	},
}
