// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package metamorphic

import (
	"bytes"
	"fmt"
	"time"

	"github.com/cockroachdb/pebble"
	"golang.org/x/exp/rand"
)

type generatorBatchData struct {
	readerIdx int
	writerIdx int
	iters     intSet
}

type generatorSnapshotData struct {
	readerIdx int
	iters     intSet
}

type generator struct {
	rng *rand.Rand

	ops []op

	// keys that have been set in the DB. Used to reuse already generated keys
	// during random key selection.
	keys [][]byte

	// The allocator for the batch/iter/reader/snapshot/writer slots when the
	// test is run.
	batchSlot    int
	iterSlot     int
	readerSlot   int
	snapshotSlot int
	writerSlot   int

	// Unordered sets of indexes into
	// metaTest.{batches,iters,readers,snapshots,writers}. The live versions will
	// have non-nil slots during execution of the operation.
	liveBatches   intSlice
	liveIters     intSlice
	liveReaders   intSlice
	liveSnapshots intSlice
	liveWriters   intSlice

	maxLiveBatches   int
	maxLiveIters     int
	maxLiveReaders   int
	maxLiveSnapshots int
	maxLiveWriters   int

	// batchIdx -> batch iters
	batches map[int]generatorBatchData
	// iterIdx -> reader iter-set
	iters map[int]intSet
	// readerIdx -> reader iter-set
	readers map[int]intSet
	// snapshotIdx -> snapshot iters
	snapshots map[int]generatorSnapshotData
}

func newGenerator() *generator {
	return &generator{
		rng:         rand.New(rand.NewSource(uint64(time.Now().UnixNano()))),
		readerSlot:  1,        // reader 0 is the DB
		writerSlot:  1,        // writer 0 is the DB
		liveReaders: []int{0}, // reader 0 is the DB
		liveWriters: []int{0}, // writer 0 is the DB
		batches:     make(map[int]generatorBatchData),
		iters:       make(map[int]intSet),
		readers:     make(map[int]intSet),
		snapshots:   make(map[int]generatorSnapshotData),
	}
}

func (g *generator) add(op op) {
	if liveBatches := len(g.liveBatches); g.maxLiveBatches < liveBatches {
		g.maxLiveBatches = liveBatches
	}
	if liveIters := len(g.liveIters); g.maxLiveIters < liveIters {
		g.maxLiveIters = liveIters
	}
	if liveReaders := len(g.liveReaders); g.maxLiveReaders < liveReaders {
		g.maxLiveReaders = liveReaders
	}
	if liveSnapshots := len(g.liveSnapshots); g.maxLiveSnapshots < liveSnapshots {
		g.maxLiveSnapshots = liveSnapshots
	}
	if liveWriters := len(g.liveWriters); g.maxLiveWriters < liveWriters {
		g.maxLiveWriters = liveWriters
	}

	g.ops = append(g.ops, op)
}

// TODO(peter): make this configurable. See config.go.
func (g *generator) randKey(newKey float64) []byte {
	if n := len(g.keys); n > 0 && g.rng.Float64() > newKey {
		return g.keys[g.rng.Intn(n)]
	}
	key := g.randValue(4, 12)
	g.keys = append(g.keys, key)
	return key
}

// TODO(peter): make this configurable. See config.go.
func (g *generator) randValue(min, max int) []byte {
	const letters = "abcdefghijklmnopqrstuvwxyz"
	const lettersLen = uint64(len(letters))
	const lettersCharsPerRand = 13 // floor(log(math.MaxUint64)/log(lettersLen))

	n := min
	if max > min {
		n += g.rng.Intn(max - min)
	}

	buf := make([]byte, n)

	var r uint64
	var q int
	for i := 0; i < len(buf); i++ {
		if q == 0 {
			r = g.rng.Uint64()
			q = lettersCharsPerRand
		}
		buf[i] = letters[r%lettersLen]
		r = r / lettersLen
		q--
	}

	return buf
}

func (g *generator) newBatch() {
	batchIdx := g.batchSlot
	g.batchSlot++
	g.liveBatches = append(g.liveBatches, batchIdx)

	writerIdx := g.writerSlot
	g.writerSlot++
	g.liveWriters = append(g.liveWriters, writerIdx)

	g.batches[batchIdx] = generatorBatchData{
		readerIdx: -1,
		writerIdx: writerIdx,
	}

	g.add(newBatchOp{
		batchIdx:  batchIdx,
		writerIdx: writerIdx,
	})
}

func (g *generator) newIndexedBatch() {
	batchIdx := g.batchSlot
	g.batchSlot++
	g.liveBatches = append(g.liveBatches, batchIdx)

	readerIdx := g.readerSlot
	g.readerSlot++
	g.liveReaders = append(g.liveReaders, readerIdx)

	writerIdx := g.writerSlot
	g.writerSlot++
	g.liveWriters = append(g.liveWriters, writerIdx)

	iters := make(intSet)
	g.batches[batchIdx] = generatorBatchData{
		readerIdx: readerIdx,
		writerIdx: writerIdx,
		iters:     iters,
	}
	g.readers[readerIdx] = iters

	g.add(newIndexedBatchOp{
		batchIdx:  batchIdx,
		readerIdx: readerIdx,
		writerIdx: writerIdx,
	})
}

func (g *generator) batchClose(batchIdx int) {
	g.liveBatches.remove(batchIdx)
	b := g.batches[batchIdx]
	delete(g.batches, batchIdx)

	if b.readerIdx >= 0 {
		g.liveReaders.remove(b.readerIdx)
		delete(g.readers, b.readerIdx)
	}
	if b.writerIdx >= 0 {
		g.liveWriters.remove(b.writerIdx)
	}
	for i := range b.iters {
		g.liveIters.remove(i)
		delete(g.iters, i)
		g.add(iterCloseOp{iterIdx: i})
	}
}

func (g *generator) batchAbort() {
	if len(g.liveBatches) == 0 {
		return
	}

	batchIdx := g.liveBatches.rand(g.rng)
	b := g.batches[batchIdx]
	g.batchClose(batchIdx)

	g.add(batchAbortOp{
		batchIdx:  batchIdx,
		readerIdx: b.readerIdx,
		writerIdx: b.writerIdx,
	})
}

func (g *generator) batchCommit() {
	if len(g.liveBatches) == 0 {
		return
	}

	batchIdx := g.liveBatches.rand(g.rng)
	b := g.batches[batchIdx]
	g.batchClose(batchIdx)

	g.add(batchCommitOp{
		batchIdx:  batchIdx,
		readerIdx: b.readerIdx,
		writerIdx: b.writerIdx,
	})
}

func (g *generator) newIter() {
	iterIdx := g.iterSlot
	g.iterSlot++
	g.liveIters = append(g.liveIters, iterIdx)

	readerIdx := g.liveReaders.rand(g.rng)
	if iters := g.readers[readerIdx]; iters != nil {
		iters[iterIdx] = struct{}{}
		g.iters[iterIdx] = iters
	}

	var lower, upper []byte
	if g.rng.Float64() <= 0.1 {
		lower = g.randKey(0.001)
	}
	if g.rng.Float64() <= 0.1 {
		upper = g.randKey(0.001)
	}
	if bytes.Compare(lower, upper) > 0 {
		lower, upper = upper, lower
	}

	g.add(newIterOp{
		readerIdx: readerIdx,
		iterIdx:   iterIdx,
		lower:     lower,
		upper:     upper,
	})
}

func (g *generator) iterClose() {
	if len(g.liveIters) == 0 {
		return
	}

	iterIdx := g.liveIters.rand(g.rng)
	g.liveIters.remove(iterIdx)
	if readerIters, ok := g.iters[iterIdx]; ok {
		delete(g.iters, iterIdx)
		delete(readerIters, iterIdx)
	}

	g.add(iterCloseOp{iterIdx: iterIdx})
}

func (g *generator) iterSetBounds() {
	if len(g.liveIters) == 0 {
		return
	}

	iterIdx := g.liveIters.rand(g.rng)
	var lower, upper []byte
	if g.rng.Float64() <= 0.5 {
		lower = g.randKey(0.001)
	}
	if g.rng.Float64() <= 0.5 {
		upper = g.randKey(0.001)
	}
	if bytes.Compare(lower, upper) > 0 {
		lower, upper = upper, lower
	}
	g.add(iterSetBoundsOp{
		iterIdx: iterIdx,
		lower:   lower,
		upper:   upper,
	})
}

func (g *generator) iterSeekGE() {
	if len(g.liveIters) == 0 {
		return
	}

	iterIdx := g.liveIters.rand(g.rng)
	g.add(iterSeekGEOp{
		iterIdx: iterIdx,
		key:     g.randKey(0.001), // 0.1% new keys
	})
}

func (g *generator) iterSeekLT() {
	if len(g.liveIters) == 0 {
		return
	}

	iterIdx := g.liveIters.rand(g.rng)
	g.add(iterSeekLTOp{
		iterIdx: iterIdx,
		key:     g.randKey(0.001), // 0.1% new keys
	})
}

func (g *generator) iterFirst() {
	if len(g.liveIters) == 0 {
		return
	}

	iterIdx := g.liveIters.rand(g.rng)
	g.add(iterFirstOp{iterIdx: iterIdx})
}

func (g *generator) iterLast() {
	if len(g.liveIters) == 0 {
		return
	}

	iterIdx := g.liveIters.rand(g.rng)
	g.add(iterLastOp{iterIdx: iterIdx})
}

func (g *generator) iterNext() {
	if len(g.liveIters) == 0 {
		return
	}

	iterIdx := g.liveIters.rand(g.rng)
	g.add(iterNextOp{iterIdx: iterIdx})
}

func (g *generator) iterPrev() {
	if len(g.liveIters) == 0 {
		return
	}

	iterIdx := g.liveIters.rand(g.rng)
	g.add(iterPrevOp{iterIdx: iterIdx})
}

func (g *generator) readerGet() {
	if len(g.liveReaders) == 0 {
		return
	}

	g.add(getOp{
		readerIdx: g.liveReaders.rand(g.rng),
		key:       g.randKey(0.001), // 0.1% new keys
	})
}

func (g *generator) newSnapshot() {
	snapIdx := g.snapshotSlot
	g.snapshotSlot++
	g.liveSnapshots = append(g.liveSnapshots, snapIdx)

	readerIdx := g.readerSlot
	g.readerSlot++
	g.liveReaders = append(g.liveReaders, readerIdx)

	iters := make(intSet)
	g.snapshots[snapIdx] = generatorSnapshotData{
		readerIdx: readerIdx,
		iters:     iters,
	}
	g.readers[readerIdx] = iters

	g.add(newSnapshotOp{
		snapIdx:   snapIdx,
		readerIdx: readerIdx,
	})
}

func (g *generator) snapshotClose() {
	if len(g.liveSnapshots) == 0 {
		return
	}

	snapIdx := g.liveSnapshots.rand(g.rng)
	g.liveSnapshots.remove(snapIdx)
	s := g.snapshots[snapIdx]
	delete(g.snapshots, snapIdx)
	g.liveReaders.remove(s.readerIdx)
	delete(g.readers, s.readerIdx)

	for i := range s.iters {
		g.liveIters.remove(i)
		delete(g.iters, i)
		g.add(iterCloseOp{iterIdx: i})
	}

	g.add(snapshotCloseOp{
		snapIdx:   snapIdx,
		readerIdx: s.readerIdx,
	})
}

func (g *generator) writerApply() {
	if len(g.liveWriters) == 0 || len(g.liveBatches) == 0 {
		return
	}

	batchIdx := g.liveBatches.rand(g.rng)
	b := g.batches[batchIdx]

	var writerIdx int
	if len(g.liveWriters) > 1 {
		for {
			writerIdx = g.liveWriters.rand(g.rng)
			if writerIdx != b.writerIdx {
				break
			}
		}
	}

	g.batchClose(batchIdx)

	g.add(applyOp{
		batchIdx:       batchIdx,
		batchReaderIdx: b.readerIdx,
		batchWriterIdx: b.writerIdx,
		writerIdx:      writerIdx,
	})
}

func (g *generator) writerDelete() {
	if len(g.liveWriters) == 0 {
		return
	}

	g.add(deleteOp{
		writerIdx: g.liveWriters.rand(g.rng),
		key:       g.randKey(0.001), // 0.1% new keys
	})
}

func (g *generator) writerDeleteRange() {
	if len(g.liveWriters) == 0 {
		return
	}

	start := g.randKey(0.001)
	end := g.randKey(0.001)
	if bytes.Compare(start, end) > 0 {
		start, end = end, start
	}

	g.add(deleteRangeOp{
		writerIdx: g.liveWriters.rand(g.rng),
		start:     start,
		end:       end,
	})
}

func (g *generator) writerIngest() {
	// TODO(peter): Convert an open batch into an sstable. Will need to expose
	// batch reader functionality in order to do this.
	panic("TODO(peter): unimplemented")
	// g.add(metaIngest{})
}

func (g *generator) writerMerge() {
	if len(g.liveWriters) == 0 {
		return
	}

	g.add(mergeOp{
		writerIdx: g.liveWriters.rand(g.rng),
		key:       g.randKey(0.2), // 20% new keys
		value:     g.randValue(0, 20),
	})
}

func (g *generator) writerSet() {
	if len(g.liveWriters) == 0 {
		return
	}

	g.add(setOp{
		writerIdx: g.liveWriters.rand(g.rng),
		key:       g.randKey(0.5), // 50% new keys
		value:     g.randValue(0, 20),
	})
}

func (g *generator) makeOps(min, max int) {
	// TODO(peter): Make the mix of operations configurable. See config.go.
	mix := []struct {
		weight    int
		generator func()
	}{
		{5, g.newBatch},
		{5, g.newIndexedBatch},
		{5, g.batchAbort},
		{5, g.batchCommit},
		{10, g.newIter},
		{10, g.iterClose},
		{10, g.iterSetBounds},
		{100, g.iterSeekGE},
		{100, g.iterSeekLT},
		{100, g.iterFirst},
		{100, g.iterLast},
		{100, g.iterNext},
		{100, g.iterPrev},
		{100, g.readerGet},
		{10, g.newSnapshot},
		{10, g.snapshotClose},
		{10, g.writerApply},
		{100, g.writerDelete},
		{100, g.writerDeleteRange},
		// TODO(peter): {100, g.writerIngest},
		{100, g.writerMerge},
		{100, g.writerSet},
	}

	// TPCC-style deck of cards randomization. Every time the end of the deck is
	// reached, we shuffle the deck.
	totalWeight := 0
	for i := range mix {
		totalWeight += mix[i].weight
	}
	deck := make([]int, 0, totalWeight)
	for i, t := range mix {
		for j := 0; j < t.weight; j++ {
			deck = append(deck, i)
		}
	}

	count := min + g.rng.Intn(max-min)
	for i := 0; i < count; i++ {
		j := i % len(deck)
		if j == 0 {
			g.rng.Shuffle(len(deck), func(i, j int) {
				deck[i], deck[j] = deck[j], deck[i]
			})
		}
		mix[deck[j]].generator()
	}

	for len(g.liveSnapshots) > 0 {
		g.snapshotClose()
	}
	for len(g.liveIters) > 0 {
		g.iterClose()
	}
}

func (g *generator) newTest() *test {
	return &test{
		ops:       g.ops,
		batches:   make([]*pebble.Batch, g.batchSlot),
		iters:     make([]*pebble.Iterator, g.iterSlot),
		readers:   make([]pebble.Reader, g.readerSlot),
		snapshots: make([]*pebble.Snapshot, g.snapshotSlot),
		writers:   make([]pebble.Writer, g.writerSlot),
	}
}

func (g *generator) String() string {
	var buf bytes.Buffer
	for _, op := range g.ops {
		fmt.Fprintf(&buf, "%s\n", op)
	}
	fmt.Fprintf(&buf, "\n")
	fmt.Fprintf(&buf, "max-live-batches:   %d\n", g.maxLiveBatches)
	fmt.Fprintf(&buf, "max-live-iters:     %d\n", g.maxLiveIters)
	fmt.Fprintf(&buf, "max-live-readers:   %d\n", g.maxLiveReaders)
	fmt.Fprintf(&buf, "max-live-snapshots: %d\n", g.maxLiveSnapshots)
	fmt.Fprintf(&buf, "max-live-writers:   %d\n", g.maxLiveWriters)
	return buf.String()
}
