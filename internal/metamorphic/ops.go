// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package metamorphic

import (
	"fmt"

	"github.com/cockroachdb/pebble"
)

type op interface {
	run(t *test, h *history)
	String() string
}

type applyOp struct {
	batchIdx       int
	batchReaderIdx int
	batchWriterIdx int
	writerIdx      int
	repr           []byte
}

func (o applyOp) run(t *test, h *history) {
	b := t.batches[o.batchIdx]
	w := t.writers[o.writerIdx]
	err := w.Apply(b, nil)
	h.recordf("%s -> %v\n", o, err)
	_ = b.Close()
	t.batches[o.batchIdx] = nil
	if o.batchReaderIdx >= 0 {
		t.readers[o.batchReaderIdx] = nil
	}
	if o.batchWriterIdx >= 0 {
		t.writers[o.batchWriterIdx] = nil
	}
}

func (o applyOp) String() string {
	return fmt.Sprintf("apply(b%d,w%d)", o.batchIdx, o.writerIdx)
}

type deleteOp struct {
	writerIdx int
	key       []byte
}

func (o deleteOp) run(t *test, h *history) {
	w := t.writers[o.writerIdx]
	err := w.Delete(o.key, nil)
	h.recordf("%s -> %v\n", o, err)
}

func (o deleteOp) String() string {
	return fmt.Sprintf("delete(w%d,%s)", o.writerIdx, o.key)
}

type deleteRangeOp struct {
	writerIdx int
	start     []byte
	end       []byte
}

func (o deleteRangeOp) run(t *test, h *history) {
	w := t.writers[o.writerIdx]
	err := w.DeleteRange(o.start, o.end, nil)
	h.recordf("%s -> %v\n", o, err)
}

func (o deleteRangeOp) String() string {
	return fmt.Sprintf("delete-range(w%d,%s,%s)", o.writerIdx, o.start, o.end)
}

type mergeOp struct {
	writerIdx int
	key       []byte
	value     []byte
}

func (o mergeOp) run(t *test, h *history) {
	w := t.writers[o.writerIdx]
	err := w.Merge(o.key, o.value, nil)
	h.recordf("%s -> %v\n", o, err)
}

func (o mergeOp) String() string {
	return fmt.Sprintf("merge(w%d,%s,%s)", o.writerIdx, o.key, o.value)
}

type setOp struct {
	writerIdx int
	key       []byte
	value     []byte
}

func (o setOp) run(t *test, h *history) {
	w := t.writers[o.writerIdx]
	err := w.Set(o.key, o.value, nil)
	h.recordf("%s -> %v\n", o, err)
}

func (o setOp) String() string {
	return fmt.Sprintf("set(w%d,%s,%s)", o.writerIdx, o.key, o.value)
}

type newBatchOp struct {
	batchIdx  int
	writerIdx int
}

func (o newBatchOp) String() string {
	return fmt.Sprintf("new-batch(b%d,w%d)", o.batchIdx, o.writerIdx)
}

func (o newBatchOp) run(t *test, h *history) {
	b := t.db.NewBatch()
	t.batches[o.batchIdx] = b
	t.writers[o.writerIdx] = b
	h.recordf("%s\n", o)
}

type newIndexedBatchOp struct {
	batchIdx  int
	readerIdx int
	writerIdx int
}

func (o newIndexedBatchOp) run(t *test, h *history) {
	b := t.db.NewIndexedBatch()
	t.batches[o.batchIdx] = b
	t.readers[o.readerIdx] = b
	t.writers[o.writerIdx] = b
	h.recordf("%s\n", o)
}

func (o newIndexedBatchOp) String() string {
	return fmt.Sprintf("new-indexed-batch(b%d,r%d,w%d)", o.batchIdx, o.readerIdx, o.writerIdx)
}

type batchAbortOp struct {
	batchIdx  int
	readerIdx int
	writerIdx int
}

func (o batchAbortOp) run(t *test, h *history) {
	b := t.batches[o.batchIdx]
	t.batches[o.batchIdx] = nil
	if o.readerIdx >= 0 {
		t.readers[o.readerIdx] = nil
	}
	if o.writerIdx >= 0 {
		t.writers[o.writerIdx] = nil
	}
	err := b.Close()
	h.recordf("%s -> %v\n", o, err)
}

func (o batchAbortOp) String() string {
	return fmt.Sprintf("batch-abort(b%d,r%d,w%d)", o.batchIdx, o.readerIdx, o.writerIdx)
}

type batchCommitOp struct {
	batchIdx  int
	readerIdx int
	writerIdx int
}

func (o batchCommitOp) run(t *test, h *history) {
	b := t.batches[o.batchIdx]
	t.batches[o.batchIdx] = nil
	if o.readerIdx >= 0 {
		t.readers[o.readerIdx] = nil
	}
	if o.writerIdx >= 0 {
		t.writers[o.writerIdx] = nil
	}
	err := b.Commit(nil)
	h.recordf("%s -> %v\n", o, err)
}

func (o batchCommitOp) String() string {
	return fmt.Sprintf("batch-commit(b%d,r%d,w%d)", o.batchIdx, o.readerIdx, o.writerIdx)
}

type ingestOp struct {
}

func (o ingestOp) run(t *test, h *history) {
	panic("TODO(peter): unimplemented")
}

func (o ingestOp) String() string {
	return fmt.Sprintf("ingest")
}

type getOp struct {
	readerIdx int
	key       []byte
}

func (o getOp) run(t *test, h *history) {
	r := t.readers[o.readerIdx]
	val, err := r.Get(o.key)
	h.recordf("%s -> [%s] %v\n", o, val, err)
}

func (o getOp) String() string {
	return fmt.Sprintf("get(r%d,%s)", o.readerIdx, o.key)
}

type newIterOp struct {
	readerIdx int
	iterIdx   int
	lower     []byte
	upper     []byte
}

func (o newIterOp) run(t *test, h *history) {
	r := t.readers[o.readerIdx]
	t.iters[o.iterIdx] = r.NewIter(&pebble.IterOptions{
		LowerBound: o.lower,
		UpperBound: o.upper,
	})
	h.recordf("%s\n", o)
}

func (o newIterOp) String() string {
	return fmt.Sprintf("new-iter(r%d,i%d,[%s-%s])",
		o.readerIdx, o.iterIdx, o.lower, o.upper)
}

type iterCloseOp struct {
	iterIdx int
}

func (o iterCloseOp) run(t *test, h *history) {
	i := t.iters[o.iterIdx]
	err := i.Close()
	h.recordf("%s -> %v\n", o, err)
}

func (o iterCloseOp) String() string {
	return fmt.Sprintf("iter-close(i%d)", o.iterIdx)
}

type iterSetBoundsOp struct {
	iterIdx int
	lower   []byte
	upper   []byte
}

func (o iterSetBoundsOp) run(t *test, h *history) {
	i := t.iters[o.iterIdx]
	i.SetBounds(o.lower, o.upper)
	h.recordf("%s -> %v\n", o, i.Error())
}

func (o iterSetBoundsOp) String() string {
	return fmt.Sprintf("iter-set-bounds(i%d,[%s-%s])", o.iterIdx, o.lower, o.upper)
}

type iterSeekGEOp struct {
	iterIdx int
	key     []byte
}

func (o iterSeekGEOp) run(t *test, h *history) {
	i := t.iters[o.iterIdx]
	valid := i.SeekGE(o.key)
	if valid {
		h.recordf("%s -> [%t,%s,%s] %v\n", o, valid, i.Key(), i.Value(), i.Error())
	} else {
		h.recordf("%s -> [%t] %v\n", o, valid, i.Error())
	}
}

func (o iterSeekGEOp) String() string {
	return fmt.Sprintf("iter-seek-ge(i%d,%s)", o.iterIdx, o.key)
}

type iterSeekLTOp struct {
	iterIdx int
	key     []byte
}

func (o iterSeekLTOp) run(t *test, h *history) {
	i := t.iters[o.iterIdx]
	valid := i.SeekLT(o.key)
	if valid {
		h.recordf("%s -> [%t,%s,%s] %v\n", o, valid, i.Key(), i.Value(), i.Error())
	} else {
		h.recordf("%s -> [%t] %v\n", o, valid, i.Error())
	}
}

func (o iterSeekLTOp) String() string {
	return fmt.Sprintf("iter-seek-lt(i%d,%s)", o.iterIdx, o.key)
}

type iterFirstOp struct {
	iterIdx int
}

func (o iterFirstOp) run(t *test, h *history) {
	i := t.iters[o.iterIdx]
	valid := i.First()
	if valid {
		h.recordf("%s -> [%t,%s,%s] %v\n", o, valid, i.Key(), i.Value(), i.Error())
	} else {
		h.recordf("%s -> [%t] %v\n", o, valid, i.Error())
	}
}

func (o iterFirstOp) String() string {
	return fmt.Sprintf("iter-first(i%d)", o.iterIdx)
}

type iterLastOp struct {
	iterIdx int
}

func (o iterLastOp) run(t *test, h *history) {
	i := t.iters[o.iterIdx]
	valid := i.Last()
	if valid {
		h.recordf("%s -> [%t,%s,%s] %v\n", o, valid, i.Key(), i.Value(), i.Error())
	} else {
		h.recordf("%s -> [%t] %v\n", o, valid, i.Error())
	}
}

func (o iterLastOp) String() string {
	return fmt.Sprintf("iter-last(i%d)", o.iterIdx)
}

type iterNextOp struct {
	iterIdx int
}

func (o iterNextOp) run(t *test, h *history) {
	i := t.iters[o.iterIdx]
	valid := i.Next()
	if valid {
		h.recordf("%s -> [%t,%s,%s] %v\n", o, valid, i.Key(), i.Value(), i.Error())
	} else {
		h.recordf("%s -> [%t] %v\n", o, valid, i.Error())
	}
}

func (o iterNextOp) String() string {
	return fmt.Sprintf("iter-next(i%d)", o.iterIdx)
}

type iterPrevOp struct {
	iterIdx int
}

func (o iterPrevOp) run(t *test, h *history) {
	i := t.iters[o.iterIdx]
	valid := i.Prev()
	if valid {
		h.recordf("%s -> [%t,%s,%s] %v\n", o, valid, i.Key(), i.Value(), i.Error())
	} else {
		h.recordf("%s -> [%t] %v\n", o, valid, i.Error())
	}
}

func (o iterPrevOp) String() string {
	return fmt.Sprintf("iter-prev(i%d)", o.iterIdx)
}

type newSnapshotOp struct {
	snapIdx   int
	readerIdx int
}

func (o newSnapshotOp) run(t *test, h *history) {
	s := t.db.NewSnapshot()
	t.snapshots[o.snapIdx] = s
	t.readers[o.readerIdx] = s
	h.recordf("%s\n", o)
}

func (o newSnapshotOp) String() string {
	return fmt.Sprintf("new-snapshot(s%d,r%d)", o.snapIdx, o.readerIdx)
}

type snapshotCloseOp struct {
	snapIdx   int
	readerIdx int
}

func (o snapshotCloseOp) run(t *test, h *history) {
	s := t.snapshots[o.snapIdx]
	t.snapshots[o.snapIdx] = nil
	t.readers[o.readerIdx] = nil
	err := s.Close()
	h.recordf("%s -> %v\n", o, err)
}

func (o snapshotCloseOp) String() string {
	return fmt.Sprintf("snapshot-close(s%d)", o.snapIdx)
}
