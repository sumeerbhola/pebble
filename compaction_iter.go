// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"io"
	"sort"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/bytealloc"
	"github.com/cockroachdb/pebble/internal/rangedel"
)

// compactionIter provides a forward-only iterator that encapsulates the logic
// for collapsing entries during compaction. It wraps an internal iterator and
// collapses entries that are no longer necessary because they are shadowed by
// newer entries. The simplest example of this is when the internal iterator
// contains two keys: a.PUT.2 and a.PUT.1. Instead of returning both entries,
// compactionIter collapses the second entry because it is no longer
// necessary. The high-level structure for compactionIter is to iterate over
// its internal iterator and output 1 entry for every user-key. There are four
// complications to this story.
//
// 1. Eliding Deletion Tombstones
//
// Consider the entries a.DEL.2 and a.PUT.1. These entries collapse to
// a.DEL.2. Do we have to output the entry a.DEL.2? Only if a.DEL.2 possibly
// shadows an entry at a lower level. If we're compacting to the base-level in
// the LSM tree then a.DEL.2 is definitely not shadowing an entry at a lower
// level and can be elided.
//
// We can do slightly better than only eliding deletion tombstones at the base
// level by observing that we can elide a deletion tombstone if there are no
// sstables that contain the entry's key. This check is performed by
// elideTombstone.
//
// 2. Merges
//
// The MERGE operation merges the value for an entry with the existing value
// for an entry. The logical value of an entry can be composed of a series of
// merge operations. When compactionIter sees a MERGE, it scans forward in its
// internal iterator collapsing MERGE operations for the same key until it
// encounters a SET or DELETE operation. For example, the keys a.MERGE.4,
// a.MERGE.3, a.MERGE.2 will be collapsed to a.MERGE.4 and the values will be
// merged using the specified Merger.
//
// An interesting case here occurs when MERGE is combined with SET. Consider
// the entries a.MERGE.3 and a.SET.2. The collapsed key will be a.SET.3. The
// reason that the kind is changed to SET is because the SET operation acts as
// a barrier preventing further merging. This can be seen better in the
// scenario a.MERGE.3, a.SET.2, a.MERGE.1. The entry a.MERGE.1 may be at lower
// (older) level and not involved in the compaction. If the compaction of
// a.MERGE.3 and a.SET.2 produced a.MERGE.3, a subsequent compaction with
// a.MERGE.1 would merge the values together incorrectly.
//
// 3. Snapshots
//
// Snapshots are lightweight point-in-time views of the DB state. At its core,
// a snapshot is a sequence number along with a guarantee from Pebble that it
// will maintain the view of the database at that sequence number. Part of this
// guarantee is relatively straightforward to achieve. When reading from the
// database Pebble will ignore sequence numbers that are larger than the
// snapshot sequence number. The primary complexity with snapshots occurs
// during compaction: the collapsing of entries that are shadowed by newer
// entries is at odds with the guarantee that Pebble will maintain the view of
// the database at the snapshot sequence number. Rather than collapsing entries
// up to the next user key, compactionIter can only collapse entries up to the
// next snapshot boundary. That is, every snapshot boundary potentially causes
// another entry for the same user-key to be emitted. Another way to view this
// is that snapshots define stripes and entries are collapsed within stripes,
// but not across stripes. Consider the following scenario:
//
//   a.PUT.9
//   a.DEL.8
//   a.PUT.7
//   a.DEL.6
//   a.PUT.5
//
// In the absence of snapshots these entries would be collapsed to
// a.PUT.9. What if there is a snapshot at sequence number 7? The entries can
// be divided into two stripes and collapsed within the stripes:
//
//   a.PUT.9        a.PUT.9
//   a.DEL.8  --->
//   a.PUT.7
//   --             --
//   a.DEL.6  --->  a.DEL.6
//   a.PUT.5
//
// All of the rules described earlier still apply, but they are confined to
// operate within a snapshot stripe. Snapshots only affect compaction when the
// snapshot sequence number lies within the range of sequence numbers being
// compacted. In the above example, a snapshot at sequence number 10 or at
// sequence number 5 would not have any effect.
//
// 4. Range Deletions
//
// Range deletions provide the ability to delete all of the keys (and values)
// in a contiguous range. Range deletions are stored indexed by their start
// key. The end key of the range is stored in the value. In order to support
// lookup of the range deletions which overlap with a particular key, the range
// deletion tombstones need to be fragmented whenever they overlap. This
// fragmentation is performed by rangedel.Fragmenter. The fragments are then
// subject to the rules for snapshots. For example, consider the two range
// tombstones [a,e)#1 and [c,g)#2:
//
//   2:     c-------g
//   1: a-------e
//
// These tombstones will be fragmented into:
//
//   2:     c---e---g
//   1: a---c---e
//
// Do we output the fragment [c,e)#1? Since it is covered by [c-e]#2 the answer
// depends on whether it is in a new snapshot stripe.
//
// In addition to the fragmentation of range tombstones, compaction also needs
// to take the range tombstones into consideration when outputting normal
// keys. Just as with point deletions, a range deletion covering an entry can
// cause the entry to be elided.
type compactionIter struct {
	cmp   Compare
	merge Merge
	iter  internalIterator
	err   error
	// `key.UserKey` is set to `keyBuf` caused by saving `i.iterKey.UserKey`
	// and `key.Trailer` is set to `i.iterKey.Trailer`. This is the
	// case on return from all public methods -- these methods return `key`.
	// Additionally, it is the internal state when the code is moving to the
	// next key so it can determine whether the user key has changed from
	// the previous key.
	key         InternalKey
	value       []byte
	valueCloser io.Closer
	// Temporary buffer used for storing the previous user key in order to
	// determine when iteration has advanced to a new user key and thus a new
	// snapshot stripe.
	keyBuf []byte
	// Is the current entry valid?
	valid     bool
	iterKey   *InternalKey
	iterValue []byte
	// `skip` indicates whether the remaining skippable entries in the current
	// snapshot stripe should be skipped or processed. An example of a non-
	// skippable entry is a range tombstone as we need to return it from the
	// `compactionIter`, even if a key covering its start key has already been
	// seen in the same stripe. `skip` has no effect when `pos == IterPosNext`.
	skip bool
	// `pos` indicates the iterator position at the top of `Next()`. Its type's
	// (`IterPos`) values take on the following meanings in the context of
	// `compactionIter`.
	//
	// - `iterPosCur`: the iterator is at the last key returned.
	// - `iterPosNext`: the iterator has already been advanced to the next
	//   candidate key. For example, this happens when processing merge operands,
	//   where we advance the iterator all the way into the next stripe or next
	//   user key to ensure we've seen all mergeable operands.
	// - `iterPosPrev`: this is invalid as compactionIter is forward-only.
	pos IterPos
	// The index of the snapshot for the current key within the snapshots slice.
	curSnapshotIdx    int
	curSnapshotSeqNum uint64
	// The snapshot sequence numbers that need to be maintained. These sequence
	// numbers define the snapshot stripes (see the Snapshots description
	// above). The sequence numbers are in ascending order.
	snapshots []uint64
	// Reference to the range deletion tombstone fragmenter (e.g.,
	// `compaction.rangeDelFrag`).
	rangeDelFrag *rangedel.Fragmenter
	// The fragmented tombstones.
	tombstones []rangedel.Tombstone
	// Byte allocator for the tombstone keys.
	alloc               bytealloc.A
	allowZeroSeqNum     bool
	elideTombstone      func(key []byte) bool
	elideRangeTombstone func(start, end []byte) bool
}

func newCompactionIter(
	cmp Compare,
	formatKey base.FormatKey,
	merge Merge,
	iter internalIterator,
	snapshots []uint64,
	rangeDelFrag *rangedel.Fragmenter,
	allowZeroSeqNum bool,
	elideTombstone func(key []byte) bool,
	elideRangeTombstone func(start, end []byte) bool,
) *compactionIter {
	i := &compactionIter{
		cmp:                 cmp,
		merge:               merge,
		iter:                iter,
		snapshots:           snapshots,
		rangeDelFrag:        rangeDelFrag,
		allowZeroSeqNum:     allowZeroSeqNum,
		elideTombstone:      elideTombstone,
		elideRangeTombstone: elideRangeTombstone,
	}
	i.rangeDelFrag.Cmp = cmp
	i.rangeDelFrag.Format = formatKey
	i.rangeDelFrag.Emit = i.emitRangeDelChunk
	return i
}

func (i *compactionIter) First() (*InternalKey, []byte) {
	if i.err != nil {
		return nil, nil
	}
	i.iterKey, i.iterValue = i.iter.First()
	if i.iterKey != nil {
		i.curSnapshotIdx, i.curSnapshotSeqNum = snapshotIndex(i.iterKey.SeqNum(), i.snapshots)
	}
	i.pos = IterPosNext
	return i.Next()
}

func (i *compactionIter) Next() (*InternalKey, []byte) {
	if i.err != nil {
		return nil, nil
	}

	// Close the closer for the current value if one was open.
	if i.valueCloser != nil {
		i.err = i.valueCloser.Close()
		i.valueCloser = nil
		if i.err != nil {
			return nil, nil
		}
	}

	// Prior to this call to `Next()` we are in one of three situations with
	// respect to `iterKey` and related state:
	//
	// - `!skip && pos == iterPosNext`: `iterKey` is already at the next key.
	// - `!skip && pos == iterPosCur`: We are at the key that has been returned.
	//   To move forward we advance by one key, even if that lands us in the same
	//   snapshot stripe.
	// - `skip && pos == iterPosCur`: We are at the key that has been returned.
	//   To move forward we skip skippable entries in the stripe.
	if i.pos == IterPosCurForward {
		if i.skip {
			i.skipInStripe()
		} else {
			i.nextInStripe()
		}
	}

	i.pos = IterPosCurForward
	i.valid = false
	for i.iterKey != nil {
		if i.iterKey.Kind() == InternalKeyKindRangeDelete {
			// Return the range tombstone so the compaction can use it for
			// file truncation and add it to the fragmenter. We do not set `skip`
			// to true before returning as there may be a forthcoming point key
			// with the same user key and sequence number. Such a point key must be
			// visible (i.e., not skipped over) since we promise point keys are
			// not deleted by range tombstones at the same sequence number.
			//
			// Although, note that `skip` may already be true before reaching here
			// due to an earlier key in the stripe. Then it is fine to leave it set
			// to true, as the earlier key must have had a higher sequence number.
			i.saveKey()
			i.value = i.iterValue
			i.valid = true
			return &i.key, i.value
		}

		if i.rangeDelFrag.Deleted(*i.iterKey, i.curSnapshotSeqNum) {
			i.saveKey()
			i.skipInStripe()
			continue
		}

		switch i.iterKey.Kind() {
		case InternalKeyKindDelete, InternalKeyKindSingleDelete:
			// If we're at the last snapshot stripe and the tombstone can be elided
			// skip skippable keys in the same stripe.
			if i.curSnapshotIdx == 0 && i.elideTombstone(i.iterKey.UserKey) {
				i.saveKey()
				i.skipInStripe()
				continue
			}

			switch i.iterKey.Kind() {
			case InternalKeyKindDelete:
				i.saveKey()
				i.value = i.iterValue
				i.valid = true
				i.skip = true
				return &i.key, i.value

			case InternalKeyKindSingleDelete:
				if i.singleDeleteNext() {
					return &i.key, i.value
				}

				continue
			}

		case InternalKeyKindSet:
			i.saveKey()
			i.value = i.iterValue
			i.valid = true
			i.skip = true
			i.maybeZeroSeqnum(i.curSnapshotIdx)
			return &i.key, i.value

		case InternalKeyKindMerge:
			// Record the snapshot index before mergeNext as merging
			// advances the iterator, adjusting curSnapshotIdx.
			origSnapshotIdx := i.curSnapshotIdx
			var valueMerger ValueMerger
			valueMerger, i.err = i.merge(i.iterKey.UserKey, i.iterValue)
			var change stripeChangeType
			if i.err == nil {
				change = i.mergeNext(valueMerger)
			}
			if i.err == nil {
				// includesBase is true whenever we've transformed the MERGE record
				// into a SET.
				includesBase := i.key.Kind() == InternalKeyKindSet
				i.value, i.valueCloser, i.err = valueMerger.Finish(includesBase)
			}
			if i.err == nil {
				// A non-skippable entry does not necessarily cover later merge
				// operands, so we must not zero the current merge result's seqnum.
				//
				// For example, suppose the forthcoming two keys are a range
				// tombstone, `[a, b)#3`, and a merge operand, `a#3`. Recall that
				// range tombstones do not cover point keys at the same seqnum, so
				// `a#3` is not deleted. The range tombstone will be seen first due
				// to its larger value type. Since it is a non-skippable key, the
				// current merge will not include `a#3`. If we zeroed the current
				// merge result's seqnum, then it would conflict with the upcoming
				// merge including `a#3`, whose seqnum will also be zeroed.
				if change != sameStripeNonSkippable {
					i.maybeZeroSeqnum(origSnapshotIdx)
				}
				return &i.key, i.value
			}
			if i.err != nil {
				i.err = base.MarkCorruptionError(i.err)
			}
			return nil, nil

		default:
			i.err = base.CorruptionErrorf("invalid internal key kind: %d", errors.Safe(i.iterKey.Kind()))
			return nil, nil
		}
	}

	return nil, nil
}

// snapshotIndex returns the index of the first sequence number in snapshots
// which is greater than or equal to seq.
func snapshotIndex(seq uint64, snapshots []uint64) (int, uint64) {
	index := sort.Search(len(snapshots), func(i int) bool {
		return snapshots[i] > seq
	})
	if index >= len(snapshots) {
		return index, InternalKeySeqNumMax
	}
	return index, snapshots[index]
}

// skipInStripe skips over skippable keys in the same stripe and user key.
func (i *compactionIter) skipInStripe() {
	i.skip = true
	var change stripeChangeType
	for {
		change = i.nextInStripe()
		if change == sameStripeNonSkippable || change == newStripe {
			break
		}
	}
	// Reset skip if we landed outside the original stripe. Otherwise, we landed
	// in the same stripe on a non-skippable key. In that case we should preserve
	// `i.skip == true` such that later keys in the stripe will continue to be
	// skipped.
	if change == newStripe {
		i.skip = false
	}
}

func (i *compactionIter) iterNext() bool {
	i.iterKey, i.iterValue = i.iter.Next()
	// We should never see a range delete sentinel in the compaction input.
	if i.iterKey != nil && i.iterKey.Trailer == InternalKeyRangeDeleteSentinel {
		panic("pebble: unexpected range delete sentinel in compaction input")
	}
	return i.iterKey != nil
}

// stripeChangeType indicates how the snapshot stripe changed relative to the previous
// key. If no change, it also indicates whether the current entry is skippable.
type stripeChangeType int

const (
	newStripe stripeChangeType = iota
	sameStripeSkippable
	sameStripeNonSkippable
)

// nextInStripe advances the iterator and returns one of the above const ints
// indicating how its state changed.
func (i *compactionIter) nextInStripe() stripeChangeType {
	if !i.iterNext() {
		return newStripe
	}
	key := i.iterKey
	if i.cmp(i.key.UserKey, key.UserKey) != 0 {
		i.curSnapshotIdx, i.curSnapshotSeqNum = snapshotIndex(key.SeqNum(), i.snapshots)
		return newStripe
	}
	origSnapshotIdx := i.curSnapshotIdx
	i.curSnapshotIdx, i.curSnapshotSeqNum = snapshotIndex(key.SeqNum(), i.snapshots)
	switch key.Kind() {
	case InternalKeyKindRangeDelete:
		// Range tombstones need to be exposed by the compactionIter to the upper level
		// `compaction` object, so return them regardless of whether they are in the same
		// snapshot stripe.
		if i.curSnapshotIdx == origSnapshotIdx {
			return sameStripeNonSkippable
		}
		return newStripe
	case InternalKeyKindInvalid:
		if i.curSnapshotIdx == origSnapshotIdx {
			return sameStripeNonSkippable
		}
		return newStripe
	}
	if i.curSnapshotIdx == origSnapshotIdx {
		return sameStripeSkippable
	}
	return newStripe
}

func (i *compactionIter) mergeNext(valueMerger ValueMerger) stripeChangeType {
	// Save the current key.
	i.saveKey()
	i.valid = true

	// Loop looking for older values in the current snapshot stripe and merge
	// them.
	for {
		if change := i.nextInStripe(); change == sameStripeNonSkippable || change == newStripe {
			i.pos = IterPosNext
			return change
		}
		key := i.iterKey
		switch key.Kind() {
		case InternalKeyKindDelete:
			// We've hit a deletion tombstone. Return everything up to this point and
			// then skip entries until the next snapshot stripe. We change the kind
			// of the result key to a Set so that it shadows keys in lower
			// levels. That is, MERGE+DEL -> SET.
			i.key.SetKind(InternalKeyKindSet)
			i.skip = true
			return sameStripeSkippable

		case InternalKeyKindSet:
			if i.rangeDelFrag.Deleted(*key, i.curSnapshotSeqNum) {
				// We change the kind of the result key to a Set so that it shadows
				// keys in lower levels. That is, MERGE+RANGEDEL -> SET. This isn't
				// strictly necessary, but provides consistency with the behavior of
				// MERGE+DEL.
				i.key.SetKind(InternalKeyKindSet)
				i.skip = true
				return sameStripeSkippable
			}

			// We've hit a Set value. Merge with the existing value and return. We
			// change the kind of the resulting key to a Set so that it shadows keys
			// in lower levels. That is, MERGE+SET -> SET.
			i.err = valueMerger.MergeOlder(i.iterValue)
			if i.err != nil {
				return sameStripeSkippable
			}
			i.key.SetKind(InternalKeyKindSet)
			i.skip = true
			return sameStripeSkippable

		case InternalKeyKindMerge:
			if i.rangeDelFrag.Deleted(*key, i.curSnapshotSeqNum) {
				// We change the kind of the result key to a Set so that it shadows
				// keys in lower levels. That is, MERGE+RANGEDEL -> SET. This isn't
				// strictly necessary, but provides consistency with the behavior of
				// MERGE+DEL.
				i.key.SetKind(InternalKeyKindSet)
				i.skip = true
				return sameStripeSkippable
			}

			// We've hit another Merge value. Merge with the existing value and
			// continue looping.
			i.err = valueMerger.MergeOlder(i.iterValue)
			if i.err != nil {
				return sameStripeSkippable
			}

		default:
			i.err = base.CorruptionErrorf("invalid internal key kind: %d", errors.Safe(i.iterKey.Kind()))
			return sameStripeSkippable
		}
	}
}

func (i *compactionIter) singleDeleteNext() bool {
	// Save the current key.
	i.saveKey()
	i.value = i.iterValue
	i.valid = true

	// Loop until finds a key to be passed to the next level.
	for {
		if change := i.nextInStripe(); change == sameStripeNonSkippable || change == newStripe {
			i.pos = IterPosNext
			return true
		}

		key := i.iterKey
		switch key.Kind() {
		case InternalKeyKindDelete, InternalKeyKindMerge:
			// We've hit a Delete or Merge, transform the SingleDelete into a full Delete.
			i.key.SetKind(InternalKeyKindDelete)
			i.skip = true
			return true

		case InternalKeyKindSet:
			i.nextInStripe()
			i.valid = false
			return false

		case InternalKeyKindSingleDelete:
			continue

		default:
			i.err = base.CorruptionErrorf("invalid internal key kind: %d", errors.Safe(i.iterKey.Kind()))
			return false
		}
	}
}

func (i *compactionIter) saveKey() {
	i.keyBuf = append(i.keyBuf[:0], i.iterKey.UserKey...)
	i.key.UserKey = i.keyBuf
	i.key.Trailer = i.iterKey.Trailer
}

func (i *compactionIter) cloneKey(key InternalKey) InternalKey {
	i.alloc, key.UserKey = i.alloc.Copy(key.UserKey)
	return key
}

func (i *compactionIter) Key() InternalKey {
	return i.key
}

func (i *compactionIter) Value() []byte {
	return i.value
}

func (i *compactionIter) Valid() bool {
	return i.valid
}

func (i *compactionIter) Error() error {
	return i.err
}

func (i *compactionIter) Close() error {
	err := i.iter.Close()
	if i.err == nil {
		i.err = err
	}

	// Close the closer for the current value if one was open.
	if i.valueCloser != nil {
		i.err = firstError(i.err, i.valueCloser.Close())
		i.valueCloser = nil
	}

	return i.err
}

// Tombstones returns a list of pending range tombstones in the fragmenter
// up to the specified key, or all pending range tombstones if key = nil.
// exclude specifies if the specified key is exclusive or inclusive.
// When exclude = true, all returned range tombstones are truncated to the
// specified key.
func (i *compactionIter) Tombstones(key []byte, exclude bool) []rangedel.Tombstone {
	switch {
	case key == nil:
		i.rangeDelFrag.Finish()
	case exclude:
		// The specified end key is exclusive; no versions of the specified
		// user key (including range tombstones covering that key) should
		// be flushed yet.
		i.rangeDelFrag.TruncateAndFlushTo(key)
	default:
		i.rangeDelFrag.FlushTo(key)
	}
	tombstones := i.tombstones
	i.tombstones = nil
	return tombstones
}

func (i *compactionIter) emitRangeDelChunk(fragmented []rangedel.Tombstone) {
	// Apply the snapshot stripe rules, keeping only the latest tombstone for
	// each snapshot stripe.
	currentIdx := -1
	for _, v := range fragmented {
		idx, _ := snapshotIndex(v.Start.SeqNum(), i.snapshots)
		if currentIdx == idx {
			continue
		}
		if idx == 0 && i.elideRangeTombstone(v.Start.UserKey, v.End) {
			// This is the last snapshot stripe and the range tombstone can be
			// elided.
			break
		}

		i.tombstones = append(i.tombstones, v)
		if idx == 0 {
			// This is the last snapshot stripe.
			break
		}
		currentIdx = idx
	}
}

// maybeZeroSeqnum attempts to set the seqnum for the current key to 0. Doing
// so improves compression and enables an optimization during forward iteration
// to skip some key comparisons. The seqnum for an entry can be zeroed if the
// entry is on the bottom snapshot stripe and on the bottom level of the LSM.
func (i *compactionIter) maybeZeroSeqnum(snapshotIdx int) {
	if !i.allowZeroSeqNum {
		// TODO(peter): allowZeroSeqNum applies to the entire compaction. We could
		// make the determination on a key by key basis, similar to what is done
		// for elideTombstone. Need to add a benchmark for compactionIter to verify
		// that isn't too expensive.
		return
	}
	if snapshotIdx > 0 {
		// This is not the last snapshot
		return
	}
	i.key.SetSeqNum(0)
}
