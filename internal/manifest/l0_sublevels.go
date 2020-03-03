package manifest

import (
	"math"
	"sort"

	"github.com/cockroachdb/pebble/internal/base"
)

// TODO(sbhola): work items:
// - Prior to integration:
//   - update the data-structures after a compaction is picked.
//   - basic correctness test.
//   - benchmark with ~1000 files to ensure that initialization and
//     picking is fast enough.
// - Integration with Pebble (probably as a followup to #546)
// - TPCC import experiments

// Intervals are of the form [start, end) with no gap between intervals. Each
// file overlaps perfectly with a sequence of intervals. This perfect overlap
// occurs because the union of file boundary keys is used to pick intervals.
// However the largest key in a file is not inclusive, so when it is used as
// an interval, the actual key is ImmediateSuccessor(key). We don't have the
// ImmediateSuccessor function to do this computation, so we instead keep an
// isLargest bool to remind the code about this fact. This is used for
// comparisons in the following manner:
// - intervalKey{k, false} < intervalKey{k, true}
// - k1 < k2 => intervalKey{k1, _} < intervalKey{k2, _}.
//
// The intervals are indexed starting from 0, with the index of the interval
// being the index of the start key of the interval.
//
// In addition to helping with compaction choosing, we use interval indices
// to assign each file an interval range once. Subsequent operations, say picking
// overlapping files for a compaction, only need to use the index numbers and
// so avoid expensive string comparisons.
type intervalKey struct {
	key       InternalKey
	isLargest bool
}

func intervalKeyCompare(cmp Compare, a, b intervalKey) int {
	rv := base.InternalCompare(cmp, a.key, b.key)
	if rv == 0 {
		if a.isLargest && !b.isLargest {
			return +1
		}
		if !a.isLargest && b.isLargest {
			return -1
		}
	}
	return rv
}

type intervalKeySorter struct {
	keys []intervalKey
	cmp  Compare
}

func (s intervalKeySorter) Len() int { return len(s.keys) }
func (s intervalKeySorter) Less(i, j int) bool {
	return intervalKeyCompare(s.cmp, s.keys[i], s.keys[j]) < 0
}
func (s intervalKeySorter) Swap(i, j int) {
	s.keys[i], s.keys[j] = s.keys[j], s.keys[i]
}

func sortAndDedup(keys []intervalKey, cmp Compare) []intervalKey {
	if len(keys) == 0 {
		return nil
	}
	sorter := intervalKeySorter{keys: keys, cmp: cmp}
	sort.Sort(sorter)
	j := 0
	for i := 1; i < len(keys); i++ {
		cmp := intervalKeyCompare(cmp, keys[i], keys[j])
		if cmp != 0 {
			j++
			keys[j] = keys[i]
		}
	}
	return keys[0 : j+1]
}

// A wrapper around FileMetadata.
type fileMeta struct {
	index int
	meta  *FileMetadata

	// TODO(sbhola): move these two fields to FileMetadata. The code
	// below is incomplete in that it doesn't initialize these fields.
	isBaseCompacting    bool
	isIntraL0Compacting bool

	// Const after initialization.
	subLevel         int
	minIntervalIndex int
	maxIntervalIndex int
}

// A key interval of the form [start, end). The end is not represented here since it
// is implicit in the start of the next interval. The last interval is an exception
// but we don't need to ever lookup the end of that interval. The set of intervals
// is const after initialization.
type fileInterval struct {
	index    int
	startKey intervalKey

	// True iff some file in this interval is compacting to base. Such intervals
	// cannot have any files participate in L0 => Lbase compactions.
	// (there can be rare cases where seqnum 0 files get added and occupy lower
	// sublevels in this interval than files undergoing compaction, which may
	// allow for this interval to participate in another compaction, but for now
	// we eschew such complexity).
	isBaseCompacting bool

	// The min and max intervals index across all the files that overlap with this
	// interval.
	filesMinIntervalIndex int
	filesMaxIntervalIndex int

	// One could consider an interval for an L0 => Lbase compaction if
	// !isBaseCompacting but if it is near another interval that is undergoing
	// such a compaction it may have a file that extends into that interval
	// and prevents compaction. To reduce the number of failed candidate
	// intervals, the following bit encodes whether any interval in
	// [filesMinIntervalIndex, filesMaxIntervalIndex] has isBaseCompacting set
	// to true. Note that this is a pessimistic filter: the file that widened
	// [filesMinIntervalIndex, filesMaxIntervalIndex] may be at a high
	// sub-level and may not need to be included in the compaction.
	intervalRangeIsBaseCompacting bool

	// fileCount - compactingFileCount is the stack depth that requires
	// starting new compactions. This metric is not precise since the
	// compactingFileCount can include files that are part of N (where N > 1)
	// intra-L0 compactions, so the stack depth after those complete will be
	// fileCount - compactingFileCount + N. We ignore this imprecision since
	// we don't want to track which files are part of which intra-L0
	// compaction.
	fileCount           int
	compactingFileCount int

	// The number of files at the top of the stack of files in this
	// range that are not compacting.
	topOfStackNonCompactingFileCount int
	lowestSubLevel                   int
	highestSubLevel                  int
	// TODO(sbhola): consider keeping a slice of sublevel numbers for this
	// interval. It will reduce the searching in *CompactionUsingSeed methods.
	// Additionally, it can be used to quickly recompute the
	// topOfStackNonCompactingFileCount and to adjust it to account for
	// earliestUnflushedSeqNum.
	fileBytes uint64 // interpolated
}

type l0SubLevels struct {
	cmp Compare

	// Oldest to youngest.
	filesByAge []*fileMeta

	// Files in each sub-level ordered by increasing key order. Sub-levels
	// are ordered from oldest to youngest.
	subLevels [][]*fileMeta

	// The file intervals in increasing key order.
	orderedIntervals []fileInterval

	// Keys to break flushes at.
	flushSplitKeys []InternalKey
}

func insertIntoSubLevel(files []*fileMeta, f *fileMeta) []*fileMeta {
	index := sort.Search(len(files), func(i int) bool {
		return f.minIntervalIndex < files[i].minIntervalIndex
	})
	if index == len(files) {
		files = append(files, f)
		return files
	}
	files = append(files, nil)
	copy(files[index+1:], files[index:])
	files[index] = f
	return files
}

func NewL0SubLevels(files []*FileMetadata, cmp Compare, flushSplitMaxBytes uint64) *l0SubLevels {
	s := &l0SubLevels{cmp: cmp}
	s.filesByAge = make([]*fileMeta, len(files))
	keys := make([]intervalKey, 0, 2*len(files))
	for i := range files {
		s.filesByAge[i] = &fileMeta{index: i, meta: files[i]}
		// TODO: incorrect hack
		s.filesByAge[i].isBaseCompacting = files[i].Compacting
		keys = append(keys, intervalKey{key: files[i].Smallest})
		keys = append(keys, intervalKey{key: files[i].Largest, isLargest: true})
	}
	keys = sortAndDedup(keys, cmp)
	s.orderedIntervals = make([]fileInterval, len(keys))
	for i := range keys {
		s.orderedIntervals[i] = fileInterval{
			index:                 i,
			startKey:              keys[i],
			filesMinIntervalIndex: i,
			filesMaxIntervalIndex: i,
		}
	}
	// Initialize minIntervalIndex and maxIntervalIndex for each file, and use that
	// to update intervals.
	intervalRangeIsBaseCompacting := make([]bool, len(keys))
	for _, f := range s.filesByAge {
		f.minIntervalIndex = sort.Search(len(keys), func(index int) bool {
			return intervalKeyCompare(cmp, intervalKey{key: f.meta.Smallest}, keys[index]) <= 0
		})
		if f.minIntervalIndex == len(keys) {
			panic("bug")
		}
		f.maxIntervalIndex = sort.Search(len(keys), func(index int) bool {
			return intervalKeyCompare(
				cmp, intervalKey{key: f.meta.Largest, isLargest: true}, keys[index]) <= 0
		})
		if f.maxIntervalIndex == len(keys) {
			panic("bug")
		}
		interpolatedBytes := f.meta.Size / uint64(f.maxIntervalIndex-f.minIntervalIndex+1)
		subLevel := 0
		for i := f.minIntervalIndex; i <= f.maxIntervalIndex; i++ {
			if subLevel < s.orderedIntervals[i].highestSubLevel {
				subLevel = s.orderedIntervals[i].highestSubLevel
			}
			s.orderedIntervals[i].fileCount++
			if f.isBaseCompacting {
				s.orderedIntervals[i].isBaseCompacting = true
				s.orderedIntervals[i].compactingFileCount++
				s.orderedIntervals[i].topOfStackNonCompactingFileCount = 0
				intervalRangeIsBaseCompacting[i] = true
			} else if f.isIntraL0Compacting {
				s.orderedIntervals[i].compactingFileCount++
				s.orderedIntervals[i].topOfStackNonCompactingFileCount = 0
			} else {
				s.orderedIntervals[i].topOfStackNonCompactingFileCount++
			}
			s.orderedIntervals[i].fileBytes += interpolatedBytes
			if f.minIntervalIndex < s.orderedIntervals[i].filesMinIntervalIndex {
				s.orderedIntervals[i].filesMinIntervalIndex = f.minIntervalIndex
			}
			if f.maxIntervalIndex > s.orderedIntervals[i].filesMaxIntervalIndex {
				s.orderedIntervals[i].filesMaxIntervalIndex = f.maxIntervalIndex
			}
		}
		subLevel++
		for i := f.minIntervalIndex; i <= f.maxIntervalIndex; i++ {
			interval := &s.orderedIntervals[i]
			if interval.fileCount == 1 {
				interval.lowestSubLevel = subLevel
			}
			interval.highestSubLevel = subLevel
		}
		if subLevel > len(s.subLevels) {
			panic("bug")
		}
		if subLevel == len(s.subLevels) {
			s.subLevels = append(s.subLevels, []*fileMeta{f})
		}
		f.subLevel = subLevel
		s.subLevels[subLevel] = insertIntoSubLevel(s.subLevels[subLevel], f)
	}
	min := 0
	var cumulativeBytes uint64
	for i := 0; i < len(s.orderedIntervals); i++ {
		if s.orderedIntervals[i].isBaseCompacting {
			minIndex := s.orderedIntervals[i].filesMinIntervalIndex
			if minIndex < min {
				minIndex = min
			}
			for j := minIndex; j < s.orderedIntervals[i].filesMaxIntervalIndex; j++ {
				min = j
				s.orderedIntervals[j].intervalRangeIsBaseCompacting = true
			}
		}
		if cumulativeBytes > flushSplitMaxBytes {
			s.flushSplitKeys = append(s.flushSplitKeys, s.orderedIntervals[i].startKey.key)
			cumulativeBytes = 0
		}
		cumulativeBytes += s.orderedIntervals[i].fileBytes
	}
	return s
}

// For stats etc.
func (s *l0SubLevels) ReadAmplification() int {
	amp := 0
	for i := range s.orderedIntervals {
		interval := &s.orderedIntervals[i]
		if amp < interval.fileCount {
			amp = interval.fileCount
		}
	}
	return amp
}

// Used by flushes to avoid writing sstables that straddle these split keys.
// These should be interpreted as the keys to start the next sstable (not the
// last key to include in the prev sstable).
//
// TODO(sbhola): Change these to user keys since we need to precisely
// truncate range tombstones when writing multiple sstables (L0 is not
// allowed to have untruncated range tombstones)
func (s *l0SubLevels) FlushSplitKeys() []InternalKey {
	return s.flushSplitKeys
}

// Used by compaction picker to decide compaction score for L0. There is no scoring for
// intra-L0 compaction -- they only run if L0 score is high but unable to pick an
// L0 => Lbase compaction.
func (s *l0SubLevels) maxDepthAfterOngoingCompactions() int {
	depth := 0
	for i := range s.orderedIntervals {
		interval := &s.orderedIntervals[i]
		intervalDepth := interval.fileCount - interval.compactingFileCount
		if depth < intervalDepth {
			depth = intervalDepth
		}
	}
	return depth
}

type level0CompactionFiles struct {
	files                           []*fileMeta
	seedIntervalStackDepthReduction int
	fileBytes                       uint64

	// For internal use.
	filesMinIntervalIndex int
	filesMaxIntervalIndex int
}

// Helper to order intervals being considered for compaction.
type intervalAndScore struct {
	interval *fileInterval
	score    int
}
type intervalSorterByDecreasingScore []intervalAndScore

func (is intervalSorterByDecreasingScore) Len() int { return len(is) }
func (is intervalSorterByDecreasingScore) Less(i, j int) bool {
	return is[i].score > is[j].score
}
func (is intervalSorterByDecreasingScore) Swap(i, j int) {
	is[i], is[j] = is[j], is[i]
}

// Compactions:
//
// The sub-levels and intervals can be visualized in 2 dimensions as the X
// axis containing intervals in increasing order and the Y axis containing
// sub-levels (older to younger). The intervals can be sparse wrt sub-levels.
// We observe that the system is typically under severe pressure in L0 during
// large imports where most files added to L0 are narrow and non-overlapping.
// In that case we expect the rectangle represented in the above visualization
// to be wide and short, and not too sparse (most intervals will have
// fileCount close to the sub-level count), which would make it amenable to
// concurrent L0 => Lbase compactions.
//
// L0 => Lbase: The high-level goal of a L0 => Lbase compaction is to reduce
// stack depth, by compacting files in the intervals with the highest
// (fileCount - compactingCount). Additionally, we would like compactions to
// not involve a huge number of files, so that they finish quickly, and to
// allow for concurrent L0 => Lbase compactions when needed. In order to
// achieve these goals we would like compactions to visualize as capturing
// thin and tall rectangles. The approach below is to consider intervals in
// some order and then try to construct a compaction using the interval. The
// first interval we can construct a compaction for is the compaction that is
// started. There can be multiple heuristics in choosing the ordering of the
// intervals -- the code uses one heuristic, but experimentation is probably
// needed to pick a good one. Additionally, the compaction that gets picked
// may be not as desirable as one that could be constructed later in terms of
// reducing stack depth (since adding more files to the compaction can get
// blocked by needing to encompass files that are already being compacted). So
// an alternative would be to try to construct more than one compaction and
// pick the best one.
//
// Intra-L0: If the L0 score is high, but PickBaseCompaction() is unable to
// pick a compaction, PickIntraL0Compaction will be used to pick an intra-L0
// compaction. Similar to L0 => Lbase compactions, we want to allow for
// multiple intra-L0 compactions and not generate wide output files that
// hinder later concurrency of L0 => Lbase compactions. Also compactions
// that produce wide files don't reduce stack depth -- they represent wide
// rectangles in our visualization, which means many intervals have their
// depth reduced by a small amount. Typically, L0 files have non-overlapping
// sequence numbers, and sticking to that invariant would require us to
// consider intra-L0 compactions that proceed from youngest to oldest files,
// which could result in the aforementioned undesirable wide rectangle
// shape. But this non-overlapping sequence number is already relaxed in
// RocksDB -- sstables are primarily ordered by their largest sequence
// number. So we can arrange for intra-L0 compactions to capture thin and
// tall rectangles starting with the top of the stack (youngest files).
// Like the L0 => Lbase case we order the intervals using a heuristic and
// consider each in turn. The same comment about better heuristics and not
// being greedy applies here.
//
// TODO(sbhola): after experimenting and settling on the right heuristics
// we can probably generalize the code below for more code sharing between
// the two kinds of compactions.

func (s *l0SubLevels) PickBaseCompaction(minCompactionDepth int) *level0CompactionFiles {
	// We consider intervals in a greedy manner in the following order:
	// - pool1: Contains intervals that are unlikely to be blocked due
	//   to ongoing L0 => Lbase compactions. These are the ones with
	//   !isBaseCompacting && !intervalRangeIsBaseCompacting.
	// - pool2: Contains intervals that are !isBaseCompacting && intervalRangeIsBaseCompacting.
	//
	// The ordering heuristic exists just to avoid wasted work. Ideally,
	// we would consider all intervals with isBaseCompacting = false and
	// construct a compaction for it and compare the constructed compactions
	// and pick the best one. If microbenchmarks show that we can afford
	// this cost we will eliminate this heuristic.
	var pool1, pool2 []intervalAndScore
	for i := range s.orderedIntervals {
		interval := &s.orderedIntervals[i]
		depth := interval.fileCount - interval.compactingFileCount
		if interval.isBaseCompacting || minCompactionDepth > depth {
			continue
		}
		if interval.intervalRangeIsBaseCompacting {
			pool2 = append(pool2, intervalAndScore{interval: interval, score: depth})
		} else {
			pool1 = append(pool1, intervalAndScore{interval: interval, score: depth})
		}
	}
	sort.Sort(intervalSorterByDecreasingScore(pool1))
	sort.Sort(intervalSorterByDecreasingScore(pool2))

	// Optimization to avoid considering different intervals that
	// are likely to choose the same seed file. Again this is just
	// to reduce wasted work.
	consideredIntervals := make([]bool, len(s.orderedIntervals))
	for _, pool := range [2][]intervalAndScore{pool1, pool2} {
		for _, interval := range pool {
			if consideredIntervals[interval.interval.index] {
				continue
			}
			// Pick the seed file for the interval as the file
			// in the lowest sub-level.
			files := s.subLevels[interval.interval.lowestSubLevel]
			index := sort.Search(len(files), func(i int) bool {
				return files[i].minIntervalIndex >= interval.interval.index
			})
			if index == len(files) {
				panic("bug")
			}
			f := files[index]
			for i := f.minIntervalIndex; i <= f.maxIntervalIndex; i++ {
				// Don't bother considering the intervals that are
				// covered by the seed file since they are likely
				// nearby. Note that it is possible that those intervals
				// have seed files at lower sub-levels so could be
				// viable for compaction.
				consideredIntervals[i] = true
			}
			c := s.baseCompactionUsingSeed(f, interval.interval.index)
			if c != nil {
				return c
			}
		}
	}
	return nil
}

func (s *l0SubLevels) baseCompactionUsingSeed(
	f *fileMeta, intervalIndex int,
) *level0CompactionFiles {
	cFiles := &level0CompactionFiles{
		files:                           []*fileMeta{f},
		filesMinIntervalIndex:           f.minIntervalIndex,
		filesMaxIntervalIndex:           f.maxIntervalIndex,
		seedIntervalStackDepthReduction: 1,
		fileBytes:                       f.meta.Size,
	}
	sl := f.subLevel
	fileIncluded := make([]bool, len(s.filesByAge))
	fileIncluded[f.index] = true
	// The seed file captures all files in the next level that fall
	// in the range of intervals. That may extend the range of
	// intervals so for correctness we need to capture all files
	// in the next level that fall in this extended interval and
	// so on. This can result in a triangular shape like the following
	// where again the X axis is the key intervals and the Y axis
	// is oldest to youngest. Note that it is not necessary for
	// correctness to fill out the shape at the higher sub-levels
	// to make it more rectangular since the invariant only requires
	// that younger versions of a key not be moved to Lbase while
	// leaving behind older versions.
	//                     -
	//                    ---
	//                   -----
	// It may be better for performance to have a more rectangular
	// shape since the files being left behind will induce touch the
	// same Lbase key range as that of this compaction. But there is
	// also the danger that in trying to construct a more rectangular
	// shape we will be forced to pull in a file that is already
	// compacting. We assume that the performance concern is not a
	// practical issue.
	for currLevel := sl - 1; currLevel >= 0; currLevel-- {
		if !s.extendFiles(currLevel, math.MaxUint64, cFiles, fileIncluded) {
			// Failed due to ongoing compaction.
			return nil
		}
	}

	// Now that we have a seed group of files we can optionally add to it by
	// stacking more files from intervalIndex and repeating. This is an
	// optional activity so when it fails we can fallback to the last
	// successful candidate. Currently the code keeps adding until it can't
	// add more, but we could optionally stop based on
	// levelOCompactionFiles.fileBytes being too large.
	lastCandidate := &level0CompactionFiles{}
	*lastCandidate = *cFiles
	sl = f.subLevel + 1
	for ; sl < len(s.subLevels); sl++ {
		index := sort.Search(len(s.subLevels[sl]), func(i int) bool {
			return s.subLevels[sl][i].minIntervalIndex >= intervalIndex
		})
		if index == len(s.subLevels[sl]) || s.subLevels[sl][index].minIntervalIndex > intervalIndex {
			// Not in the interval.
			continue
		}
		// In the interval.
		f2 := s.subLevels[sl][index]
		cFiles.seedIntervalStackDepthReduction++
		cFiles.fileBytes += f2.meta.Size
		cFiles.files = append(cFiles.files, f2)
		// Reset the min and max to that of this file. See the triangular
		// shape comment above on why this is correct.
		cFiles.filesMinIntervalIndex = f2.minIntervalIndex
		cFiles.filesMinIntervalIndex = f2.maxIntervalIndex
		done := false
		for currLevel := sl - 1; currLevel >= 0; currLevel-- {
			if !s.extendFiles(currLevel, math.MaxUint64, cFiles, fileIncluded) {
				// Failed to extend due to ongoing compaction.
				done = true
				break
			}
		}
		if done {
			break
		}
		*lastCandidate = *cFiles
	}
	return lastCandidate
}

func (s *l0SubLevels) extendFiles(
	sl int, earliestUnflushdSeqNum uint64, cFiles *level0CompactionFiles, fileIncluded []bool,
) bool {
	index := sort.Search(len(s.subLevels[sl]), func(i int) bool {
		return s.subLevels[sl][i].minIntervalIndex >= cFiles.filesMinIntervalIndex
	})
	for ; index < len(s.subLevels[sl]); index++ {
		f := s.subLevels[sl][index]
		if f.minIntervalIndex > cFiles.filesMaxIntervalIndex {
			break
		}
		if fileIncluded[f.index] || f.meta.LargestSeqNum >= earliestUnflushdSeqNum {
			continue
		}
		if f.isBaseCompacting || f.isIntraL0Compacting {
			return false
		}
		cFiles.files = append(cFiles.files, f)
		cFiles.fileBytes += f.meta.Size
		if f.minIntervalIndex < cFiles.filesMinIntervalIndex {
			cFiles.filesMinIntervalIndex = f.minIntervalIndex
		}
		if f.maxIntervalIndex > cFiles.filesMaxIntervalIndex {
			cFiles.filesMaxIntervalIndex = f.maxIntervalIndex
		}
	}
	return true
}

func (s *l0SubLevels) PickIntraL0Compaction(
	earliestUnflushedSeqNum uint64, minCompactionDepth int, minStackDepthReduction int,
) *level0CompactionFiles {
	var pool []intervalAndScore
	for i := range s.orderedIntervals {
		interval := &s.orderedIntervals[i]
		depth := interval.fileCount - interval.compactingFileCount
		if minCompactionDepth > depth || interval.topOfStackNonCompactingFileCount < minStackDepthReduction {
			continue
		}
		// Is there a way to incorporate topOfStackNonCompactingFileCount into the score?
		pool = append(pool, intervalAndScore{interval: interval, score: depth})
	}
	sort.Sort(intervalSorterByDecreasingScore(pool))

	// Optimization to avoid considering different intervals that
	// are likely to choose the same seed file. This is just
	// to reduce wasted work.
	consideredIntervals := make([]bool, len(s.orderedIntervals))
	for _, interval := range pool {
		if consideredIntervals[interval.interval.index] {
			continue
		}
		// Pick the seed file for the interval as the file
		// in the lowest sub-level.
		sl := interval.interval.highestSubLevel
		adjustedNonCompactingFileCount := interval.interval.topOfStackNonCompactingFileCount
		var f *fileMeta
		for ; sl >= 0; sl-- {
			files := s.subLevels[sl]
			index := sort.Search(len(files), func(i int) bool {
				return files[i].minIntervalIndex >= interval.interval.index
			})
			if index == len(files) || files[index].minIntervalIndex > interval.interval.index {
				// No files for this interval
				continue
			}
			// index refers to the file for this interval.
			f = files[index]
			for i := f.minIntervalIndex; i <= f.maxIntervalIndex; i++ {
				consideredIntervals[i] = true
			}
			// Can this be the seed file?
			if f.meta.LargestSeqNum >= earliestUnflushedSeqNum {
				adjustedNonCompactingFileCount--
				if adjustedNonCompactingFileCount == 0 {
					break
				}
			} else {
				break
			}
		}
		if adjustedNonCompactingFileCount < minStackDepthReduction {
			// Can't use this interval.
			continue
		}
		if sl < 0 {
			panic("bug")
		}
		// We have a seed file.
		c := s.intraL0CompactionUsingSeed(f, interval.interval.index, earliestUnflushedSeqNum)
		if c != nil && c.seedIntervalStackDepthReduction >= minStackDepthReduction {
			return c
		}
	}
	return nil
}

func (s *l0SubLevels) intraL0CompactionUsingSeed(
	f *fileMeta, intervalIndex int, earliestUnflushedSeqNum uint64,
) *level0CompactionFiles {
	// We know that all the files that overlap with intervalIndex have
	// LargestSeqNum < earliestUnflushedSeqNum, but for other intervals
	// we need to exclude files >= earliestUnflushedSeqNum

	cFiles := &level0CompactionFiles{
		files:                           []*fileMeta{f},
		filesMinIntervalIndex:           f.minIntervalIndex,
		filesMaxIntervalIndex:           f.maxIntervalIndex,
		seedIntervalStackDepthReduction: 1,
		fileBytes:                       f.meta.Size,
	}
	sl := f.subLevel
	fileIncluded := make([]bool, len(s.filesByAge))
	fileIncluded[f.index] = true

	// The seed file captures all files in the higher level that fall in the
	// range of intervals. That may extend the range of intervals so for
	// correctness we need to capture all files in the next higher level that
	// fall in this extended interval and so on. This can result in an
	// inverted triangular shape like the following where again the X axis is the
	// key intervals and the Y axis is oldest to youngest. Note that it is not
	// necessary for correctness to fill out the shape at lower sub-levels to
	// make it more rectangular since the invariant only requires that if we
	// move an older seqnum for key k into a file that has a higher seqnum, we
	// also move all younger seqnums for that key k into that file.
	//                  -----
	//                   ---
	//                    -
	//
	// It may be better for performance to have a more rectangular shape since
	// it will reduce the stack depth for more intervals. But there is also
	// the danger that in explicitly trying to construct a more rectangular
	// shape we will be forced to pull in a file that is already compacting.
	// We assume that the performance concern is not a practical issue.
	for currLevel := sl + 1; currLevel < len(s.subLevels); currLevel++ {
		if !s.extendFiles(currLevel, earliestUnflushedSeqNum, cFiles, fileIncluded) {
			// Failed due to ongoing compaction.
			return nil
		}
	}

	// Now that we have a seed group of files we can optionally add to it by
	// stacking more files from intervalIndex and repeating. This is an
	// optional activity so when it fails we can fallback to the last
	// successful candidate. Currently the code keeps adding until it can't
	// add more, but we could optionally stop based on
	// levelOCompactionFiles.fileBytes being too large.
	lastCandidate := &level0CompactionFiles{}
	*lastCandidate = *cFiles
	sl = f.subLevel - 1
	for ; sl >= 0; sl-- {
		index := sort.Search(len(s.subLevels[sl]), func(i int) bool {
			return s.subLevels[sl][i].minIntervalIndex >= intervalIndex
		})
		if index == len(s.subLevels[sl]) || s.subLevels[sl][index].minIntervalIndex > intervalIndex {
			// Not in the interval.
			continue
		}
		// In the interval.
		f2 := s.subLevels[sl][index]
		cFiles.seedIntervalStackDepthReduction++
		cFiles.fileBytes += f2.meta.Size
		cFiles.files = append(cFiles.files, f2)
		// Reset the min and max to that of this file. See the triangular
		// shape comment above on why this is correct.
		cFiles.filesMinIntervalIndex = f2.minIntervalIndex
		cFiles.filesMinIntervalIndex = f2.maxIntervalIndex
		done := false
		for currLevel := sl + 1; currLevel < len(s.subLevels); currLevel++ {
			if !s.extendFiles(currLevel, earliestUnflushedSeqNum, cFiles, fileIncluded) {
				// Failed to extend due to ongoing compaction.
				done = true
				break
			}
		}
		if done {
			break
		}
		*lastCandidate = *cFiles
	}
	return lastCandidate
}
