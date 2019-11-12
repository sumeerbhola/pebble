// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"fmt"
	"math"
	"sort"
	"strings"

	"github.com/cockroachdb/pebble/internal/manifest"
)

type compactionPickerInterface interface {
	estimatedCompactionDebt(l0ExtraSize uint64) uint64
	pickAuto(opts *Options, bytesCompacted *uint64) (c *compaction)
	pickManual(
		opts *Options, manual *manualCompaction, bytesCompacted *uint64,
	) (c *compaction, err error)
	getBaseLevel() int
	getEstimatedMaxWAmp() float64
	getLevelMaxBytes() [numLevels]int64
}

// Information about a compaction that is queued inside the compaction picker.
type compactionInfo struct {
	// The score of the level to be compacted.
	score float64
	level int
	// The level to compact to.
	outputLevel int
	// The seed file in level that will be compacted. Additional files may be picked by the
	// compaction to guarantee the level invariants. A value of -1 represents it should compact
	// all the files in that level.
	file int
}

// Information about in-progress compactions provided to the compaction picker. These are used to
// constrain the new compactions that will be picked. The count of the in-progress compactions also
// affects how many compactions will be queued up inside the picker since there is no point enqueuing
// more than the max number of concurrent compactions minus the in-progress compactions as this
// picker will be replaced with a new picker when a new version is installed.
type inProgressCompactionInfo interface {
	inputLevel() int
	outputLevel() int
	smallest() *InternalKey
	largest() *InternalKey
}

// compactionPicker2 holds the state and logic for picking new compactions. A
// compaction picker is associated with a single version. A new compaction
// picker is created and initialized every time a new version is installed.
//
// The picker algorithm is designed to handle the case of a high rate of ingested files while
// maintaining a healthy LSM tree. It is based on the assumption that the system will permit at
// least two concurrent compactions.
// - The picker allows compactions out of L0 to go to curBaseLevel - 1 if curBaseLevel is busy with
//   a compaction, when L0 is considered "critical" wrt compaction. This utilizes more of the levels
//   that have non-overlapping files and is considered better for LSM health than keeping files
//   in L0, say using intra-L0 compactions (which are not supported here). Intra-L0 compactions
//   result in wide and huge files which increase the cost of future compactions and can be
//   considered wasted work from a write amplification perspective (the only reason to do those
//   compactions is reducing read amplification).
// - The corollary to utilizing intermediate levels that would otherwise be empty is to drain them
//   once the system is no longer under stress. This drain scheme is conservative about when to
//   drain and what levels to drain. The highestBaseLevel in the code is what it will drain down
//   to.
//
// For the rest of this comment consider that the picker has caused the base level to move up to
// L1 to handle heavy write load. We consider how such an LSM tree will be kept healthy in
// the presence of continued heavy load (note that the code is more general, but we use L1 for ease
// of exposition).
// - The picker does not allow L1 to become "too big" even if L0 has a high compaction score (based
//   on number of files), by picking L1=>L2 compactions over L0=>L1 compactions. Allowing L1 to
//   become too big will cause large L0=>L1 compactions because of too many bytes in L1 and we want
//   to avoid that. We observe that comparing the compaction scores of L0 and L1, to choose
//   between them, is not necessarily appropriate since they are computed differently: L0's
//   score is based on file count which affects read amplification, but if there are many small L0
//   files (which is common with imports), a high score does not mean L0 is in a bad state wrt
//   impact on future write amplification. So we avoid comparing L0 and L1 scores with each other.
// - The previous bullet implies that there is backpressure on L0 to allow L1 to compact. We assume
//   that an import client properly listens to this backpressure signal. Specifically, the import client
//   should pay attention to both the file count and bytes in L0: even if it is willing to tolerate
//   a high file count, it should not go over a file bytes limit, say 512MB.
// - By not allowing L1 to become "too big" we expect that L0=>L1 and L1=>L2 compactions will take
//   turns, and since compaction concurrency > 1, the remaining compaction slot(s) will be
//   used to keep the levels >= 2 healthy.
// - It is probable that only two concurrent compactions are permitted, so one compaction slot for
//   levels >= 2. It seems likely that some levels >= 2 will fall behind, so one question is whether
//   the compaction score for a level (which looks at ratio of bytes to max bytes) is sufficient to
//   prioritize. We note that since L1 will be small because of the previously discussed behavior,
//   we do not want L2 to fall behind too much since L1=>L2 compactions will become expensive. A
//   more general observation is that a compaction from level i to i + 1 is wasteful when the ratio
//   of bytes(i + 1) / bytes(i), say R, is significantly higher than the levelMultiplier, since
//   each byte in level i (on average) will overlap with R bytes in level i + 1. If R is not
//   higher than the levelMultiplier, the system can continue to function efficiently wrt
//   compaction bandwidth, even if both these levels have high compaction scores. Based on this
//   logic we prioritize between multiple compactions that have a high score by using the ratio of
//   bytes across levels.
//
// The algorithm does not use a smoothed value of level multiplier as used in RockDB and the
// previous Pebble compaction picker. This is because it is hard to compute a highestBaseLevel for
// draining higher levels when we smooth the level multiplier. This may have some negative effect
// in write amplification in the steady state. Recollect that the levelMultiplier is high (10) to
// allow the DB with a fixed number of levels (hence constrained read amplification) to accommodate
// a large number of total bytes. But a lower effective/smoother multiplier can reduce the write
// amplification (with higher read amplification). Consider a DB with total size of approximately
// 2000MB bytes and level multipliers of 2 and 10 respectively. The approximate max bytes for each
// level are (assuming we lower bound max bytes to 64MB):
// - multiplier 2:  L6: 1024, L5: 512, L4: 256, L3: 128, L2: 64
// - multiplier 10: L6: 1900, L5: 190, L4: 64
// And say L0 is approximately 64MB.
// The corresponding max write amplification for multiplier 2 is 3 + 3 + 3 + 3 + 2 = 14, and for
// multiplier 10 is 11 + 4 + 2 = 17. It is not clear to me how this max write amplification will
// relate to actual observed write amplification, but if it is indeed higher for this algorithm, we
// could figure out a way to introduce a smoothed value.
//
// Alternatives to consider:
// - A concurrent compaction version of the current simple score based picker.
// - Constrain L0 compactions to no higher than highestBaseLevel. Since HBL has a max size of 6.4MB,
//   HBL + 1 will have a max size of 64MB, so we will ensure that HBL => HBL + 1 compactions are
//   short, which is the main purpose of utilizing additional levels (get data out of L0 and out of HBL
//   with short compactions). This may also allow us to eliminate the draining code.
// - The mechanisms relating to HBL and critical/semi-critical categories of compactions (and how
//   to prioritize within each category) are orthogonal: one could decouple them and try the latter
//   without the former.
type compactionPicker2 struct {
	opts *Options
	vers *version

	// This is one of the following (a) if there are no bytes in levels lower than L0, this is
	// the lowest configured level, (b) the highest level lower than L0 that has non zero bytes,
	// if the total bytes in the DB do not indicate that this level is overfull, else it is one
	// level higher than this highest level. This is where compactions out of L0 will typically
	// go to, unless we decide that we are in an extreme situation and need to send them to higher
	// levels.
	currBaseLevel int
	// This is the highest level that should be the base level in a steady state system that is
	// experiencing extreme write traffic. Note that highestBaseLevel can be less, equal or greater
	// than currBaseLevel. The greater case will happen when we have used higher levels to absorb
	// heavy write traffic. This value is used to decide which levels to drain when the traffic
	// returns to normal.
	highestBaseLevel int
	// estimatedMaxWAmp is the estimated maximum write amp per byte that is
	// added to L0. This is a simple calculation that uses the configured levelMultiplier and
	// currBaseLevel -- it does not take into account the current bytes in each level.
	estimatedMaxWAmp float64
	// levelMaxBytes holds the dynamically adjusted max bytes setting for each
	// level.
	levelMaxBytes [numLevels]int64
	// These are the compaction scores for each level. A score > 1 indicates a level should be
	// compacted. For levels > L0, this is based on a ratio of actual bytes and levelMaxBytes. For
	// L0, this is based on the file count.
	scores [numLevels]float64
	// For each level > 0, this is the ratio of its bytes to the bytes in the next higher level
	// (the level that will compact into this level).
	currentByteRatios [numLevels]float64
	// The queue of compactions to start next. This will not be bigger than
	// maxConcurrentCompactions - len(inProgressCompactions).
	compactionQueue []compactionInfo
	// The levels with ongoing compactions.
	levelsWithCompactions map[int]bool
}

var _ compactionPickerInterface = &compactionPicker2{}

func newCompactionPicker2(
	v *version,
	opts *Options,
	maxConcurrentCompactions int,
	inProgressCompactions []inProgressCompactionInfo,
) *compactionPicker2 {
	p := &compactionPicker2{
		opts: opts,
		vers: v,
	}
	p.initLevelState()
	n := maxConcurrentCompactions - len(inProgressCompactions)
	if n > 0 {
		p.initCompactionQueue(n, inProgressCompactions)
	}
	/*
		fmt.Printf("newCompactionPicker2 %p: max: %d, ongoing: %d, picked: %d, level0: %d\n%s\n",
			p,
			maxConcurrentCompactions, len(inProgressCompactions), len(p.compactionQueue), len(v.Files[0]),
			string(debug.Stack()),
		)
	*/
	return p
}

func (p *compactionPicker2) getBaseLevel() int {
	if p == nil {
		return 1
	}
	return p.currBaseLevel
}

func (p *compactionPicker2) getEstimatedMaxWAmp() float64 {
	return p.estimatedMaxWAmp
}

func (p *compactionPicker2) getLevelMaxBytes() [numLevels]int64 {
	return p.levelMaxBytes
}

// estimatedCompactionDebt estimates the number of bytes which need to be
// compacted before the LSM tree becomes stable. This is not a sophisticated computation:
// it does not take into account any draining of levels that may need to happen, or the effect
// of ongoing compactions.
func (p *compactionPicker2) estimatedCompactionDebt(l0ExtraSize uint64) uint64 {
	if p == nil {
		return 0
	}

	// We assume that all the bytes in L0 need to be compacted to L1. This is unlike
	// the RocksDB logic that figures out whether L0 needs compaction.
	compactionDebt := totalSize(p.vers.Files[0]) + l0ExtraSize
	bytesAddedToNextLevel := compactionDebt

	levelSize := totalSize(p.vers.Files[p.currBaseLevel])
	// estimatedL0CompactionSize is the estimated size of the L0 component in the
	// current or next L0->LBase compaction. This is needed to estimate the number
	// of L0->LBase compactions which will need to occur for the LSM tree to
	// become stable.
	estimatedL0CompactionSize := uint64(p.opts.L0CompactionThreshold * p.opts.MemTableSize)
	// The ratio bytesAddedToNextLevel(L0 Size)/estimatedL0CompactionSize is the
	// estimated number of L0->LBase compactions which will need to occur for the
	// LSM tree to become stable. Let this ratio be N.
	//
	// We assume that each of these N compactions will overlap with all the current bytes
	// in LBase, so we multiply N * totalSize(LBase) to count the contribution of LBase inputs
	// to these compactions. Note that each compaction is adding bytes to LBase that will take
	// part in future compactions, but we have already counted those.
	compactionDebt += (levelSize * bytesAddedToNextLevel) / estimatedL0CompactionSize

	var nextLevelSize uint64
	for level := p.currBaseLevel; level < numLevels-1; level++ {
		levelSize += bytesAddedToNextLevel
		bytesAddedToNextLevel = 0
		nextLevelSize = totalSize(p.vers.Files[level+1])
		// fmt.Printf("level: %d, size: %d, max: %d\n", level, levelSize, p.levelMaxBytes[level])
		if levelSize > uint64(p.levelMaxBytes[level]) {
			bytesAddedToNextLevel = levelSize - uint64(p.levelMaxBytes[level])
			levelRatio := float64(nextLevelSize) / float64(levelSize)
			// The current level contributes bytesAddedToNextLevel to compactions.
			// The next level contributes levelRatio * bytesAddedToNextLevel.
			compactionDebt += uint64(float64(bytesAddedToNextLevel) * (levelRatio + 1))
		}
		levelSize = nextLevelSize
	}

	return compactionDebt
}

func (p *compactionPicker2) initLevelState() {
	firstNonEmptyLevelAfterL0 := -1
	var bottomLevelSize int64
	var bottomLevel int
	var dbSize int64
	var prevLevelSize int64
	for level := 0; level < numLevels; level++ {
		levelSize := int64(totalSize(p.vers.Files[level]))
		if levelSize > 0 {
			if level > 0 && firstNonEmptyLevelAfterL0 == -1 {
				firstNonEmptyLevelAfterL0 = level
			}
			bottomLevelSize = levelSize
			bottomLevel = level
			if prevLevelSize == 0 {
				p.currentByteRatios[level] = float64(levelSize)
			} else {
				p.currentByteRatios[level] = float64(levelSize) / float64(prevLevelSize)
			}
		}
		prevLevelSize = levelSize
		dbSize += levelSize
	}

	// Initialize the max-bytes setting for each level to "infinity" which will
	// disallow compaction for that level. We'll fill in the actual value below
	// for levels we want to allow compactions from.
	for level := 0; level < numLevels; level++ {
		p.levelMaxBytes[level] = math.MaxInt64
	}

	// We treat level-0 specially by bounding the number of files instead of
	// number of bytes for two reasons:
	//
	// (1) With larger write-buffer sizes, it is nice not to do too many
	// level-0 compactions.
	//
	// (2) The files in level-0 are merged on every read and therefore we
	// wish to avoid too many files when the individual file size is small
	// (perhaps because of a small ingested files, small write-buffer setting, or very high
	// compression ratios, or lots of overwrites/deletions).
	p.scores[0] = float64(len(p.vers.Files[0])) / float64(p.opts.L0CompactionThreshold)

	if bottomLevel == 0 {
		// No levels for L1 and below contain any data. Target L0 compactions for the
		// last level.
		p.currBaseLevel = numLevels - 1
		p.highestBaseLevel = numLevels - 1
		return
	}

	// bottomLevel > 0. It is possible that bottomLevel < numLevels - 1 because of a compaction
	// that deleted all existing data in numLevels - 1. That situation is not interesting, so we
	// do not use the actual value of bottomLevel in the following computation.

	levelMultiplier := 10.0

	baseBytesMax := p.opts.LBaseMaxBytes
	baseBytesMin := int64(float64(baseBytesMax) / levelMultiplier)

	// It is possible that the bottom level is not large enough due to bytes accumulating in other
	// levels. In that case we set the bottom bytes to 90% of the total bytes in the db.
	// Note that this is just a heuristic since we don't know how many of the total bytes represent
	// live data, but it should help with extreme imbalanced cases.
	if float64(dbSize)*0.9 > float64(bottomLevelSize) {
		bottomLevelSize = int64(float64(dbSize) * 0.9)
	}

	p.currBaseLevel = firstNonEmptyLevelAfterL0
	b := bottomLevelSize
	// If the currBaseLevel is too low a level based on the bytes in the DB, move it to a higher
	// level.
	for level := numLevels - 2; level >= p.currBaseLevel; level-- {
		b = int64(float64(b) / levelMultiplier)
	}
	for level := p.currBaseLevel - 1; level > 0 && b > baseBytesMax; level-- {
		b = int64(float64(b) / levelMultiplier)
		p.currBaseLevel = level
	}

	// Compute the highestBaseLevel. We err on the side of making this small, which is why we use
	// baseBytesMin in this computation.
	nextLevelSize := bottomLevelSize
	p.highestBaseLevel = 1
	for level := numLevels - 1; level >= 1; level-- {
		curLevelSize := nextLevelSize
		nextLevelSize = int64(float64(nextLevelSize) / levelMultiplier)
		if curLevelSize <= baseBytesMin {
			p.highestBaseLevel = level
			break
		}
	}

	// Compute levelMaxBytes and compaction scores for each level > 0 and < numLevels -1. We have
	// already computed the score for L0 above. We lower bound the levelMaxBytes by baseBytesMax
	// since we don't want to run compactions for tiny levels by unnecessarily inflating the score
	// -- if we have ended up with tiny levels due to currBaseLevel < highestBaseLevel we will handle
	// that separately by draining some of the higher levels without needing a high score.
	p.levelMaxBytes[numLevels-1] = bottomLevelSize
	//	fmt.Printf("scores\n")
	for level := numLevels - 2; level >= 1; level-- {
		p.levelMaxBytes[level] = int64(float64(p.levelMaxBytes[level+1]) / levelMultiplier)
		if p.levelMaxBytes[level] < baseBytesMax {
			p.levelMaxBytes[level] = baseBytesMax
		}
		p.scores[level] = float64(totalSize(p.vers.Files[level])) / float64(p.levelMaxBytes[level])
		//		fmt.Printf("level: %d, max size: %d, score: %f\n", level, p.levelMaxBytes[level], p.scores[level])
	}

	p.estimatedMaxWAmp = float64(numLevels-p.currBaseLevel) * (levelMultiplier + 1)
}

type sortCompactionsDecreasingMeasure struct {
	level   []int
	measure [numLevels]float64
}

func (s sortCompactionsDecreasingMeasure) Len() int { return len(s.level) }
func (s sortCompactionsDecreasingMeasure) Less(i, j int) bool {
	return s.measure[s.level[i]] > s.measure[s.level[j]]
}
func (s sortCompactionsDecreasingMeasure) Swap(i, j int) {
	s.level[i], s.level[j] = s.level[j], s.level[i]
}

func (p *compactionPicker2) initCompactionQueue(
	numCompactions int, inProgressCompactions []inProgressCompactionInfo,
) {
	levelsWithCompactions := make(map[int]bool)
	p.levelsWithCompactions = make(map[int]bool)
	for _, c := range inProgressCompactions {
		levelsWithCompactions[c.inputLevel()] = true
		levelsWithCompactions[c.outputLevel()] = true
		p.levelsWithCompactions[c.inputLevel()] = true
		p.levelsWithCompactions[c.outputLevel()] = true

	}
	if numCompactions == 0 {
		return
	}
	var logString strings.Builder
	defer func() { p.opts.Logger.Infof(logString.String()) }()

	fmt.Fprintf(&logString, "free: %d,", numCompactions)
	// See the high-level comment at the top of the file for a broad overview of the algorithmic
	// approach. We consider compactions in the following order:
	// - Critical compactions: these are compactions out of currBaseLevel, and out of L0, when the
	//   compaction scores of these levels are "high". Within critical compactions, the compaction
	//   out of currBaseLevel is more important since that is the only way we can apply backpressure
	//   on L0.
	// - Semi-critical compactions: These can be at any level. These consider levels with compaction
	//   score > 2. Within this set the prioritization is based on currentByteRatios since
	//   a level i + 1 that is unreasonably large compared to level i causes level i => i + 1
	//   compactions to have higher write amplification than desired. Note that there will be some
	//   correlation between the compaction score and this ratio: e.g. if level 1 has score 2 and
	//   level 2 has score 10, then the ratio for level 2 will also be high. But if level 3 has
	//   score 12, this will end up preferring the compaction from level 2 => 3 over a compaction
	//   from level 3 => 4. Eventually, this should cause the ratio for level 3 to be large enough
	//   that it will be preferred.
	// - Non-critical compactions: These can be at any level, and ordering is based on comparing
	//   the score.

	// 2 and 4 are somewhat arbitrarily chosen.
	const highScore = 2
	const veryHighScore = 4

	// Critical compactions.
	cblScore := p.scores[p.currBaseLevel]
	canCompactCBL := p.currBaseLevel < numLevels-1 && !levelsWithCompactions[p.currBaseLevel] &&
		!levelsWithCompactions[p.currBaseLevel+1]
	// If the currBaseLevel is critical to compact, and can be compacted, pick it.
	if canCompactCBL && (cblScore > veryHighScore || (cblScore > highScore && p.currBaseLevel > 1)) {
		p.compactionQueue = append(p.compactionQueue,
			compactionInfo{score: cblScore, level: p.currBaseLevel, outputLevel: p.currBaseLevel + 1})
		levelsWithCompactions[p.currBaseLevel] = true
		levelsWithCompactions[p.currBaseLevel+1] = true
		numCompactions--
		fmt.Fprintf(&logString, "cc: base(level, score): %d, %f,", p.currBaseLevel, cblScore)
		if numCompactions == 0 {
			return
		}
	}

	if p.scores[0] > highScore && !levelsWithCompactions[0] {
		// L0 is critical to compact and not undergoing compaction.
		cblIsCompacting := levelsWithCompactions[p.currBaseLevel]
		if !cblIsCompacting {
			// Note that is is possible we get here amd cblScore is high but not yet picked to
			// compact because it is either not high enough or cbl + 1 is compacting. Both cases
			// will eventually no longer hold: cblScore will eventually become high enough to be
			// critical; and cbl + 1 is not getting more bytes without cbl compacting so cbl + 1
			// will eventually stop compacting.
			//
			// Compact to cbl.
			p.compactionQueue = append(p.compactionQueue,
				compactionInfo{score: p.scores[0], level: 0, outputLevel: p.currBaseLevel})
			levelsWithCompactions[0] = true
			levelsWithCompactions[p.currBaseLevel] = true
			fmt.Fprintf(&logString, "cc: 0-to-base(score): %f,", cblScore)
			numCompactions--
		} else if p.scores[0] > veryHighScore && p.currBaseLevel >= 1 {
			// Compact to cbl - 1.
			p.compactionQueue = append(p.compactionQueue,
				compactionInfo{score: p.scores[0], level: 0, outputLevel: p.currBaseLevel - 1})
			levelsWithCompactions[0] = true
			levelsWithCompactions[p.currBaseLevel-1] = true
			fmt.Fprintf(&logString, "cc: 0-to-below-base(score): %f,", cblScore)
			numCompactions--
		}
	}
	if numCompactions == 0 {
		return
	}

	// Semi-critical and non-critical compactions.
	var semiCriticalCompactions []int
	var nonCriticalCompactions []int
	for i := 0; i < numLevels-1; i++ {
		//		fmt.Printf("level: %d\n", i)
		outputLevel := i + 1
		if i == 0 {
			outputLevel = p.currBaseLevel
		}
		if !levelsWithCompactions[i] && !levelsWithCompactions[outputLevel] {
			if p.scores[i] > highScore {
				semiCriticalCompactions = append(semiCriticalCompactions, i)
				fmt.Fprintf(&logString, "scc(l,s,r): %d,%f,%f,", i, p.scores[i], p.currentByteRatios[i])
			} else if p.scores[i] >= 1 {
				//				fmt.Printf("level: %d, outputLevel: %d, score: %f\n", i, outputLevel, p.scores[i])
				nonCriticalCompactions = append(nonCriticalCompactions, i)
				fmt.Fprintf(&logString, "ncc(l,s,r): %d,%f,%f,", i, p.scores[i], p.currentByteRatios[i])
			}
		}
	}
	sort.Sort(sortCompactionsDecreasingMeasure{
		level: semiCriticalCompactions, measure: p.currentByteRatios,
	})
	sort.Sort(sortCompactionsDecreasingMeasure{
		level: nonCriticalCompactions, measure: p.scores,
	})
	lists := [2][]int{semiCriticalCompactions, nonCriticalCompactions}
	for i := 0; i < len(lists) && numCompactions > 0; i++ {
		c := lists[i]
		for j := 0; numCompactions > 0 && j < len(c); j++ {
			outputLevel := c[j] + 1
			if c[j] == 0 {
				outputLevel = p.currBaseLevel
			}
			//			fmt.Printf("level: %d, outputLevel: %d, score: %f\n", c[j], outputLevel, p.scores[c[j]])
			if !levelsWithCompactions[c[j]] && !levelsWithCompactions[outputLevel] {
				p.compactionQueue = append(p.compactionQueue,
					compactionInfo{score: p.scores[c[j]], level: c[j], outputLevel: outputLevel})
				numCompactions--
				levelsWithCompactions[c[j]] = true
				levelsWithCompactions[outputLevel] = true
				fmt.Fprintf(&logString, "scc&ncc(l): %d,", c[j])
			}
		}
	}

	// Select the seed file for each compaction in the compactionQueue.
	for i := 0; i < len(p.compactionQueue); i++ {
		// TODO(peter): Select the file within the level to compact. See the
		// kMinOverlappingRatio heuristic in RocksDB which chooses the file with the
		// minimum overlapping ratio with the next level. This minimizes write
		// amplification. We also want to computed a "compensated size" which adjusts
		// the size of a table based on the number of deletions it contains.
		//
		// We want to minimize write amplification, but also ensure that deletes
		// are propagated to the bottom level in a timely fashion so as to reclaim
		// disk space. A table's smallest sequence number provides a measure of its
		// age. The ratio of overlapping-bytes / table-size gives an indication of
		// write amplification (a smaller ratio is preferrable).
		//
		// Simulate various workloads:
		// - Uniform random write
		// - Uniform random write+delete
		// - Skewed random write
		// - Skewed random write+delete
		// - Sequential write
		// - Sequential write+delete (queue)
		//
		// The current heuristic matches the RocksDB kOldestSmallestSeqFirst
		// heuristic.
		smallestSeqNum := uint64(math.MaxUint64)
		files := p.vers.Files[p.compactionQueue[i].level]
		for j := range files {
			f := &files[j]
			if smallestSeqNum > f.SmallestSeqNum {
				smallestSeqNum = f.SmallestSeqNum
				p.compactionQueue[i].file = j
			}
		}
	}

	// If there is compaction capacity left, we may no longer be experiencing heavy load and
	// can drain lower levels.
	// TODO(sbhola): potentially introduce a time delay before doing this if heavy load has short
	// lulls which allow the system to catchup on compactions but not yet worthwhile to drain.
	if numCompactions > 0 && len(inProgressCompactions) == 0 && len(p.compactionQueue) == 0 &&
		p.highestBaseLevel > p.currBaseLevel && p.scores[0] < 0.5 && p.scores[p.currBaseLevel] < 1 &&
		p.scores[p.currBaseLevel+1] < 1 {
		// Both levels have < LBaseMaxBytes so ok to compact all files.
		p.compactionQueue = append(p.compactionQueue,
			compactionInfo{
				score:       p.scores[p.currBaseLevel],
				level:       p.currBaseLevel,
				outputLevel: p.currBaseLevel + 1,
				file:        -1,
			})
	}

	// TODO(peter): When a snapshot is released, we may need to compact tables at
	// the bottom level in order to free up entries that were pinned by the
	// snapshot.
}

// TODO(sbhola):
// Consider keeping the full list of potential compactions since manual compactions can cause
// us to not be able to do some of these compactions. Should we remove all the conflict checking
// from the previous method?

// pickAuto picks the best compaction, if any.
func (p *compactionPicker2) pickAuto(opts *Options, bytesCompacted *uint64) (c *compaction) {
	for len(p.compactionQueue) > 0 {
		cInfo := p.compactionQueue[0]
		p.compactionQueue = p.compactionQueue[1:]
		if p.levelsWithCompactions[cInfo.level] || p.levelsWithCompactions[cInfo.outputLevel] {
			continue
		}
		level := cInfo.level
		if level == 0 && cInfo.outputLevel < p.currBaseLevel {
			p.currBaseLevel = cInfo.outputLevel
		}
		baseLevel := p.currBaseLevel
		vers := p.vers
		c = newCompaction(opts, vers, level, baseLevel, bytesCompacted)
		if cInfo.file == -1 {
			c.inputs[0] = vers.Files[c.startLevel]
		} else {
			c.inputs[0] = vers.Files[c.startLevel][cInfo.file : cInfo.file+1]
			// Files in level 0 may overlap each other, so pick up all overlapping ones.
			if c.startLevel == 0 {
				cmp := opts.Comparer.Compare
				smallest, largest := manifest.KeyRange(cmp, c.inputs[0], nil)
				c.inputs[0] = vers.Overlaps(0, cmp, smallest.UserKey, largest.UserKey)
				if len(c.inputs[0]) == 0 {
					panic("pebble: empty compaction")
				}
			}
		}

		c.setupInputs()
		p.levelsWithCompactions[level] = true
		p.levelsWithCompactions[cInfo.outputLevel] = true
		return c
	}
	return nil
}

func (p *compactionPicker2) pickManual(
	opts *Options, manual *manualCompaction, bytesCompacted *uint64,
) (c *compaction, err error) {
	if p == nil {
		return nil, nil
	}

	cur := p.vers
	outputLevel := manual.level + 1
	if manual.level == 0 {
		outputLevel = p.currBaseLevel
	}
	if p.levelsWithCompactions[manual.level] || p.levelsWithCompactions[outputLevel] {
		return nil, fmt.Errorf("need to wait")
	}
	// fmt.Printf("pickManual checked: %d, %d\n", manual.level, outputLevel)
	c = newCompaction(opts, cur, manual.level, p.currBaseLevel, bytesCompacted)
	if c.outputLevel != outputLevel {
		panic("pebble: manual compaction picked unexpected output level")
	}
	manual.outputLevel = c.outputLevel
	cmp := opts.Comparer.Compare
	c.inputs[0] = cur.Overlaps(manual.level, cmp, manual.start.UserKey, manual.end.UserKey)
	if len(c.inputs[0]) == 0 {
		// fmt.Printf("pickManual: no inputs\n")
		return nil, nil
	}
	p.levelsWithCompactions[manual.level] = true
	p.levelsWithCompactions[manual.outputLevel] = true
	// fmt.Printf("set to true: %d, %d\n", manual.level, manual.outputLevel)
	c.setupInputs()
	return c, nil
}
