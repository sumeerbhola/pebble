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

// compactionPicker holds the state and logic for picking a compaction. A
// compaction picker is associated with a single version. A new compaction
// picker is created and initialized every time a new version is installed.
type compactionPicker struct {
	opts *Options
	vers *version

	// The level to target for L0 compactions. Levels L1 to baseLevel must be
	// empty.
	baseLevel int

	// estimatedMaxWAmp is the estimated maximum write amp per byte that is
	// added to L0.
	estimatedMaxWAmp float64

	// smoothedLevelMultiplier is the size ratio between one level and the next.
	smoothedLevelMultiplier float64

	// levelMaxBytes holds the dynamically adjusted max bytes setting for each
	// level.
	levelMaxBytes [numLevels]int64

	scores                [numLevels]float64
	compactionQueue       []compactionInfo
	levelsWithCompactions map[int]bool
}

var _ compactionPickerInterface = &compactionPicker{}

func newCompactionPicker(
	v *version,
	opts *Options,
	maxConcurrentCompactions int,
	inProgressCompactions []inProgressCompactionInfo,
) *compactionPicker {
	p := &compactionPicker{
		opts: opts,
		vers: v,
	}
	p.initLevelMaxBytes(v, opts)
	n := maxConcurrentCompactions - len(inProgressCompactions)
	p.initCompactionQueue(v, opts, n, inProgressCompactions)
	return p
}

func (p *compactionPicker) getBaseLevel() int {
	if p == nil {
		return 1
	}
	return p.baseLevel
}

func (p *compactionPicker) getEstimatedMaxWAmp() float64 {
	return p.estimatedMaxWAmp
}

func (p *compactionPicker) getLevelMaxBytes() [numLevels]int64 {
	return p.levelMaxBytes
}

// estimatedCompactionDebt estimates the number of bytes which need to be
// compacted before the LSM tree becomes stable.
func (p *compactionPicker) estimatedCompactionDebt(l0ExtraSize uint64) uint64 {
	if p == nil {
		return 0
	}

	// We assume that all the bytes in L0 need to be compacted to L1. This is unlike
	// the RocksDB logic that figures out whether L0 needs compaction.
	compactionDebt := totalSize(p.vers.Files[0]) + l0ExtraSize
	bytesAddedToNextLevel := compactionDebt

	levelSize := totalSize(p.vers.Files[p.baseLevel])
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
	for level := p.baseLevel; level < numLevels-1; level++ {
		levelSize += bytesAddedToNextLevel
		bytesAddedToNextLevel = 0
		nextLevelSize = totalSize(p.vers.Files[level+1])
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

func (p *compactionPicker) initLevelMaxBytes(v *version, opts *Options) {
	// Determine the first non-empty level and the maximum size of any level.
	firstNonEmptyLevel := -1
	var bottomLevelSize int64
	for level := 1; level < numLevels; level++ {
		levelSize := int64(totalSize(v.Files[level]))
		if levelSize > 0 {
			if firstNonEmptyLevel == -1 {
				firstNonEmptyLevel = level
			}
			bottomLevelSize = levelSize
		}
	}

	// Initialize the max-bytes setting for each level to "infinity" which will
	// disallow compaction for that level. We'll fill in the actual value below
	// for levels we want to allow compactions from.
	for level := 0; level < numLevels; level++ {
		p.levelMaxBytes[level] = math.MaxInt64
	}

	if bottomLevelSize == 0 {
		// No levels for L1 and up contain any data. Target L0 compactions for the
		// last level.
		p.baseLevel = numLevels - 1
		return
	}

	levelMultiplier := 10.0

	baseBytesMax := opts.LBaseMaxBytes
	baseBytesMin := int64(float64(baseBytesMax) / levelMultiplier)

	curLevelSize := bottomLevelSize
	for level := numLevels - 2; level >= firstNonEmptyLevel; level-- {
		curLevelSize = int64(float64(curLevelSize) / levelMultiplier)
	}

	if curLevelSize <= baseBytesMin {
		// If we make target size of last level to be bottomLevelSize, target size of
		// the first non-empty level would be smaller than baseBytesMin. We set it
		// be baseBytesMin.
		p.baseLevel = firstNonEmptyLevel
	} else {
		// Compute base level (where L0 data is compacted to).
		p.baseLevel = firstNonEmptyLevel
		for p.baseLevel > 1 && curLevelSize > baseBytesMax {
			p.baseLevel--
			curLevelSize = int64(float64(curLevelSize) / levelMultiplier)
		}
	}

	if p.baseLevel < numLevels-1 {
		p.smoothedLevelMultiplier = math.Pow(
			float64(bottomLevelSize)/float64(baseBytesMax),
			1.0/float64(numLevels-p.baseLevel-1))
	} else {
		p.smoothedLevelMultiplier = 1.0
	}

	p.estimatedMaxWAmp = float64(numLevels-p.baseLevel) * (p.smoothedLevelMultiplier + 1)

	levelSize := float64(baseBytesMax)
	for level := p.baseLevel; level < numLevels; level++ {
		if level > p.baseLevel && levelSize > 0 {
			levelSize *= p.smoothedLevelMultiplier
		}
		// Round the result since test cases use small target level sizes, which
		// can be impacted by floating-point imprecision + integer truncation.
		roundedLevelSize := math.Round(levelSize)
		if roundedLevelSize > float64(math.MaxInt64) {
			p.levelMaxBytes[level] = math.MaxInt64
		} else {
			p.levelMaxBytes[level] = int64(roundedLevelSize)
		}
	}
}

// TODO(sbhola): revise comment.
// initTarget initializes the compaction score and level. If the compaction
// score indicates compaction is needed, a target table within the target level
// is selected for compaction.
func (p *compactionPicker) initCompactionQueue(
	v *version, opts *Options, numCompactions int, inProgressCompactions []inProgressCompactionInfo,
) {
	// We treat level-0 specially by bounding the number of files instead of
	// number of bytes for two reasons:
	//
	// (1) With larger write-buffer sizes, it is nice not to do too many
	// level-0 compactions.
	//
	// (2) The files in level-0 are merged on every read and therefore we
	// wish to avoid too many files when the individual file size is small
	// (perhaps because of a small write-buffer setting, or very high
	// compression ratios, or lots of overwrites/deletions).
	p.scores[0] = float64(len(v.Files[0])) / float64(opts.L0CompactionThreshold)

	for level := 1; level < numLevels-1; level++ {
		p.scores[level] = float64(totalSize(v.Files[level])) / float64(p.levelMaxBytes[level])
	}

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

	var potentialCompactions []int
	for level := 0; level < numLevels-1; level++ {
		if p.scores[level] >= 1 {
			potentialCompactions = append(potentialCompactions, level)
			fmt.Fprintf(&logString, "c(l,s): %d,%f,", level, p.scores[level])
		}
	}
	sort.Sort(sortCompactionsDecreasingMeasure{
		level: potentialCompactions, measure: p.scores,
	})
	for i := 0; i < len(potentialCompactions) && numCompactions > 0; i++ {
		level := potentialCompactions[i]
		outputLevel := level + 1
		if level == 0 {
			outputLevel = p.baseLevel
		}
		if !levelsWithCompactions[level] && !levelsWithCompactions[outputLevel] {
			p.compactionQueue = append(p.compactionQueue,
				compactionInfo{score: p.scores[level], level: level, outputLevel: outputLevel})
			numCompactions--
			levelsWithCompactions[level] = true
			levelsWithCompactions[level] = true
			fmt.Fprintf(&logString, "picked(l): %d,", level)
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

		// The current heuristic matches the RocksDB kOldestSmallestSeqFirst
		// heuristic.
		smallestSeqNum := uint64(math.MaxUint64)
		files := v.Files[p.compactionQueue[i].level]
		for j := range files {
			f := &files[j]
			if smallestSeqNum > f.SmallestSeqNum {
				smallestSeqNum = f.SmallestSeqNum
				p.compactionQueue[i].file = j
			}
		}
		return
	}

	/*
		// No levels exceeded their size threshold. Check for forced compactions.
		for level := 0; level < numLevels-1; level++ {
			files := v.Files[p.level]
			for i := range files {
				f := &files[i]
				if f.MarkedForCompaction {
					p.score = 1.0
					p.level = level
					p.file = i
					return
				}
			}
		}
	*/

	// TODO(peter): When a snapshot is released, we may need to compact tables at
	// the bottom level in order to free up entries that were pinned by the
	// snapshot.
}

// pickAuto picks the best compaction, if any.
func (p *compactionPicker) pickAuto(opts *Options, bytesCompacted *uint64) (c *compaction) {
	for len(p.compactionQueue) > 0 {
		cInfo := p.compactionQueue[0]
		p.compactionQueue = p.compactionQueue[1:]
		if p.levelsWithCompactions[cInfo.level] || p.levelsWithCompactions[cInfo.outputLevel] {
			continue
		}
		level := cInfo.level
		vers := p.vers
		c = newCompaction(opts, vers, level, p.baseLevel, bytesCompacted)
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

		c.setupInputs()
		p.levelsWithCompactions[level] = true
		p.levelsWithCompactions[cInfo.outputLevel] = true
		return c
	}
	return nil
}

func (p *compactionPicker) pickManual(
	opts *Options, manual *manualCompaction, bytesCompacted *uint64,
) (c *compaction, err error) {
	if p == nil {
		return nil, nil
	}

	cur := p.vers
	outputLevel := manual.level + 1
	if manual.level == 0 {
		outputLevel = p.baseLevel
	}
	if p.levelsWithCompactions[manual.level] || p.levelsWithCompactions[outputLevel] {
		return nil, fmt.Errorf("need to wait")
	}
	// fmt.Printf("pickManual checked: %d, %d\n", manual.level, outputLevel)
	c = newCompaction(opts, cur, manual.level, p.baseLevel, bytesCompacted)
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

// =========

// compactionPicker holds the state and logic for picking a compaction. A
// compaction picker is associated with a single version. A new compaction
// picker is created and initialized every time a new version is installed.
type compactionPickerTest struct {
	opts *Options
	vers *version

	// The level to target for L0 compactions. Levels L1 to baseLevel must be
	// empty.
	baseLevel int

	// estimatedMaxWAmp is the estimated maximum write amp per byte that is
	// added to L0.
	estimatedMaxWAmp float64

	// smoothedLevelMultiplier is the size ratio between one level and the next.
	smoothedLevelMultiplier float64

	// levelMaxBytes holds the dynamically adjusted max bytes setting for each
	// level.
	levelMaxBytes [numLevels]int64

	// These fields are the level that should be compacted next and its
	// compaction score. A score < 1 means that compaction is not strictly
	// needed.
	score float64
	level int
	file  int
}

var _ compactionPickerInterface = &compactionPickerTest{}

func newCompactionPickerTest(v *version, opts *Options) *compactionPickerTest {
	p := &compactionPickerTest{
		opts: opts,
		vers: v,
	}
	p.initLevelMaxBytes(v, opts)
	p.initTarget(v, opts)
	return p
}

func (p *compactionPickerTest) getBaseLevel() int {
	return p.baseLevel
}
func (p *compactionPickerTest) getEstimatedMaxWAmp() float64 {
	return p.estimatedMaxWAmp
}
func (p *compactionPickerTest) getLevelMaxBytes() [numLevels]int64 {
	return p.levelMaxBytes
}

func (p *compactionPickerTest) compactionNeeded() bool {
	if p == nil {
		return false
	}
	return p.score >= 1
}

// estimatedCompactionDebt estimates the number of bytes which need to be
// compacted before the LSM tree becomes stable.
func (p *compactionPickerTest) estimatedCompactionDebt(l0ExtraSize uint64) uint64 {
	if p == nil {
		return 0
	}

	// We assume that all the bytes in L0 need to be compacted to L1. This is unlike
	// the RocksDB logic that figures out whether L0 needs compaction.
	compactionDebt := totalSize(p.vers.Files[0]) + l0ExtraSize
	bytesAddedToNextLevel := compactionDebt

	levelSize := totalSize(p.vers.Files[p.baseLevel])
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
	for level := p.baseLevel; level < numLevels-1; level++ {
		levelSize += bytesAddedToNextLevel
		bytesAddedToNextLevel = 0
		nextLevelSize = totalSize(p.vers.Files[level+1])
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

func (p *compactionPickerTest) initLevelMaxBytes(v *version, opts *Options) {
	// Determine the first non-empty level and the maximum size of any level.
	firstNonEmptyLevel := -1
	var bottomLevelSize int64
	for level := 1; level < numLevels; level++ {
		levelSize := int64(totalSize(v.Files[level]))
		if levelSize > 0 {
			if firstNonEmptyLevel == -1 {
				firstNonEmptyLevel = level
			}
			bottomLevelSize = levelSize
		}
	}

	// Initialize the max-bytes setting for each level to "infinity" which will
	// disallow compaction for that level. We'll fill in the actual value below
	// for levels we want to allow compactions from.
	for level := 0; level < numLevels; level++ {
		p.levelMaxBytes[level] = math.MaxInt64
	}

	if bottomLevelSize == 0 {
		// No levels for L1 and up contain any data. Target L0 compactions for the
		// last level.
		p.baseLevel = numLevels - 1
		return
	}

	levelMultiplier := 10.0

	baseBytesMax := opts.LBaseMaxBytes
	baseBytesMin := int64(float64(baseBytesMax) / levelMultiplier)

	curLevelSize := bottomLevelSize
	for level := numLevels - 2; level >= firstNonEmptyLevel; level-- {
		curLevelSize = int64(float64(curLevelSize) / levelMultiplier)
	}

	if curLevelSize <= baseBytesMin {
		// If we make target size of last level to be bottomLevelSize, target size of
		// the first non-empty level would be smaller than baseBytesMin. We set it
		// be baseBytesMin.
		p.baseLevel = firstNonEmptyLevel
	} else {
		// Compute base level (where L0 data is compacted to).
		p.baseLevel = firstNonEmptyLevel
		for p.baseLevel > 1 && curLevelSize > baseBytesMax {
			p.baseLevel--
			curLevelSize = int64(float64(curLevelSize) / levelMultiplier)
		}
	}

	if p.baseLevel < numLevels-1 {
		p.smoothedLevelMultiplier = math.Pow(
			float64(bottomLevelSize)/float64(baseBytesMax),
			1.0/float64(numLevels-p.baseLevel-1))
	} else {
		p.smoothedLevelMultiplier = 1.0
	}

	p.estimatedMaxWAmp = float64(numLevels-p.baseLevel) * (p.smoothedLevelMultiplier + 1)

	levelSize := float64(baseBytesMax)
	for level := p.baseLevel; level < numLevels; level++ {
		if level > p.baseLevel && levelSize > 0 {
			levelSize *= p.smoothedLevelMultiplier
		}
		// Round the result since test cases use small target level sizes, which
		// can be impacted by floating-point imprecision + integer truncation.
		roundedLevelSize := math.Round(levelSize)
		if roundedLevelSize > float64(math.MaxInt64) {
			p.levelMaxBytes[level] = math.MaxInt64
		} else {
			p.levelMaxBytes[level] = int64(roundedLevelSize)
		}
	}
}

// initTarget initializes the compaction score and level. If the compaction
// score indicates compaction is needed, a target table within the target level
// is selected for compaction.
func (p *compactionPickerTest) initTarget(v *version, opts *Options) {
	// We treat level-0 specially by bounding the number of files instead of
	// number of bytes for two reasons:
	//
	// (1) With larger write-buffer sizes, it is nice not to do too many
	// level-0 compactions.
	//
	// (2) The files in level-0 are merged on every read and therefore we
	// wish to avoid too many files when the individual file size is small
	// (perhaps because of a small write-buffer setting, or very high
	// compression ratios, or lots of overwrites/deletions).
	p.score = float64(len(v.Files[0])) / float64(opts.L0CompactionThreshold)
	p.level = 0

	for level := 1; level < numLevels-1; level++ {
		score := float64(totalSize(v.Files[level])) / float64(p.levelMaxBytes[level])
		if p.score < score {
			p.score = score
			p.level = level
		}
	}

	if p.score >= 1 {
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

		// The current heuristic matches the RocksDB kOldestSmallestSeqFirst
		// heuristic.
		smallestSeqNum := uint64(math.MaxUint64)
		files := v.Files[p.level]
		for i := range files {
			f := &files[i]
			if smallestSeqNum > f.SmallestSeqNum {
				smallestSeqNum = f.SmallestSeqNum
				p.file = i
			}
		}
		return
	}

	// No levels exceeded their size threshold. Check for forced compactions.
	for level := 0; level < numLevels-1; level++ {
		files := v.Files[p.level]
		for i := range files {
			f := &files[i]
			if f.MarkedForCompaction {
				p.score = 1.0
				p.level = level
				p.file = i
				return
			}
		}
	}

	// TODO(peter): When a snapshot is released, we may need to compact tables at
	// the bottom level in order to free up entries that were pinned by the
	// snapshot.
}

// pickAuto picks the best compaction, if any.
func (p *compactionPickerTest) pickAuto(opts *Options, bytesCompacted *uint64) (c *compaction) {
	if !p.compactionNeeded() {
		return nil
	}

	vers := p.vers
	c = newCompaction(opts, vers, p.level, p.baseLevel, bytesCompacted)
	c.inputs[0] = vers.Files[c.startLevel][p.file : p.file+1]

	// Files in level 0 may overlap each other, so pick up all overlapping ones.
	if c.startLevel == 0 {
		cmp := opts.Comparer.Compare
		smallest, largest := manifest.KeyRange(cmp, c.inputs[0], nil)
		c.inputs[0] = vers.Overlaps(0, cmp, smallest.UserKey, largest.UserKey)
		if len(c.inputs[0]) == 0 {
			panic("pebble: empty compaction")
		}
	}

	c.setupInputs()
	return c
}

func (p *compactionPickerTest) pickManual(
	opts *Options, manual *manualCompaction, bytesCompacted *uint64,
) (c *compaction, err error) {
	if p == nil {
		return nil, nil
	}

	cur := p.vers
	c = newCompaction(opts, cur, manual.level, p.baseLevel, bytesCompacted)
	manual.outputLevel = c.outputLevel
	cmp := opts.Comparer.Compare
	c.inputs[0] = cur.Overlaps(manual.level, cmp, manual.start.UserKey, manual.end.UserKey)
	if len(c.inputs[0]) == 0 {
		return nil, nil
	}
	c.setupInputs()
	return c, nil
}
