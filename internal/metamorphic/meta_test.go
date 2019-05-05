// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package metamorphic

import "testing"

func TestMeta(t *testing.T) {
	// TODO(peter): Run the test repeatedly with different options. In between
	// each step, randomly flush or compact. Verify the histories of each test
	// are identical. Options to randomize:
	//
	//   LevelOptions
	//     BlockRestartInterval
	//     BlockSize
	//     BlockSizeThreshold
	//     Compression
	//     FilterType
	//     TargetFileSize
	//   Options
	//     Cache size
	//     L0CompactionThreshold
	//     L0SlowdownWritesThreshold
	//     L0StopWritesThreshold
	//     L1MaxBytes
	//     MaxManifestFileSize
	//     MaxOpenFiles
	//     MemTableSize
	//     MemTableStopWritesThreshold
	//
	// In addition to random options, there should be a list of specific option
	// configurations, such as 0 size cache, 1-byte TargetFileSize, 1-byte
	// L1MaxBytes, etc. These extrema configurations will help exercise edge
	// cases.
	//
	// Verify the metamorphic test catchs various bugs:
	// - Instability of keys returned from range-del-iters and used by truncate
	// - Lack of support for lower/upper bound in flushableBatchIter
	g := newGenerator()
	g.makeOps(5000, 10000)
	m := g.newTest()
	// TODO(peter): The history should probably be stored to a file on disk.
	h := &history{}
	if err := m.init(h); err != nil {
		t.Fatal(err)
	}
	for m.step(h) {
	}
	m.finish(h)
	if testing.Verbose() {
		t.Logf("%s", h)
	}
}
