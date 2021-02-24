// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package metamorphic

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"

	"github.com/cockroachdb/pebble"
)

// MVCC encoding and decoding routines adapted from CockroachDB sources. Used
// to perform apples-to-apples benchmarking for CockroachDB's usage of RocksDB.

var mvccComparer = &pebble.Comparer{
	Compare: mvccCompare,

	AbbreviatedKey: func(k []byte) uint64 {
		key, _, ok := mvccSplitKey(k)
		if !ok {
			return 0
		}
		return pebble.DefaultComparer.AbbreviatedKey(key)
	},

	Equal: func(a, b []byte) bool {
		return mvccCompare(a, b) == 0
	},

	Separator: func(dst, a, b []byte) []byte {
		aKey, _, ok := mvccSplitKey(a)
		if !ok {
			return append(dst, a...)
		}
		bKey, _, ok := mvccSplitKey(b)
		if !ok {
			return append(dst, a...)
		}
		// If the keys are the same just return a.
		if bytes.Equal(aKey, bKey) {
			return append(dst, a...)
		}
		n := len(dst)
		// MVCC key comparison uses bytes.Compare on the roachpb.Key, which is the same semantics as
		// pebble.DefaultComparer, so reuse the latter's Separator implementation.
		dst = pebble.DefaultComparer.Separator(dst, aKey, bKey)
		// Did it pick a separator different than aKey -- if it did not we can't do better than a.
		buf := dst[n:]
		if bytes.Equal(aKey, buf) {
			return append(dst[:n], a...)
		}
		// The separator is > aKey, so we only need to add the timestamp sentinel.
		return append(dst, 0)
	},

	Successor: func(dst, a []byte) []byte {
		aKey, _, ok := mvccSplitKey(a)
		if !ok {
			return append(dst, a...)
		}
		n := len(dst)
		// MVCC key comparison uses bytes.Compare on the roachpb.Key, which is the same semantics as
		// pebble.DefaultComparer, so reuse the latter's Successor implementation.
		dst = pebble.DefaultComparer.Successor(dst, aKey)
		// Did it pick a successor different than aKey -- if it did not we can't do better than a.
		buf := dst[n:]
		if bytes.Equal(aKey, buf) {
			return append(dst[:n], a...)
		}
		// The successor is > aKey, so we only need to add the timestamp sentinel.
		return append(dst, 0)
	},

	Split: func(k []byte) int {
		key, _, ok := mvccSplitKey(k)
		if !ok {
			return len(k)
		}
		// This matches the behavior of libroach/KeyPrefix. RocksDB requires that
		// keys generated via a SliceTransform be comparable with normal encoded
		// MVCC keys. Encoded MVCC keys have a suffix indicating the number of
		// bytes of timestamp data. MVCC keys without a timestamp have a suffix of
		// 0. We're careful in EncodeKey to make sure that the user-key always has
		// a trailing 0. If there is no timestamp this falls out naturally. If
		// there is a timestamp we prepend a 0 to the encoded timestamp data.
		return len(key) + 1
	},

	Name: "cockroach_comparator",
}

func mvccSplitKey(mvccKey []byte) (key []byte, ts []byte, ok bool) {
	if len(mvccKey) == 0 {
		return nil, nil, false
	}
	n := len(mvccKey) - 1
	tsLen := int(mvccKey[n])
	if n < tsLen {
		return nil, nil, false
	}
	key = mvccKey[:n-tsLen]
	if tsLen > 0 {
		ts = mvccKey[n-tsLen+1 : len(mvccKey)-1]
	}
	return key, ts, true
}

func mvccCompare(a, b []byte) int {
	// NB: For performance, this routine manually splits the key into the
	// user-key and timestamp components rather than using SplitMVCCKey. Don't
	// try this at home kids: use SplitMVCCKey.

	aEnd := len(a) - 1
	bEnd := len(b) - 1
	if aEnd < 0 || bEnd < 0 {
		// This should never happen unless there is some sort of corruption of
		// the keys. This is a little bizarre, but the behavior exactly matches
		// engine/db.cc:DBComparator.
		return bytes.Compare(a, b)
	}

	// Compute the index of the separator between the key and the timestamp.
	aSep := aEnd - int(a[aEnd])
	bSep := bEnd - int(b[bEnd])
	if aSep < 0 || bSep < 0 {
		// This should never happen unless there is some sort of corruption of
		// the keys. This is a little bizarre, but the behavior exactly matches
		// engine/db.cc:DBComparator.
		return bytes.Compare(a, b)
	}

	// Compare the "user key" part of the key.
	if c := bytes.Compare(a[:aSep], b[:bSep]); c != 0 {
		return c
	}

	// Compare the timestamp part of the key.
	aTS := a[aSep:aEnd]
	bTS := b[bSep:bEnd]
	if len(aTS) == 0 {
		if len(bTS) == 0 {
			return 0
		}
		return -1
	} else if len(bTS) == 0 {
		return 1
	}
	return bytes.Compare(bTS, aTS)
}

func prettyPrintKey(k []byte) string {
	if len(k) == 0 {
		return "\"\""
	}
	key, walltime := mvccDecode(k)
	return fmt.Sprintf("\"%s/%d\"", string(key), walltime)
}

func prettyParseKey(k string) []byte {
	var key string
	var walltime uint64
	ks := strings.Split(k, "/")
	if len(ks) != 2 {
		panic("expected two parts")
	}
	key = ks[0]
	t, err := strconv.Atoi(ks[1])
	if err != nil {
		panic(err)
	}
	walltime = uint64(t)
	// _, err := fmt.Sscanf(k, "%s/%d", &key, &walltime)
	// if err != nil {
	//	panic(err)
	// }
	return mvccEncode([]byte(key), walltime, 0)
}
func encodeUint32Ascending(b []byte, v uint32) []byte {
	return append(b, byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
}

func encodeUint64Ascending(b []byte, v uint64) []byte {
	return append(b,
		byte(v>>56), byte(v>>48), byte(v>>40), byte(v>>32),
		byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
}

func mvccDecode(encodedKey []byte) (key []byte, walltime uint64) {
	var ts []byte
	var ok bool
	key, ts, ok = mvccSplitKey(encodedKey)
	if !ok {
		panic("cannot decode key")
	}
	walltime = 0
	if len(ts) > 0 {
		if len(ts) != 8 {
			panic("unexpected length")
		}
		walltime = binary.BigEndian.Uint64(ts)
	}
	return key, walltime
}

// <key>\x00[<wall_time>[<logical>]]<#timestamp-bytes>
func mvccEncode(key []byte, walltime uint64, logical uint32) []byte {
	dst := append([]byte(nil), key...)
	dst = append(dst, 0)
	if walltime != 0 || logical != 0 {
		extra := byte(1 + 8)
		dst = encodeUint64Ascending(dst, walltime)
		if logical != 0 {
			dst = encodeUint32Ascending(dst, logical)
			extra += 4
		}
		dst = append(dst, extra)
	}
	return dst
}

var fauxMVCCMerger = &pebble.Merger{
	Name: "cockroach_merge_operator",
	Merge: func(key, value []byte) (pebble.ValueMerger, error) {
		// This merger is used by the compact benchmark and use the
		// pebble default value merger to concatenate values.
		// It shouldn't materially affect the benchmarks.
		return pebble.DefaultMerger.Merge(key, value)
	},
}
