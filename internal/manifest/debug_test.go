package manifest

import (
	"bytes"
	"fmt"
	"io"
	"math"
	"os"
	"sort"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/record"
	"github.com/stretchr/testify/require"
)

const (
	engineKeyNoVersion                             = 0
	engineKeyVersionWallTimeLen                    = 8
	engineKeyVersionWallAndLogicalTimeLen          = 12
	engineKeyVersionWallLogicalAndSyntheticTimeLen = 13
	engineKeyVersionLockTableLen                   = 17

	mvccEncodedTimeSentinelLen  = 1
	mvccEncodedTimeWallLen      = 8
	mvccEncodedTimeLogicalLen   = 4
	mvccEncodedTimeSyntheticLen = 1
	mvccEncodedTimeLengthLen    = 1
)

var zeroLogical [mvccEncodedTimeLogicalLen]byte

func readCockroachManifest(t *testing.T, filename string) (*Version, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	rr := record.NewReader(f, 0 /* logNum */)
	var v *Version
	addedByFileNum := make(map[base.FileNum]*FileMetadata)
	for {
		offset := rr.Offset()
		if offset == 5186696 {
			break
		}
		r, err := rr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		var ve VersionEdit
		if err = ve.Decode(r); err != nil {
			return nil, err
		}
		var bve BulkVersionEdit
		bve.AddedByFileNum = addedByFileNum
		if err := bve.Accumulate(&ve); err != nil {
			return nil, err
		}
		zombies := map[base.DiskFileNum]uint64{}
		if v, err = bve.Apply(v, EngineComparer.Compare, EngineComparer.FormatKey, 10<<20, 32000, zombies); err != nil {
			return nil, err
		}
		t.Logf("offset: %d, called AddL0Files %t\n", offset, ApplyCalledAddL0Files)
	}
	t.Log(v.L0Sublevels.describe(true /* verbose */))

	t.Logf("---------------------------------------\n\n")
	offset := rr.Offset()
	r, err := rr.Next()
	if err == io.EOF {
		return nil, errors.Errorf("foo")
	}
	if err != nil {
		return nil, err
	}
	var ve VersionEdit
	if err = ve.Decode(r); err != nil {
		return nil, err
	}
	entries := make([]DeletedFileEntry, 0, len(ve.DeletedFiles))
	for df := range ve.DeletedFiles {
		entries = append(entries, df)
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].Level != entries[j].Level {
			return entries[i].Level < entries[j].Level
		}
		return entries[i].FileNum < entries[j].FileNum
	})
	compactedFiles := map[base.FileNum]struct{}{}
	for _, df := range entries {
		t.Logf("  deleted:       L%d %s\n", df.Level, df.FileNum)
		compactedFiles[df.FileNum] = struct{}{}
	}
	var meta *FileMetadata
	var seedMeta *FileMetadata
	minIntervalIndex := math.MaxInt
	maxIntervalIndex := math.MinInt
	for i := 0; i < len(v.L0Sublevels.levelFiles); i++ {
		files := v.L0Sublevels.levelFiles[i]
		for _, f := range files {
			if _, ok := compactedFiles[f.FileNum]; ok {
				t.Logf("0.%2d: %7d [%6d,%6d]\n", i, f.FileNum, f.minIntervalIndex, f.maxIntervalIndex)
				if f.minIntervalIndex < minIntervalIndex {
					minIntervalIndex = f.minIntervalIndex
				}
				if f.maxIntervalIndex > maxIntervalIndex {
					maxIntervalIndex = f.maxIntervalIndex
				}
			}
			if f.FileNum == 2105037 {
				meta = f
			}
			if f.FileNum == 2105041 {
				seedMeta = f
			}
		}
	}
	t.Logf("**0.%2d: %7d [%6d,%6d]\n", meta.SubLevel, meta.FileNum, meta.minIntervalIndex, meta.maxIntervalIndex)
	t.Logf("**0.%2d: %7d [%6d,%6d]\n", seedMeta.SubLevel, seedMeta.FileNum, seedMeta.minIntervalIndex, seedMeta.maxIntervalIndex)
	for i := seedMeta.minIntervalIndex; i <= seedMeta.maxIntervalIndex; i++ {
		if i != 75 {
			continue
		}
		c := v.L0Sublevels.intraL0CompactionUsingSeed(seedMeta, i, 11097369955, 4)
		if c != nil {
			found2105037 := false
			found2105038 := false
			found2105034 := false
			for _, f := range c.Files {
				if f.FileNum == 2105037 {
					found2105037 = true
				}
				if f.FileNum == 2105038 {
					found2105038 = true
				}
				if f.FileNum == 2105034 {
					found2105034 = true
				}
			}
			prefix := ""
			if found2105037 != found2105038 {
				prefix = "*"
			}
			t.Logf("%sseed compaction %d: 2105034=%t, 2105037=%t, 2105038=%t\n", prefix, i,
				found2105034, found2105037, found2105038)
		}
	}
	for i := 0; i < len(v.L0Sublevels.orderedIntervals); i++ {
		interval := v.L0Sublevels.orderedIntervals[i]
		n := len(interval.files)
		if n == 0 {
			continue
		}
		f := interval.files[n-1]
		c := v.L0Sublevels.intraL0CompactionUsingSeed(f, i, math.MaxUint64, 4)
		if c != nil {
			found2105037 := false
			found2105038 := false
			for _, f := range c.Files {
				if f.FileNum == 2105037 {
					found2105037 = true
				}
				if f.FileNum == 2105038 {
					found2105038 = true
				}
			}
			prefix := ""
			if found2105037 != found2105038 {
				prefix = "*"
			}
			t.Logf("%sinterval compaction %d: 2105037=%t, 2105038=%t\n", prefix, i, found2105037, found2105038)
		}
	}
	var bve BulkVersionEdit
	bve.AddedByFileNum = addedByFileNum
	if err := bve.Accumulate(&ve); err != nil {
		return nil, err
	}
	zombies := map[base.DiskFileNum]uint64{}
	if v, err = bve.Apply(v, EngineComparer.Compare, EngineComparer.FormatKey, 10<<20, 32000, zombies); err != nil {
		return nil, err
	}
	t.Logf("offset: %d, called AddL0Files %t\n", offset, ApplyCalledAddL0Files)

	t.Logf("---------------------------------------\n\n")

	t.Log(v.L0Sublevels.describe(true /* verbose */))


	return v, nil
}

func TestL0Sublevels_BUG(t *testing.T) {
	_, err := readCockroachManifest(t, "testdata/MANIFEST-2065751")
	require.NoError(t, err)
}

// EngineComparer is a Comparer object that implements MVCC-specific
// comparator settings for use with Pebble.
var EngineComparer = &base.Comparer{
	Compare: EngineKeyCompare,

	Equal: EngineKeyEqual,

	AbbreviatedKey: func(k []byte) uint64 {
		key, ok := GetKeyPartFromEngineKey(k)
		if !ok {
			return 0
		}
		return base.DefaultComparer.AbbreviatedKey(key)
	},

	FormatKey: func(k []byte) fmt.Formatter {
		decoded, _ := DecodeEngineKey(k)
		return EngineKeyFormatter{key: decoded}
	},

	Separator: func(dst, a, b []byte) []byte {
		aKey, ok := GetKeyPartFromEngineKey(a)
		if !ok {
			return append(dst, a...)
		}
		bKey, ok := GetKeyPartFromEngineKey(b)
		if !ok {
			return append(dst, a...)
		}
		// If the keys are the same just return a.
		if bytes.Equal(aKey, bKey) {
			return append(dst, a...)
		}
		n := len(dst)
		// Engine key comparison uses bytes.Compare on the roachpb.Key, which is the same semantics as
		// pebble.DefaultComparer, so reuse the latter's Separator implementation.
		dst = base.DefaultComparer.Separator(dst, aKey, bKey)
		// Did it pick a separator different than aKey -- if it did not we can't do better than a.
		buf := dst[n:]
		if bytes.Equal(aKey, buf) {
			return append(dst[:n], a...)
		}
		// The separator is > aKey, so we only need to add the sentinel.
		return append(dst, 0)
	},

	Successor: func(dst, a []byte) []byte {
		aKey, ok := GetKeyPartFromEngineKey(a)
		if !ok {
			return append(dst, a...)
		}
		n := len(dst)
		// Engine key comparison uses bytes.Compare on the roachpb.Key, which is the same semantics as
		// pebble.DefaultComparer, so reuse the latter's Successor implementation.
		dst = base.DefaultComparer.Successor(dst, aKey)
		// Did it pick a successor different than aKey -- if it did not we can't do better than a.
		buf := dst[n:]
		if bytes.Equal(aKey, buf) {
			return append(dst[:n], a...)
		}
		// The successor is > aKey, so we only need to add the sentinel.
		return append(dst, 0)
	},

	ImmediateSuccessor: func(dst, a []byte) []byte {
		// The key `a` is guaranteed to be a bare prefix: It's a
		// `engineKeyNoVersion` key without a version—just a trailing 0-byte to
		// signify the length of the version. For example the user key "foo" is
		// encoded as: "foo\0". We need to encode the immediate successor to
		// "foo", which in the natural byte ordering is "foo\0".  Append a
		// single additional zero, to encode the user key "foo\0" with a
		// zero-length version.
		return append(append(dst, a...), 0)
	},

	Split: func(k []byte) int {
		keyLen := len(k)
		if keyLen == 0 {
			return 0
		}
		// Last byte is the version length + 1 when there is a version,
		// else it is 0.
		versionLen := int(k[keyLen-1])
		// keyPartEnd points to the sentinel byte.
		keyPartEnd := keyLen - 1 - versionLen
		if keyPartEnd < 0 {
			return keyLen
		}
		// Pebble requires that keys generated via a split be comparable with
		// normal encoded engine keys. Encoded engine keys have a suffix
		// indicating the number of bytes of version data. Engine keys without a
		// version have a suffix of 0. We're careful in EncodeKey to make sure
		// that the user-key always has a trailing 0. If there is no version this
		// falls out naturally. If there is a version we prepend a 0 to the
		// encoded version data.
		return keyPartEnd + 1
	},

	Name: "cockroach_comparator",
}

// EngineKeyFormatter is a fmt.Formatter for EngineKeys.
type EngineKeyFormatter struct {
	key EngineKey
}

var _ fmt.Formatter = EngineKeyFormatter{}

// Format implements the fmt.Formatter interface.
func (m EngineKeyFormatter) Format(f fmt.State, c rune) {
	m.key.Format(f, c)
}

type EngineKey struct {
	Key     []byte
	Version []byte
}

// Format implements the fmt.Formatter interface
func (k EngineKey) Format(f fmt.State, c rune) {
	fmt.Fprintf(f, "%s/%x", k.Key, k.Version)
}

// IsMVCCKey returns true if the key can be decoded as an MVCCKey.
// This includes the case of an empty timestamp.
func (k EngineKey) IsMVCCKey() bool {
	l := len(k.Version)
	return l == engineKeyNoVersion ||
		l == engineKeyVersionWallTimeLen ||
		l == engineKeyVersionWallAndLogicalTimeLen ||
		l == engineKeyVersionWallLogicalAndSyntheticTimeLen
}

// GetKeyPartFromEngineKey is a specialization of DecodeEngineKey which avoids
// constructing a slice for the version part of the key, since the caller does
// not need it.
func GetKeyPartFromEngineKey(engineKey []byte) (key []byte, ok bool) {
	if len(engineKey) == 0 {
		return nil, false
	}
	// Last byte is the version length + 1 when there is a version,
	// else it is 0.
	versionLen := int(engineKey[len(engineKey)-1])
	// keyPartEnd points to the sentinel byte.
	keyPartEnd := len(engineKey) - 1 - versionLen
	if keyPartEnd < 0 {
		return nil, false
	}
	// Key excludes the sentinel byte.
	return engineKey[:keyPartEnd], true
}

// EngineKeyCompare compares cockroach keys, including the version (which
// could be MVCC timestamps).
func EngineKeyCompare(a, b []byte) int {
	// NB: For performance, this routine manually splits the key into the
	// user-key and version components rather than using DecodeEngineKey. In
	// most situations, use DecodeEngineKey or GetKeyPartFromEngineKey or
	// SplitMVCCKey instead of doing this.
	aEnd := len(a) - 1
	bEnd := len(b) - 1
	if aEnd < 0 || bEnd < 0 {
		// This should never happen unless there is some sort of corruption of
		// the keys.
		return bytes.Compare(a, b)
	}

	// Compute the index of the separator between the key and the version. If the
	// separator is found to be at -1 for both keys, then we are comparing bare
	// suffixes without a user key part. Pebble requires bare suffixes to be
	// comparable with the same ordering as if they had a common user key.
	aSep := aEnd - int(a[aEnd])
	bSep := bEnd - int(b[bEnd])
	if aSep == -1 && bSep == -1 {
		aSep, bSep = 0, 0 // comparing bare suffixes
	}
	if aSep < 0 || bSep < 0 {
		// This should never happen unless there is some sort of corruption of
		// the keys.
		return bytes.Compare(a, b)
	}

	// Compare the "user key" part of the key.
	if c := bytes.Compare(a[:aSep], b[:bSep]); c != 0 {
		return c
	}

	// Compare the version part of the key. Note that when the version is a
	// timestamp, the timestamp encoding causes byte comparison to be equivalent
	// to timestamp comparison.
	aVer := a[aSep:aEnd]
	bVer := b[bSep:bEnd]
	if len(aVer) == 0 {
		if len(bVer) == 0 {
			return 0
		}
		return -1
	} else if len(bVer) == 0 {
		return 1
	}
	aVer = normalizeEngineKeyVersionForCompare(aVer)
	bVer = normalizeEngineKeyVersionForCompare(bVer)
	return bytes.Compare(bVer, aVer)
}

func normalizeEngineKeyVersionForCompare(a []byte) []byte {
	// In general, the version could also be a non-timestamp version, but we know
	// that engineKeyVersionLockTableLen+mvccEncodedTimeSentinelLen is a different
	// constant than the above, so there is no danger here of stripping parts from
	// a non-timestamp version.
	const withWall = mvccEncodedTimeSentinelLen + mvccEncodedTimeWallLen
	const withLogical = withWall + mvccEncodedTimeLogicalLen
	const withSynthetic = withLogical + mvccEncodedTimeSyntheticLen
	if len(a) == withSynthetic {
		// Strip the synthetic bit component from the timestamp version. The
		// presence of the synthetic bit does not affect key ordering or equality.
		a = a[:withLogical]
	}
	if len(a) == withLogical {
		// If the timestamp version contains a logical timestamp component that is
		// zero, strip the component. encodeMVCCTimestampToBuf will typically omit
		// the entire logical component in these cases as an optimization, but it
		// does not guarantee to never include a zero logical component.
		// Additionally, we can fall into this case after stripping off other
		// components of the key version earlier on in this function.
		if bytes.Equal(a[withWall:], zeroLogical[:]) {
			a = a[:withWall]
		}
	}
	return a
}

// DecodeEngineKey decodes the given bytes as an EngineKey. If the caller
// already knows that the key is an MVCCKey, the Version returned is the
// encoded timestamp.
func DecodeEngineKey(b []byte) (key EngineKey, ok bool) {
	if len(b) == 0 {
		return EngineKey{}, false
	}
	// Last byte is the version length + 1 when there is a version,
	// else it is 0.
	versionLen := int(b[len(b)-1])
	// keyPartEnd points to the sentinel byte.
	keyPartEnd := len(b) - 1 - versionLen
	if keyPartEnd < 0 {
		return EngineKey{}, false
	}
	// Key excludes the sentinel byte.
	key.Key = b[:keyPartEnd]
	if versionLen > 0 {
		// Version consists of the bytes after the sentinel and before the length.
		key.Version = b[keyPartEnd+1 : len(b)-1]
	}
	return key, true
}

// EngineKeyEqual checks for equality of cockroach keys, including the version
// (which could be MVCC timestamps).
func EngineKeyEqual(a, b []byte) bool {
	// NB: For performance, this routine manually splits the key into the
	// user-key and version components rather than using DecodeEngineKey. In
	// most situations, use DecodeEngineKey or GetKeyPartFromEngineKey or
	// SplitMVCCKey instead of doing this.
	aEnd := len(a) - 1
	bEnd := len(b) - 1
	if aEnd < 0 || bEnd < 0 {
		// This should never happen unless there is some sort of corruption of
		// the keys.
		return bytes.Equal(a, b)
	}

	// Last byte is the version length + 1 when there is a version,
	// else it is 0.
	aVerLen := int(a[aEnd])
	bVerLen := int(b[bEnd])

	// Fast-path. If the key version is empty or contains only a walltime
	// component then normalizeEngineKeyVersionForCompare is a no-op, so we don't
	// need to split the "user key" from the version suffix before comparing to
	// compute equality. Instead, we can check for byte equality immediately.
	const withWall = mvccEncodedTimeSentinelLen + mvccEncodedTimeWallLen
	const withLockTableLen = mvccEncodedTimeSentinelLen + engineKeyVersionLockTableLen
	if (aVerLen <= withWall && bVerLen <= withWall) || (aVerLen == withLockTableLen && bVerLen == withLockTableLen) {
		return bytes.Equal(a, b)
	}

	// Compute the index of the separator between the key and the version. If the
	// separator is found to be at -1 for both keys, then we are comparing bare
	// suffixes without a user key part. Pebble requires bare suffixes to be
	// comparable with the same ordering as if they had a common user key.
	aSep := aEnd - aVerLen
	bSep := bEnd - bVerLen
	if aSep == -1 && bSep == -1 {
		aSep, bSep = 0, 0 // comparing bare suffixes
	}
	if aSep < 0 || bSep < 0 {
		// This should never happen unless there is some sort of corruption of
		// the keys.
		return bytes.Equal(a, b)
	}

	// Compare the "user key" part of the key.
	if !bytes.Equal(a[:aSep], b[:bSep]) {
		return false
	}

	// Compare the version part of the key.
	aVer := a[aSep:aEnd]
	bVer := b[bSep:bEnd]
	aVer = normalizeEngineKeyVersionForCompare(aVer)
	bVer = normalizeEngineKeyVersionForCompare(bVer)
	return bytes.Equal(aVer, bVer)
}
