package sstable

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
	"unsafe"

	"github.com/stretchr/testify/require"
)

func genRandomInts(rng *rand.Rand) [3]uint32 {
	var nums [3]uint32
	for i := range nums {
		// bits \in [1,32]
		bits := rng.Intn(32) + 1
		nums[i] = uint32(rng.Int63n(int64(uint64(1) << bits)))
	}
	return nums
}

func TestThreeVarints(t *testing.T) {
	fmt.Printf("ctrlState size: %d\n", unsafe.Sizeof(ctrlState{}))
	type testCase struct {
		a, b, c uint32
	}
	testCases := []testCase{
		{
			a: 0, b: 0, c: 0,
		},
		{
			a: 57, b: 453, c: 123457,
		},
	}
	encDecFunc := func(tc testCase, encode func(a, b, c uint32, buf []byte) int,
		decode func(unsafe.Pointer) (a, b, c uint32, p unsafe.Pointer), useGroupVarintLen bool) {
		fmt.Printf("%+v\n", tc)
		bufLength := 20
		if useGroupVarintLen {
			bufLength = lenThreeGroupVarints(tc.a, tc.b, tc.c)
		}
		buf := make([]byte, bufLength)
		if cap(buf) > bufLength {
			fmt.Printf("cap-len: %d\n", cap(buf)-bufLength)
		}
		n1 := encode(tc.a, tc.b, tc.c, buf)
		if useGroupVarintLen && n1 != bufLength {
			t.Fatalf("%d != %d", n1, bufLength)
		}
		ptr := unsafe.Pointer(&buf[0])
		a, b, c, ptr2 := decode(ptr)
		require.Equal(t, tc.a, a)
		require.Equal(t, tc.b, b)
		require.Equal(t, tc.c, c)
		require.Equal(t, n1, int(uintptr(ptr2)-uintptr(ptr)))
	}
	runTestCase := func(tc testCase) {
		t.Run(fmt.Sprintf("%+v", tc), func(t *testing.T) {
			encDecFunc(tc, writeThreeVarints, readThreeVarints, false)
			encDecFunc(tc, writeThreeGroupVarint, readThreeGroupVarint1, true)
			encDecFunc(tc, writeThreeGroupVarint, readThreeGroupVarint2, true)
			encDecFunc(tc, writeThreeGroupVarint, readThreeGroupVarint3, true)
			encDecFunc(tc, writeThreeGroupVarint, readThreeGroupVarint4, true)
			encDecFunc(tc, writeThreeGroupVarint, readThreeGroupVarint5, true)
		})
	}
	for _, tc := range testCases {
		runTestCase(tc)
	}
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < 10000; i++ {
		nums := genRandomInts(rng)
		tc := testCase{a: nums[0], b: nums[1], c: nums[2]}
		runTestCase(tc)
	}
}

func runEncodeBenchmark(b *testing.B, encode func(a, b, c uint32, buf []byte) int) {
	buf := make([]byte, 50)
	const x = 234789
	for i := 0; i < b.N; i++ {
		encode(x, x, x, buf)
	}
}

func BenchmarkVarintGroupEncode(b *testing.B) {
	runEncodeBenchmark(b, writeThreeGroupVarint)
}

func BenchmarkVarintEncode(b *testing.B) {
	runEncodeBenchmark(b, writeThreeVarints)
}

type benchCase struct {
	buf  [15]byte
	nums [3]uint32
	n    int
}

func runDecodeBenchmark(b *testing.B, encode func(a, b, c uint32, buf []byte) int,
	decode func(unsafe.Pointer) (a, b, c uint32, p unsafe.Pointer)) {
	rng := rand.New(rand.NewSource(0))
	cases := make([]benchCase, 500)
	for i := 0; i < 500; i++ {
		cases[i].nums = genRandomInts(rng)
		cases[i].n = encode(cases[i].nums[0], cases[i].nums[1], cases[i].nums[2], cases[i].buf[:])
	}
	var ptr2 unsafe.Pointer
	var a1, b1, c1 uint32
	j := 0
	for i := 0; i < b.N; i++ {
		j = (j + 1) % 500
		a1, b1, c1, ptr2 = decode(unsafe.Pointer(&cases[j].buf[0]))
	}
	if b.N > 0 && int(uintptr(ptr2)-uintptr(unsafe.Pointer(&cases[j].buf[0]))) != cases[j].n {
		b.Fatalf("%d %d %d", a1, b1, c1)
	}
}

/*
func runDecodeBenchmark2(b *testing.B, encode func(a, b, c uint32, buf []byte) int,
	decode func(unsafe.Pointer) (a, b, c uint32, p unsafe.Pointer)) {
	buf := make([]byte, 50)
	const x = 234789
	n := encode(x, x, x, buf)
	ptr := unsafe.Pointer(&buf[0])
	var ptr2 unsafe.Pointer
	var a1, b1, c1 uint32
	for i := 0; i < b.N; i++ {
		a1, b1, c1, ptr2 = decode(ptr)
	}
	if b.N > 0 && int(uintptr(ptr2)-uintptr(ptr)) != n {
		b.Fatalf("%d %d %d", a1, b1, c1)
	}
}
*/

func BenchmarkVarintGroupDecode1(b *testing.B) {
	runDecodeBenchmark(b, writeThreeGroupVarint, readThreeGroupVarint1)
}

func BenchmarkVarintGroupDecode2(b *testing.B) {
	runDecodeBenchmark(b, writeThreeGroupVarint, readThreeGroupVarint2)
}

func BenchmarkVarintGroupDecode3(b *testing.B) {
	runDecodeBenchmark(b, writeThreeGroupVarint, readThreeGroupVarint3)
}

func BenchmarkVarintGroupDecode4(b *testing.B) {
	runDecodeBenchmark(b, writeThreeGroupVarint, readThreeGroupVarint4)
}

func BenchmarkVarintGroupDecode5(b *testing.B) {
	runDecodeBenchmark(b, writeThreeGroupVarint, readThreeGroupVarint5)
}

func BenchmarkVarintDecode(b *testing.B) {
	runDecodeBenchmark(b, writeThreeVarints, readThreeVarints)
}
