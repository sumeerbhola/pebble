// Copyright 2022 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"unsafe"
)

// Adapted from https://github.com/lemire/streamvbyte. Since we have only a
// single set of 3 integers, this is actually Varint-GB [1] as described in
// https://arxiv.org/pdf/1709.08990.pdf (the description here looks like big
// endian but what we implement below is little endian).
// [1] J. Dean, Challenges in building large-scale information retrieval
// systems: invited talk, in: Proceedings of the Second ACM International
// Conference on Web Search and Data Mining, WSDM ’09, ACM, New York, NY, USA,
// 2009, pp. 1–1.

// TODO: benchmark both with
// - representative values
// - random values.
// The reason for grouping is to reduce branch mis-prediction by a factor of 3.
// Also the data dependency in the if-else block in varint decoding is eliminated.

// So if we
// TODO: also compare the lengths! Group varint may be 1 byte longer in many cases

type ctrlState struct {
	masks   [3]uint32
	offsets [4]uint8
}

var ctrlToState [256]ctrlState

func init() {
	for a := uint8(1); a <= 4; a++ {
		for b := uint8(1); b <= 4; b++ {
			for c := uint8(1); c <= 4; c++ {
				for d := uint8(1); d <= 4; d++ {
					ctrl := a - 1
					ctrl = ctrl | ((b - 1) << 2)
					ctrl = ctrl | ((c - 1) << 4)
					ctrlToState[ctrl].offsets[0] = 1
					ctrlToState[ctrl].offsets[1] = (a + 1)
					ctrlToState[ctrl].offsets[2] = (a + b + 1)
					ctrlToState[ctrl].offsets[3] = (a + b + c + 1)
					computeMask := func(len uint8) uint32 {
						switch len {
						case 1:
							return 0xFF
						case 2:
							return 0xFFFF
						case 3:
							return 0xFFFFFF
						case 4:
							return 0xFFFFFFFF
						default:
							panic("")
						}
					}
					ctrlToState[ctrl].masks[0] = computeMask(a)
					ctrlToState[ctrl].masks[1] = computeMask(b)
					ctrlToState[ctrl].masks[2] = computeMask(c)
				}
			}
		}
	}
}

func lenThreeGroupVarints(a, b, c uint32) int {
	lenFunc := func(x uint32) int {
		length := 0
		for x >= 256 {
			x >>= 8
			length++
		}
		length++
		return length
	}
	return lenFunc(a) + lenFunc(b) + lenFunc(c) + 1
}

func writeThreeGroupVarint(a, b, c uint32, buf []byte) int {
	var lena, lenb, lenc uint8
	n := uint8(1)
	{
		x := a
		for x >= 256 {
			buf[n+lena] = byte(x & 0xff)
			x >>= 8
			lena++
		}
		buf[n+lena] = byte(x)
		lena++
		n += lena
	}
	{
		x := b
		for x >= 256 {
			buf[n+lenb] = byte(x & 0xff)
			x >>= 8
			lenb++
		}
		buf[n+lenb] = byte(x)
		lenb++
		n += lenb
	}
	{
		x := c
		for x >= 256 {
			buf[n+lenc] = byte(x & 0xff)
			x >>= 8
			lenc++
		}
		buf[n+lenc] = byte(x)
		lenc++
		n += lenc
	}
	buf[0] = (lena - 1) | (lenb-1)<<2 | (lenc-1)<<4
	return int(n)
}

func readThreeGroupVarint1(ptr unsafe.Pointer) (a, b, c uint32, p unsafe.Pointer) {
	var lena, lenb, lenc uint8
	ctrl := *(*uint8)(ptr)
	lena = (ctrl & 0x03) + 1
	lenb = ((ctrl >> 2) & 0x03) + 1
	lenc = ((ctrl >> 4) & 0x03) + 1
	{
		offset := uintptr(1)
		b0 := *(*uint8)(unsafe.Pointer(uintptr(ptr) + offset))
		switch lena {
		case 1:
			a = uint32(b0)
		case 2:
			b1 := *(*uint8)(unsafe.Pointer(uintptr(ptr) + offset + 1))
			a = uint32(b0) | uint32(b1)<<8
		case 3:
			b1 := *(*uint8)(unsafe.Pointer(uintptr(ptr) + offset + 1))
			b2 := *(*uint8)(unsafe.Pointer(uintptr(ptr) + offset + 2))
			a = uint32(b0) | uint32(b1)<<8 | uint32(b2)<<16
		case 4:
			b1 := *(*uint8)(unsafe.Pointer(uintptr(ptr) + offset + 1))
			b2 := *(*uint8)(unsafe.Pointer(uintptr(ptr) + offset + 2))
			b3 := *(*uint8)(unsafe.Pointer(uintptr(ptr) + offset + 3))
			a = uint32(b0) | uint32(b1)<<8 | uint32(b2)<<16 | uint32(b3)<<24
		}
	}
	{
		offset := uintptr(1 + lena)
		b0 := *(*uint8)(unsafe.Pointer(uintptr(ptr) + offset))
		switch lenb {
		case 1:
			b = uint32(b0)
		case 2:
			b1 := *(*uint8)(unsafe.Pointer(uintptr(ptr) + offset + 1))
			b = uint32(b0) | uint32(b1)<<8
		case 3:
			b1 := *(*uint8)(unsafe.Pointer(uintptr(ptr) + offset + 1))
			b2 := *(*uint8)(unsafe.Pointer(uintptr(ptr) + offset + 2))
			b = uint32(b0) | uint32(b1)<<8 | uint32(b2)<<16
		case 4:
			b1 := *(*uint8)(unsafe.Pointer(uintptr(ptr) + offset + 1))
			b2 := *(*uint8)(unsafe.Pointer(uintptr(ptr) + offset + 2))
			b3 := *(*uint8)(unsafe.Pointer(uintptr(ptr) + offset + 3))
			b = uint32(b0) | uint32(b1)<<8 | uint32(b2)<<16 | uint32(b3)<<24
		}
	}
	{
		offset := uintptr(1 + lena + lenb)
		b0 := *(*uint8)(unsafe.Pointer(uintptr(ptr) + offset))
		switch lenc {
		case 1:
			c = uint32(b0)
		case 2:
			b1 := *(*uint8)(unsafe.Pointer(uintptr(ptr) + offset + 1))
			c = uint32(b0) | uint32(b1)<<8
		case 3:
			b1 := *(*uint8)(unsafe.Pointer(uintptr(ptr) + offset + 1))
			b2 := *(*uint8)(unsafe.Pointer(uintptr(ptr) + offset + 2))
			c = uint32(b0) | uint32(b1)<<8 | uint32(b2)<<16
		case 4:
			b1 := *(*uint8)(unsafe.Pointer(uintptr(ptr) + offset + 1))
			b2 := *(*uint8)(unsafe.Pointer(uintptr(ptr) + offset + 2))
			b3 := *(*uint8)(unsafe.Pointer(uintptr(ptr) + offset + 3))
			c = uint32(b0) | uint32(b1)<<8 | uint32(b2)<<16 | uint32(b3)<<24
		}
	}
	return a, b, c, unsafe.Pointer(uintptr(ptr) + uintptr(1+lena+lenb+lenc))
}

func readThreeGroupVarint2(ptr unsafe.Pointer) (a, b, c uint32, p unsafe.Pointer) {
	var lens [3]uint8
	ctrl := *(*uint8)(ptr)
	lens[0] = (ctrl & 0x03) + 1
	lens[1] = ((ctrl >> 2) & 0x03) + 1
	lens[2] = ((ctrl >> 4) & 0x03) + 1
	var offsets [3]uintptr
	offsets[0] = uintptr(1)
	offsets[1] = uintptr(1 + lens[0])
	offsets[2] = uintptr(1 + lens[0] + lens[1])
	var vals [3]uint32
	for i := 0; i < 3; i++ {
		offset := offsets[i]
		b0 := *(*uint8)(unsafe.Pointer(uintptr(ptr) + offset))
		switch lens[i] {
		case 1:
			vals[i] = uint32(b0)
		case 2:
			b1 := *(*uint8)(unsafe.Pointer(uintptr(ptr) + offset + 1))
			vals[i] = uint32(b0) | uint32(b1)<<8
		case 3:
			b1 := *(*uint8)(unsafe.Pointer(uintptr(ptr) + offset + 1))
			b2 := *(*uint8)(unsafe.Pointer(uintptr(ptr) + offset + 2))
			vals[i] = uint32(b0) | uint32(b1)<<8 | uint32(b2)<<16
		case 4:
			b1 := *(*uint8)(unsafe.Pointer(uintptr(ptr) + offset + 1))
			b2 := *(*uint8)(unsafe.Pointer(uintptr(ptr) + offset + 2))
			b3 := *(*uint8)(unsafe.Pointer(uintptr(ptr) + offset + 3))
			vals[i] = uint32(b0) | uint32(b1)<<8 | uint32(b2)<<16 | uint32(b3)<<24
		}
	}

	return vals[0], vals[1], vals[2], unsafe.Pointer(uintptr(ptr) + offsets[2] + uintptr(lens[2]))
}

func readThreeGroupVarint3(ptr unsafe.Pointer) (a, b, c uint32, p unsafe.Pointer) {
	var lena, lenb, lenc uint8
	ctrl := *(*uint8)(ptr)
	lena = (ctrl & 0x03) + 1
	lenb = ((ctrl >> 2) & 0x03) + 1
	lenc = ((ctrl >> 4) & 0x03) + 1
	offseta := uintptr(1)
	offsetb := uintptr(1 + lena)
	offsetc := uintptr(1 + lena + lenb)
	a0 := *(*uint8)(unsafe.Pointer(uintptr(ptr) + offseta))
	b0 := *(*uint8)(unsafe.Pointer(uintptr(ptr) + offsetb))
	c0 := *(*uint8)(unsafe.Pointer(uintptr(ptr) + offsetc))
	var a1, a2, a3, b1, b2, b3, c1, c2, c3 uint8
	if lena > 1 {
		a1 = *(*uint8)(unsafe.Pointer(uintptr(ptr) + offseta + 1))
	}
	if lenb > 1 {
		b1 = *(*uint8)(unsafe.Pointer(uintptr(ptr) + offsetb + 1))
	}
	if lenc > 1 {
		c1 = *(*uint8)(unsafe.Pointer(uintptr(ptr) + offsetc + 1))
	}
	if lena > 2 {
		a2 = *(*uint8)(unsafe.Pointer(uintptr(ptr) + offseta + 2))
	}
	if lenb > 2 {
		b2 = *(*uint8)(unsafe.Pointer(uintptr(ptr) + offsetb + 2))
	}
	if lenc > 2 {
		c2 = *(*uint8)(unsafe.Pointer(uintptr(ptr) + offsetc + 2))
	}
	if lena == 4 {
		a3 = *(*uint8)(unsafe.Pointer(uintptr(ptr) + offseta + 3))
	}
	if lenb == 4 {
		b3 = *(*uint8)(unsafe.Pointer(uintptr(ptr) + offsetb + 3))
	}
	if lenc == 4 {
		c3 = *(*uint8)(unsafe.Pointer(uintptr(ptr) + offsetc + 3))
	}
	a = uint32(a0) | uint32(a1)<<8 | uint32(a2)<<16 | uint32(a3)<<24
	b = uint32(b0) | uint32(b1)<<8 | uint32(b2)<<16 | uint32(b3)<<24
	c = uint32(c0) | uint32(c1)<<8 | uint32(c2)<<16 | uint32(c3)<<24
	return a, b, c, unsafe.Pointer(uintptr(ptr) + uintptr(1+lena+lenb+lenc))
}

func readThreeGroupVarint4(ptr unsafe.Pointer) (a, b, c uint32, p unsafe.Pointer) {
	var lena, lenb, lenc uint8
	ctrl := *(*uint8)(ptr)
	lena = (ctrl & 0x03) + 1
	lenb = ((ctrl >> 2) & 0x03) + 1
	lenc = ((ctrl >> 4) & 0x03) + 1
	offseta := uintptr(1)
	offsetb := uintptr(1 + lena)
	offsetc := uintptr(1 + lena + lenb)
	a0 := *(*uint8)(unsafe.Pointer(uintptr(ptr) + offseta))
	b0 := *(*uint8)(unsafe.Pointer(uintptr(ptr) + offsetb))
	c0 := *(*uint8)(unsafe.Pointer(uintptr(ptr) + offsetc))
	var a1, a2, a3, b1, b2, b3, c1, c2, c3 uint8
	if lena > 1 {
		a1 = *(*uint8)(unsafe.Pointer(uintptr(ptr) + offseta + 1))
		if lena > 2 {
			a2 = *(*uint8)(unsafe.Pointer(uintptr(ptr) + offseta + 2))
			if lena == 4 {
				a3 = *(*uint8)(unsafe.Pointer(uintptr(ptr) + offseta + 3))
			}
		}
	}
	if lenb > 1 {
		b1 = *(*uint8)(unsafe.Pointer(uintptr(ptr) + offsetb + 1))
		if lenb > 2 {
			b2 = *(*uint8)(unsafe.Pointer(uintptr(ptr) + offsetb + 2))
			if lenb == 4 {
				b3 = *(*uint8)(unsafe.Pointer(uintptr(ptr) + offsetb + 3))
			}
		}
	}
	if lenc > 1 {
		c1 = *(*uint8)(unsafe.Pointer(uintptr(ptr) + offsetc + 1))
		if lenc > 2 {
			c2 = *(*uint8)(unsafe.Pointer(uintptr(ptr) + offsetc + 2))
			if lenc == 4 {
				c3 = *(*uint8)(unsafe.Pointer(uintptr(ptr) + offsetc + 3))
			}
		}
	}
	a = uint32(a0) | uint32(a1)<<8 | uint32(a2)<<16 | uint32(a3)<<24
	b = uint32(b0) | uint32(b1)<<8 | uint32(b2)<<16 | uint32(b3)<<24
	c = uint32(c0) | uint32(c1)<<8 | uint32(c2)<<16 | uint32(c3)<<24
	return a, b, c, unsafe.Pointer(uintptr(ptr) + uintptr(1+lena+lenb+lenc))
}

/*
goos: darwin
goarch: arm64
pkg: github.com/cockroachdb/pebble/sstable
BenchmarkVarintGroupEncode-10     	180893488	         6.519 ns/op
BenchmarkVarintEncode-10          	182714532	         6.524 ns/op
BenchmarkVarintGroupDecode1-10    	185554659	         6.465 ns/op
BenchmarkVarintGroupDecode2-10    	124914559	         9.584 ns/op
BenchmarkVarintGroupDecode3-10    	179450142	         6.682 ns/op
BenchmarkVarintGroupDecode4-10    	204201415	         5.854 ns/op
BenchmarkVarintGroupDecode5-10    	222037824	         5.403 ns/op
BenchmarkVarintDecode-10          	227156979	         5.274 ns/op

goos: linux
goarch: amd64
pkg: github.com/cockroachdb/pebble/sstable
cpu: Intel(R) Xeon(R) CPU @ 2.30GHz
BenchmarkVarintGroupEncode-24     	100000000	        11.62 ns/op
BenchmarkVarintEncode-24          	100000000	        11.94 ns/op
BenchmarkVarintGroupDecode1-24    	100000000	        11.62 ns/op
BenchmarkVarintGroupDecode2-24    	74613084	        15.47 ns/op
BenchmarkVarintGroupDecode3-24    	73081914	        15.92 ns/op
BenchmarkVarintGroupDecode4-24    	80123619	        14.56 ns/op
BenchmarkVarintGroupDecode5-24    	149651772	         7.979 ns/op
BenchmarkVarintDecode-24          	127804464	         9.415 ns/op
*/
func readThreeGroupVarint5(ptr unsafe.Pointer) (a, b, c uint32, p unsafe.Pointer) {
	ctrl := *(*uint8)(ptr)
	ctrlState := ctrlToState[ctrl]
	// Doing unaligned access to uint32 works ok. But we could also be pointing to parts that are
	// not allocated and masking it. Not sure why that is not causing a failure.
	// This can be fixed since we have at least 4 allocated bytes in ptr (1 ctrl byte plus 1 byte
	// each for a, b, c), so we store an offset from which the mask should be applied (already have
	// the offset), and then how much to right shift.
	// For example for 0x00, 0x01, 0x01, 0x01, i.e., a,b,c = 1, we would have
	// offsets[0],[1],[2]=0, masks[0]=0x00ff00, masks[1]=0x0000ff00,
	// masks[2]=0x000000ff, and rightShifts[0]=8, rightShifts[1]=16,
	// rightShifts[2]=24.
	a = *(*uint32)(unsafe.Pointer(uintptr(ptr) + uintptr(ctrlState.offsets[0]))) & ctrlState.masks[0]
	b = *(*uint32)(unsafe.Pointer(uintptr(ptr) + uintptr(ctrlState.offsets[1]))) & ctrlState.masks[1]
	c = *(*uint32)(unsafe.Pointer(uintptr(ptr) + uintptr(ctrlState.offsets[2]))) & ctrlState.masks[2]
	return a, b, c, unsafe.Pointer(uintptr(ptr) + uintptr(ctrlState.offsets[3]))
}

// Varint is using little endian order.
func writeThreeVarints(a, b, c uint32, buf []byte) int {
	n := 0
	{
		x := a
		for x >= 0x80 {
			buf[n] = byte(x) | 0x80
			x >>= 7
			n++
		}
		buf[n] = byte(x)
		n++
	}
	{
		x := b
		for x >= 0x80 {
			buf[n] = byte(x) | 0x80
			x >>= 7
			n++
		}
		buf[n] = byte(x)
		n++
	}
	{
		x := c
		for x >= 0x80 {
			buf[n] = byte(x) | 0x80
			x >>= 7
			n++
		}
		buf[n] = byte(x)
		n++
	}
	return n
}

func readThreeVarints(ptr unsafe.Pointer) (uint32, uint32, uint32, unsafe.Pointer) {
	var shared uint32
	if a := *((*uint8)(ptr)); a < 128 {
		shared = uint32(a)
		ptr = unsafe.Pointer(uintptr(ptr) + 1)
	} else if a, b := a&0x7f, *((*uint8)(unsafe.Pointer(uintptr(ptr) + 1))); b < 128 {
		shared = uint32(b)<<7 | uint32(a)
		ptr = unsafe.Pointer(uintptr(ptr) + 2)
	} else if b, c := b&0x7f, *((*uint8)(unsafe.Pointer(uintptr(ptr) + 2))); c < 128 {
		shared = uint32(c)<<14 | uint32(b)<<7 | uint32(a)
		ptr = unsafe.Pointer(uintptr(ptr) + 3)
	} else if c, d := c&0x7f, *((*uint8)(unsafe.Pointer(uintptr(ptr) + 3))); d < 128 {
		shared = uint32(d)<<21 | uint32(c)<<14 | uint32(b)<<7 | uint32(a)
		ptr = unsafe.Pointer(uintptr(ptr) + 4)
	} else {
		d, e := d&0x7f, *((*uint8)(unsafe.Pointer(uintptr(ptr) + 4)))
		shared = uint32(e)<<28 | uint32(d)<<21 | uint32(c)<<14 | uint32(b)<<7 | uint32(a)
		ptr = unsafe.Pointer(uintptr(ptr) + 5)
	}

	var unshared uint32
	if a := *((*uint8)(ptr)); a < 128 {
		unshared = uint32(a)
		ptr = unsafe.Pointer(uintptr(ptr) + 1)
	} else if a, b := a&0x7f, *((*uint8)(unsafe.Pointer(uintptr(ptr) + 1))); b < 128 {
		unshared = uint32(b)<<7 | uint32(a)
		ptr = unsafe.Pointer(uintptr(ptr) + 2)
	} else if b, c := b&0x7f, *((*uint8)(unsafe.Pointer(uintptr(ptr) + 2))); c < 128 {
		unshared = uint32(c)<<14 | uint32(b)<<7 | uint32(a)
		ptr = unsafe.Pointer(uintptr(ptr) + 3)
	} else if c, d := c&0x7f, *((*uint8)(unsafe.Pointer(uintptr(ptr) + 3))); d < 128 {
		unshared = uint32(d)<<21 | uint32(c)<<14 | uint32(b)<<7 | uint32(a)
		ptr = unsafe.Pointer(uintptr(ptr) + 4)
	} else {
		d, e := d&0x7f, *((*uint8)(unsafe.Pointer(uintptr(ptr) + 4)))
		unshared = uint32(e)<<28 | uint32(d)<<21 | uint32(c)<<14 | uint32(b)<<7 | uint32(a)
		ptr = unsafe.Pointer(uintptr(ptr) + 5)
	}

	var value uint32
	if a := *((*uint8)(ptr)); a < 128 {
		value = uint32(a)
		ptr = unsafe.Pointer(uintptr(ptr) + 1)
	} else if a, b := a&0x7f, *((*uint8)(unsafe.Pointer(uintptr(ptr) + 1))); b < 128 {
		value = uint32(b)<<7 | uint32(a)
		ptr = unsafe.Pointer(uintptr(ptr) + 2)
	} else if b, c := b&0x7f, *((*uint8)(unsafe.Pointer(uintptr(ptr) + 2))); c < 128 {
		value = uint32(c)<<14 | uint32(b)<<7 | uint32(a)
		ptr = unsafe.Pointer(uintptr(ptr) + 3)
	} else if c, d := c&0x7f, *((*uint8)(unsafe.Pointer(uintptr(ptr) + 3))); d < 128 {
		value = uint32(d)<<21 | uint32(c)<<14 | uint32(b)<<7 | uint32(a)
		ptr = unsafe.Pointer(uintptr(ptr) + 4)
	} else {
		d, e := d&0x7f, *((*uint8)(unsafe.Pointer(uintptr(ptr) + 4)))
		value = uint32(e)<<28 | uint32(d)<<21 | uint32(c)<<14 | uint32(b)<<7 | uint32(a)
		ptr = unsafe.Pointer(uintptr(ptr) + 5)
	}
	return shared, unshared, value, ptr
}
