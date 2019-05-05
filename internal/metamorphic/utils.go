// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package metamorphic

import "golang.org/x/exp/rand"

// intSlice is an unordered set of integers used when random selection of an
// element is required.
type intSlice []int

// Remove removes the specified integer from the set.
//
// TODO(peter): If this proves slow, we can replace this implementation with a
// map and a slice. The slice would provide for random selection of an element,
// while the map would provide quick location of an element to remove.
func (s *intSlice) remove(i int) {
	n := len(*s)
	for j := 0; j < n; j++ {
		if (*s)[j] == i {
			(*s)[j], (*s)[n-1] = (*s)[n-1], (*s)[j]
			(*s) = (*s)[:n-1]
			break
		}
	}
}

func (s *intSlice) rand(rng *rand.Rand) int {
	return (*s)[rng.Intn(len(*s))]
}

// intSet is an unordered set of integers.
type intSet map[int]struct{}
