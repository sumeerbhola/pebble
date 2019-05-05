// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package metamorphic

import (
	"bytes"
	"fmt"
)

// TODO(peter):
// - record of each op executed and what it produced (error, results, etc)
// - event listener that records events in relation to the current op being performed
type history struct {
	buf   bytes.Buffer
	count int
}

func (h *history) recordf(format string, args ...interface{}) {
	h.count++
	fmt.Fprintf(&h.buf, format, args...)
}

func (h *history) String() string {
	return h.buf.String()
}
