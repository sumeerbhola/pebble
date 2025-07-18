//go:build !linux

package vfs

import (
	"os"

	"github.com/cockroachdb/errors"
)

// OpenDirectIO is not supported on non-Linux platforms.
func (fs defaultFS) OpenDirectIO(name string, flag int, perm os.FileMode) (File, error) {
	return nil, errors.New("direct I/O not supported on this platform")
}