//go:build !linux

package vfs

// isDirectIOFile always returns false on non-Linux platforms
func isDirectIOFile(f File) bool {
	return false
}