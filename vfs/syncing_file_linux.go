//go:build linux

package vfs

// isDirectIOFile checks if the given File is a DirectIOFile
func isDirectIOFile(f File) bool {
	_, ok := f.(*DirectIOFile)
	return ok
}