//go:build linux

package vfs

import (
	"os"
	"syscall"
	"unsafe"

	"github.com/cockroachdb/errors"
)

// DirectIOFile wraps an os.File to handle O_DIRECT writes with proper alignment.
type DirectIOFile struct {
	*os.File
	alignment int
}

// OpenDirectIO opens a file with O_DIRECT flag for direct I/O operations.
// This bypasses the OS page cache and requires aligned memory operations.
func (fs defaultFS) OpenDirectIO(name string, flag int, perm os.FileMode) (File, error) {
	f, err := os.OpenFile(name, flag|syscall.O_DIRECT, perm)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	
	// Wrap with DirectIOFile to handle alignment requirements
	return &DirectIOFile{
		File:      f,
		alignment: 4096, // Most common alignment for direct I/O
	}, nil
}

// Write ensures the data is properly aligned for O_DIRECT writes.
func (f *DirectIOFile) Write(p []byte) (n int, err error) {
	// For O_DIRECT, we need to ensure proper alignment
	if len(p) == 0 {
		return 0, nil
	}
	
	// Check if the buffer is already aligned
	if uintptr(unsafe.Pointer(&p[0]))%uintptr(f.alignment) == 0 && len(p)%f.alignment == 0 {
		// Already aligned, write directly
		return f.File.Write(p)
	}
	
	// Need to create an aligned buffer
	alignedSize := ((len(p) + f.alignment - 1) / f.alignment) * f.alignment
	alignedBuf := make([]byte, alignedSize+f.alignment)
	
	// Align the buffer pointer
	offset := uintptr(unsafe.Pointer(&alignedBuf[0])) % uintptr(f.alignment)
	if offset != 0 {
		alignedBuf = alignedBuf[f.alignment-offset:]
	}
	alignedBuf = alignedBuf[:alignedSize]
	
	// Copy data to aligned buffer
	copy(alignedBuf, p)
	
	n, err = f.File.Write(alignedBuf)
	if n > len(p) {
		n = len(p)
	}
	return n, err
}

// Stat implements the File interface.
func (f *DirectIOFile) Stat() (FileInfo, error) {
	fi, err := f.File.Stat()
	if err != nil {
		return nil, err
	}
	return defaultFileInfo{fi}, nil
}

// SyncData implements the File interface.
func (f *DirectIOFile) SyncData() error {
	return f.File.Sync()
}

// SyncTo implements the File interface.
func (f *DirectIOFile) SyncTo(offset int64) (fullSync bool, err error) {
	// For O_DIRECT files, sync the entire file
	return true, f.File.Sync()
}

// Preallocate implements the File interface.
func (f *DirectIOFile) Preallocate(offset, length int64) error {
	// Use fallocate if available
	fd := f.File.Fd()
	if fd == InvalidFd {
		return nil
	}
	_, _, errno := syscall.Syscall6(syscall.SYS_FALLOCATE, fd, uintptr(0), uintptr(offset), uintptr(length), 0, 0)
	if errno != 0 {
		return errno
	}
	return nil
}

// Prefetch implements the File interface.
func (f *DirectIOFile) Prefetch(offset int64, length int64) error {
	// O_DIRECT bypasses page cache, so prefetch is not applicable
	return nil
}