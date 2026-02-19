package common

import (
	"io"
	"os"
)

// CopyFile copies a file from src to dst.
// It preserves file permissions and flushes writes to stable storage.
func CopyFile(src, dst string) error {
	// 1. Open the source file for reading
	sourceFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer sourceFile.Close()

	// 2. Create the destination file
	// os.Create truncates the file if it exists, or creates it with mode 0666 (before umask)
	destFile, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer destFile.Close()

	// 3. Perform the actual copy using the kernel's sendfile (if supported)
	// io.Copy automatically uses optimizations like sendfile on Linux
	if _, err := io.Copy(destFile, sourceFile); err != nil {
		return err
	}

	// 4. Flush writes to stable storage (Critical for data integrity)
	if err := destFile.Sync(); err != nil {
		return err
	}

	// 5. Optional: Copy file permissions (chmod)
	si, err := sourceFile.Stat()
	if err == nil {
		err = os.Chmod(dst, si.Mode())
	}

	return err
}
