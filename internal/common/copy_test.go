package common

import (
	"os"
	"path/filepath"
	"testing"
)

func TestCopyFile(t *testing.T) {
	// Create a temporary directory for the test
	tmpDir := t.TempDir()

	// define source and destination paths
	srcPath := filepath.Join(tmpDir, "source.txt")
	dstPath := filepath.Join(tmpDir, "dest.txt")

	// Create a source file with some content
	content := []byte("Hello, World!")
	if err := os.WriteFile(srcPath, content, 0644); err != nil {
		t.Fatalf("Failed to create source file: %v", err)
	}

	// Set specific permissions to test permission copying
	// Note: on Windows, permissions simpler, but strictly on Linux/Unix 0600 should work
	if err := os.Chmod(srcPath, 0600); err != nil {
		t.Fatalf("Failed to chmod source file: %v", err)
	}

	// Perform the copy
	if err := CopyFile(srcPath, dstPath); err != nil {
		t.Fatalf("CopyFile failed: %v", err)
	}

	// Verify the destination file exists
	if _, err := os.Stat(dstPath); os.IsNotExist(err) {
		t.Fatalf("Destination file does not exist")
	}

	// Verify content
	readContent, err := os.ReadFile(dstPath)
	if err != nil {
		t.Fatalf("Failed to read destination file: %v", err)
	}
	if string(readContent) != string(content) {
		t.Errorf("Content mismatch: expected %q, got %q", content, readContent)
	}

	// Verify permissions
	// Note: file systems might not preserve exact bits (e.g. setuid), but 0600 should stay 0600 on standard FS
	filteredMode := func(m os.FileMode) os.FileMode {
		return m & 0777
	}
	srcStat, _ := os.Stat(srcPath)
	dstStat, err := os.Stat(dstPath)
	if err != nil {
		t.Fatalf("Failed to stat dest file: %v", err)
	}

	if filteredMode(srcStat.Mode()) != filteredMode(dstStat.Mode()) {
		t.Errorf("Permissions mismatch: expected %v, got %v", filteredMode(srcStat.Mode()), filteredMode(dstStat.Mode()))
	}
}
