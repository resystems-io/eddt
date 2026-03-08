package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCLI_GenerateWriters(t *testing.T) {
	tmpDir := t.TempDir()

	file1 := filepath.Join(tmpDir, "model.go")
	content1 := `package dummy

type Person struct {
	Name string
	Age  int32
}
`
	err := os.WriteFile(file1, []byte(content1), 0644)
	if err != nil {
		t.Fatalf("Failed to write to file1.go: %v", err)
	}

	modContent := "module dummy\n\ngo 1.25.0\n"
	err = os.WriteFile(filepath.Join(tmpDir, "go.mod"), []byte(modContent), 0644)
	if err != nil {
		t.Fatalf("Failed to write to go.mod: %v", err)
	}

	outFile := filepath.Join(tmpDir, "arrow-writer-gen.go")

	cmd := newRootCmd()
	cmd.SetArgs([]string{
		"--pkg", tmpDir,
		"--structs", "Person",
		"--out", outFile,
	})

	err = cmd.Execute()
	if err != nil {
		t.Fatalf("Cmd execute failed: %v", err)
	}

	out, err := os.ReadFile(outFile)
	if err != nil {
		t.Fatalf("Failed to read output file: %v", err)
	}

	got := string(out)
	if !strings.Contains(got, "type PersonArrowWriter struct") {
		t.Errorf("Expected PersonArrowWriter, but got:\n%s", got)
	}
}

func TestCLI_MissingStructs(t *testing.T) {
	cmd := newRootCmd()
	cmd.SetArgs([]string{
		"--pkg", ".",
		"--out", "dummy.go",
	})

	err := cmd.Execute()
	if err == nil {
		t.Fatalf("Expected err executing without --structs")
	}
	if !strings.Contains(err.Error(), "required flag(s) \"structs\" not set") {
		t.Errorf("Expected required flag error, got: %v", err)
	}
}

func TestCLI_InvalidPackage(t *testing.T) {
	cmd := newRootCmd()
	cmd.SetArgs([]string{
		"--pkg", "/does/not/exist/surely",
		"--structs", "Person",
		"--out", "dummy.go",
	})

	err := cmd.Execute()
	if err == nil {
		t.Fatalf("Expected err executing with invalid package")
	}
	if !strings.Contains(err.Error(), "failed to load package directory") {
		t.Errorf("Expected failed to load package error, got: %v", err)
	}
}
