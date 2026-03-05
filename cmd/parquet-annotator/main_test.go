package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCLI_AnnotateDir(t *testing.T) {
	// Create a temporary directory structure
	tmpDir := t.TempDir()

	file1 := filepath.Join(tmpDir, "file1.go")
	content1 := `package test

type Person struct {
	Name string
	Age  int
}
`
	err := os.WriteFile(file1, []byte(content1), 0644)
	if err != nil {
		t.Fatalf("Failed to write to file1.go: %v", err)
	}

	// Create root command and execute it against tmpDir
	cmd := newRootCmd()
	cmd.SetArgs([]string{"--dir", tmpDir})

	err = cmd.Execute()
	if err != nil {
		t.Fatalf("Cmd execute failed: %v", err)
	}

	// Verify output
	out, err := os.ReadFile(file1)
	if err != nil {
		t.Fatalf("Failed to read file1.go: %v", err)
	}

	got := string(out)
	if !strings.Contains(got, "`parquet:\"name=Name, type=BYTE_ARRAY, logicaltype=String\"`") {
		t.Errorf("Expected Parquet tag on Name field, but got:\n%s", got)
	}
	if !strings.Contains(got, "`parquet:\"name=Age, type=INT64\"`") {
		t.Errorf("Expected Parquet tag on Age field, but got:\n%s", got)
	}
}

func TestCLI_InvalidDir(t *testing.T) {
	cmd := newRootCmd()
	cmd.SetArgs([]string{"--dir", "/does/not/exist/surely"})

	err := cmd.Execute()
	if err == nil {
		t.Fatalf("Expected err executing with invalid directory")
	}
	if !strings.Contains(err.Error(), "does not exist") {
		t.Errorf("Expected error string 'does not exist', got: %v", err)
	}
}

func TestCLI_AnnotateFiles(t *testing.T) {
	tmpDir := t.TempDir()

	file1 := filepath.Join(tmpDir, "file1.go")
	content1 := `package test

type Person struct {
	Name string
}
`
	err := os.WriteFile(file1, []byte(content1), 0644)
	if err != nil {
		t.Fatalf("Failed to write file1: %v", err)
	}

	file2 := filepath.Join(tmpDir, "file2.go")
	content2 := `package test

type Animal struct {
	Species string
}
`
	err = os.WriteFile(file2, []byte(content2), 0644)
	if err != nil {
		t.Fatalf("Failed to write file2: %v", err)
	}

	cmd := newRootCmd()
	cmd.SetArgs([]string{"-f", file1, "--file", file2})

	err = cmd.Execute()
	if err != nil {
		t.Fatalf("Cmd execute failed: %v", err)
	}

	out1, err := os.ReadFile(file1)
	if err != nil {
		t.Fatalf("Failed to read file1: %v", err)
	}
	if !strings.Contains(string(out1), "`parquet:\"name=Name, type=BYTE_ARRAY, logicaltype=String\"`") {
		t.Errorf("Expected Parquet tag in file1, got:\n%s", string(out1))
	}

	out2, err := os.ReadFile(file2)
	if err != nil {
		t.Fatalf("Failed to read file2: %v", err)
	}
	if !strings.Contains(string(out2), "`parquet:\"name=Species, type=BYTE_ARRAY, logicaltype=String\"`") {
		t.Errorf("Expected Parquet tag in file2, got:\n%s", string(out2))
	}
}
