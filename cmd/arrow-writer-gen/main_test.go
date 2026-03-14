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

func TestCLI_ImportPathNotInGoMod(t *testing.T) {
	tmpDir := t.TempDir()

	// Create a minimal module so the CLI has a module context.
	if err := os.WriteFile(filepath.Join(tmpDir, "go.mod"), []byte("module dummy\n\ngo 1.25.0\n"), 0644); err != nil {
		t.Fatalf("write go.mod: %v", err)
	}
	if err := os.WriteFile(filepath.Join(tmpDir, "main.go"), []byte("package dummy\n"), 0644); err != nil {
		t.Fatalf("write: %v", err)
	}

	// Chdir so the import path is resolved against this module.
	origDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	if err := os.Chdir(tmpDir); err != nil {
		t.Fatalf("chdir: %v", err)
	}
	defer os.Chdir(origDir)

	cmd := newRootCmd()
	cmd.SetArgs([]string{
		"--pkg", "github.com/nonexistent/pkg123456789",
		"--structs", "Person",
		"--out", filepath.Join(tmpDir, "dummy.go"),
	})

	err = cmd.Execute()
	if err == nil {
		t.Fatal("Expected error for nonexistent import path, got nil")
	}

	errMsg := err.Error()
	if !strings.Contains(errMsg, "go get") {
		t.Errorf("Expected error to contain 'go get' remediation guidance, got: %s", errMsg)
	}
	if strings.Contains(errMsg, "failed to load package directory") {
		t.Errorf("Expected import-path error, not filesystem-path error, got: %s", errMsg)
	}
}

func TestCLI_PkgAlias(t *testing.T) {
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
		"--pkg-name", "dummy",
		"--pkg-alias", "dummy=mydummy",
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

	if !strings.Contains(got, `mydummy "dummy"`) {
		t.Errorf("Expected aliased import mydummy \"dummy\", but got:\n%s", got)
	}

	if !strings.Contains(got, "func (w *PersonArrowWriter) Append(row *mydummy.Person)") {
		t.Errorf("Expected func (w *PersonArrowWriter) Append(row *mydummy.Person), but got:\n%s", got)
	}
}
