package gencommon

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestIsFilesystemPath(t *testing.T) {
	tests := []struct {
		input    string
		expected bool
	}{
		{".", true},
		{"./internal/model", true},
		{"../sibling", true},
		{"/home/user/project", true},
		{"github.com/user/repo/pkg", false},
		{"fmt", false},
		{"mypackage", false},
		{"golang.org/x/tools", false},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := isFilesystemPath(tt.input)
			if got != tt.expected {
				t.Errorf("isFilesystemPath(%q) = %v, want %v", tt.input, got, tt.expected)
			}
		})
	}
}

// TestLoadPackages_ImportPath tests that loadPackages can load a package via its
// Go import path (not just a filesystem directory).
func TestLoadPackages_ImportPath(t *testing.T) {
	tmpDir := t.TempDir()

	// Create a module with two sub-packages.
	if err := os.WriteFile(filepath.Join(tmpDir, "go.mod"), []byte("module example.com/testmod\n\ngo 1.25.0\n"), 0644); err != nil {
		t.Fatalf("write go.mod: %v", err)
	}

	// Sub-package "model"
	modelDir := filepath.Join(tmpDir, "model")
	if err := os.MkdirAll(modelDir, 0755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(modelDir, "model.go"), []byte(`package model

type User struct {
	ID   int32
	Name string
}
`), 0644); err != nil {
		t.Fatalf("write model.go: %v", err)
	}

	// Run loadPackages from within the temp module directory so that
	// packages.Load can resolve the import path.
	origDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	if err := os.Chdir(tmpDir); err != nil {
		t.Fatalf("chdir: %v", err)
	}
	defer os.Chdir(origDir)

	pkgs, err := loadPackages([]string{"example.com/testmod/model"})
	if err != nil {
		t.Fatalf("loadPackages() failed: %v", err)
	}

	if len(pkgs) != 1 {
		t.Fatalf("expected 1 package, got %d", len(pkgs))
	}
	if pkgs[0].Name != "model" {
		t.Errorf("expected package name 'model', got %q", pkgs[0].Name)
	}
	if pkgs[0].PkgPath != "example.com/testmod/model" {
		t.Errorf("expected PkgPath 'example.com/testmod/model', got %q", pkgs[0].PkgPath)
	}
}

// TestLoadPackages_MixedInputs tests that loadPackages handles a mix of
// filesystem paths and import paths in a single invocation.
func TestLoadPackages_MixedInputs(t *testing.T) {
	tmpDir := t.TempDir()

	// Module root
	if err := os.WriteFile(filepath.Join(tmpDir, "go.mod"), []byte("module example.com/mixedmod\n\ngo 1.25.0\n"), 0644); err != nil {
		t.Fatalf("write go.mod: %v", err)
	}

	// Sub-package "alpha" — will be loaded via filesystem path
	alphaDir := filepath.Join(tmpDir, "alpha")
	if err := os.MkdirAll(alphaDir, 0755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(alphaDir, "alpha.go"), []byte(`package alpha

type A struct {
	X int32
}
`), 0644); err != nil {
		t.Fatalf("write: %v", err)
	}

	// Sub-package "beta" — will be loaded via import path
	betaDir := filepath.Join(tmpDir, "beta")
	if err := os.MkdirAll(betaDir, 0755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(betaDir, "beta.go"), []byte(`package beta

type B struct {
	Y string
}
`), 0644); err != nil {
		t.Fatalf("write: %v", err)
	}

	// Chdir so import paths resolve against this module.
	origDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	if err := os.Chdir(tmpDir); err != nil {
		t.Fatalf("chdir: %v", err)
	}
	defer os.Chdir(origDir)

	pkgs, err := loadPackages([]string{alphaDir, "example.com/mixedmod/beta"})
	if err != nil {
		t.Fatalf("loadPackages() failed: %v", err)
	}

	if len(pkgs) != 2 {
		t.Fatalf("expected 2 packages, got %d", len(pkgs))
	}

	names := map[string]bool{}
	for _, p := range pkgs {
		names[p.Name] = true
	}
	if !names["alpha"] {
		t.Errorf("expected 'alpha' package in results")
	}
	if !names["beta"] {
		t.Errorf("expected 'beta' package in results")
	}
}

// TestLoadPackages_ImportPathNotInGoMod tests that an unresolvable import path
// produces an error with actionable go-get guidance.
func TestLoadPackages_ImportPathNotInGoMod(t *testing.T) {
	tmpDir := t.TempDir()

	// Create a minimal module so packages.Load has a context.
	if err := os.WriteFile(filepath.Join(tmpDir, "go.mod"), []byte("module example.com/emptymod\n\ngo 1.25.0\n"), 0644); err != nil {
		t.Fatalf("write go.mod: %v", err)
	}
	if err := os.WriteFile(filepath.Join(tmpDir, "main.go"), []byte("package main\n"), 0644); err != nil {
		t.Fatalf("write: %v", err)
	}

	origDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	if err := os.Chdir(tmpDir); err != nil {
		t.Fatalf("chdir: %v", err)
	}
	defer os.Chdir(origDir)

	_, err = loadPackages([]string{"github.com/nonexistent/pkg123456789"})
	if err == nil {
		t.Fatal("expected error for nonexistent import path, got nil")
	}

	errMsg := err.Error()
	if !strings.Contains(errMsg, "go get") {
		t.Errorf("expected error to contain 'go get' guidance, got: %s", errMsg)
	}
	if strings.Contains(errMsg, "failed to load package directory") {
		t.Errorf("expected import-path error, not filesystem-path error, got: %s", errMsg)
	}
}
