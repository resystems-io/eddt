package gencommon

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
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

func TestParsePkgAliases(t *testing.T) {
	tests := []struct {
		name    string
		input   []string
		want    map[string]string
		wantErr string
	}{
		{"empty", nil, map[string]string{}, ""},
		{"single", []string{"example.com/pkg=mypkg"}, map[string]string{"example.com/pkg": "mypkg"}, ""},
		{"multiple", []string{"a=b", "c=d"}, map[string]string{"a": "b", "c": "d"}, ""},
		{"missing-equals", []string{"noequalssign"}, nil, "invalid --pkg-alias"},
		{"empty-original", []string{"=alias"}, nil, "invalid --pkg-alias"},
		{"empty-alias", []string{"path="}, nil, "invalid --pkg-alias"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParsePkgAliases(tt.input)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Errorf("ParsePkgAliases() error = %v, want containing %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParsePkgAliases() unexpected error: %v", err)
			}
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("ParsePkgAliases() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestResolveOutputContext(t *testing.T) {
	reserved := map[string]bool{"arrow": true, "array": true, "memory": true}

	t.Run("auto-detect-pkg-name", func(t *testing.T) {
		structs := []StructInfo{{Name: "Foo", PkgPath: "example.com/mypkg", PkgName: "mypkg"}}
		pkgName, _, err := ResolveOutputContext("mypkg", structs, "", nil, reserved)
		if err != nil {
			t.Fatal(err)
		}
		if pkgName != "mypkg" {
			t.Errorf("got pkgName %q, want %q", pkgName, "mypkg")
		}
	})

	t.Run("override-pkg-name", func(t *testing.T) {
		structs := []StructInfo{{Name: "Foo", PkgPath: "example.com/mypkg", PkgName: "mypkg"}}
		pkgName, imports, err := ResolveOutputContext("mypkg", structs, "outpkg", nil, reserved)
		if err != nil {
			t.Fatal(err)
		}
		if pkgName != "outpkg" {
			t.Errorf("got pkgName %q, want %q", pkgName, "outpkg")
		}
		// mypkg differs from outpkg, so it should be imported.
		if len(imports) != 1 || imports[0].Path != "example.com/mypkg" {
			t.Errorf("expected import for example.com/mypkg, got %v", imports)
		}
		// Qualifier should be set on the struct.
		if structs[0].Qualifier != "mypkg." {
			t.Errorf("Qualifier = %q, want %q", structs[0].Qualifier, "mypkg.")
		}
	})

	t.Run("alias-applied", func(t *testing.T) {
		structs := []StructInfo{{Name: "Foo", PkgPath: "example.com/mypkg", PkgName: "mypkg"}}
		aliases := map[string]string{"example.com/mypkg": "mp"}
		_, imports, err := ResolveOutputContext("mypkg", structs, "outpkg", aliases, reserved)
		if err != nil {
			t.Fatal(err)
		}
		if len(imports) != 1 || imports[0].Alias != "mp" {
			t.Errorf("expected aliased import, got %v", imports)
		}
		if structs[0].Qualifier != "mp." {
			t.Errorf("Qualifier = %q, want %q", structs[0].Qualifier, "mp.")
		}
	})

	t.Run("same-package-no-import", func(t *testing.T) {
		structs := []StructInfo{{Name: "Foo", PkgPath: "example.com/mypkg", PkgName: "mypkg"}}
		_, imports, err := ResolveOutputContext("mypkg", structs, "", nil, reserved)
		if err != nil {
			t.Fatal(err)
		}
		if len(imports) != 0 {
			t.Errorf("expected no imports for same-package, got %v", imports)
		}
		if structs[0].Qualifier != "" {
			t.Errorf("Qualifier should be empty for same-package, got %q", structs[0].Qualifier)
		}
	})

	t.Run("reserved-name-collision-output-pkg", func(t *testing.T) {
		structs := []StructInfo{{Name: "Foo", PkgPath: "example.com/arrow", PkgName: "arrow"}}
		_, _, err := ResolveOutputContext("arrow", structs, "", nil, reserved)
		if err == nil || !strings.Contains(err.Error(), "collides") {
			t.Errorf("expected collision error, got %v", err)
		}
	})

	t.Run("reserved-name-collision-import", func(t *testing.T) {
		structs := []StructInfo{{Name: "Foo", PkgPath: "example.com/memory", PkgName: "memory"}}
		_, _, err := ResolveOutputContext("auto", structs, "outpkg", nil, reserved)
		if err == nil || !strings.Contains(err.Error(), "collides") {
			t.Errorf("expected collision error, got %v", err)
		}
	})

	t.Run("struct-name-collision", func(t *testing.T) {
		structs := []StructInfo{
			{Name: "Foo", PkgPath: "example.com/a", PkgName: "a"},
			{Name: "Foo", PkgPath: "example.com/b", PkgName: "b"},
		}
		_, _, err := ResolveOutputContext("a", structs, "", nil, reserved)
		if err == nil || !strings.Contains(err.Error(), "multiple packages") {
			t.Errorf("expected collision error, got %v", err)
		}
	})

	t.Run("empty-structs", func(t *testing.T) {
		_, _, err := ResolveOutputContext("mypkg", nil, "", nil, reserved)
		if err == nil || !strings.Contains(err.Error(), "no target structs") {
			t.Errorf("expected error for empty structs, got %v", err)
		}
	})

	t.Run("unexported-fields-filtered", func(t *testing.T) {
		structs := []StructInfo{{
			Name:    "Foo",
			PkgPath: "example.com/mypkg",
			PkgName: "mypkg",
			Fields: []FieldInfo{
				{Name: "Exported", GoType: "int32"},
				{Name: "unexported", GoType: "string"},
			},
		}}
		_, _, err := ResolveOutputContext("mypkg", structs, "outpkg", nil, reserved)
		if err != nil {
			t.Fatal(err)
		}
		if len(structs[0].Fields) != 1 || structs[0].Fields[0].Name != "Exported" {
			t.Errorf("expected only Exported field, got %v", structs[0].Fields)
		}
	})

	t.Run("struct-qualifier-propagated", func(t *testing.T) {
		innerFields := []FieldInfo{
			{Name: "X", GoType: "int32"},
		}
		structs := []StructInfo{
			{
				Name:    "Outer",
				PkgPath: "example.com/mypkg",
				PkgName: "mypkg",
				Fields: []FieldInfo{
					{Name: "Inner", IsStruct: true, StructName: "Inner"},
					{Name: "OptInner", IsStruct: true, IsPointer: true, StructName: "Inner"},
					{Name: "Items", IsList: true, EltInfo: &FieldInfo{IsStruct: true, StructName: "Inner"}},
					{Name: "Lookup", IsMap: true,
						KeyInfo: &FieldInfo{GoType: "string"},
						EltInfo: &FieldInfo{IsStruct: true, StructName: "Inner"},
					},
				},
			},
			{
				Name:    "Inner",
				PkgPath: "example.com/otherpkg",
				PkgName: "otherpkg",
				Fields:  innerFields,
			},
		}
		_, _, err := ResolveOutputContext("mypkg", structs, "outpkg", nil, reserved)
		if err != nil {
			t.Fatal(err)
		}

		// Inner struct should get qualifier since otherpkg != outpkg.
		if structs[1].Qualifier != "otherpkg." {
			t.Errorf("Inner StructInfo.Qualifier = %q, want %q", structs[1].Qualifier, "otherpkg.")
		}

		outer := structs[0]
		// Direct struct field.
		if outer.Fields[0].StructQualifier != "otherpkg." {
			t.Errorf("Inner field StructQualifier = %q, want %q", outer.Fields[0].StructQualifier, "otherpkg.")
		}
		// Pointer to struct.
		if outer.Fields[1].StructQualifier != "otherpkg." {
			t.Errorf("OptInner field StructQualifier = %q, want %q", outer.Fields[1].StructQualifier, "otherpkg.")
		}
		// List element.
		if outer.Fields[2].EltInfo.StructQualifier != "otherpkg." {
			t.Errorf("Items.EltInfo.StructQualifier = %q, want %q", outer.Fields[2].EltInfo.StructQualifier, "otherpkg.")
		}
		// Map value.
		if outer.Fields[3].EltInfo.StructQualifier != "otherpkg." {
			t.Errorf("Lookup.EltInfo.StructQualifier = %q, want %q", outer.Fields[3].EltInfo.StructQualifier, "otherpkg.")
		}
	})

	t.Run("sorted-imports", func(t *testing.T) {
		structs := []StructInfo{
			{Name: "B", PkgPath: "example.com/z", PkgName: "z"},
			{Name: "A", PkgPath: "example.com/a", PkgName: "a"},
		}
		_, imports, err := ResolveOutputContext("", structs, "outpkg", nil, reserved)
		if err != nil {
			t.Fatal(err)
		}
		if len(imports) != 2 {
			t.Fatalf("expected 2 imports, got %d", len(imports))
		}
		if imports[0].Path != "example.com/a" || imports[1].Path != "example.com/z" {
			t.Errorf("imports not sorted: %v", imports)
		}
	})
}
