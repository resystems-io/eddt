package deltagen

// parse_lookup.go resolves the eddt runtime package from the loaded package set
// and locates named struct types in top-level package scopes. These helpers are
// called by the parseSnapshot orchestration in parse.go.

import (
	"fmt"
	"go/types"

	"golang.org/x/tools/go/packages"
)

// runtimePkgImportPath is the canonical import path for the eddt runtime
// package. It is used to locate runtime.Header for type-identity comparison.
const runtimePkgImportPath = "go.resystems.io/eddt/runtime"

// headerTypeFor returns the types.Type for runtime.Header by resolving the
// eddt runtime package from the transitive package closure.
//
// The runtime package is a dependency of any conforming Snapshot package and
// is therefore reachable via FindPkgByPath when NeedDeps was set during load.
// If it is not found (because the source package does not import it), the
// returned error guides the user to add the dependency.
func headerTypeFor(pkgs []*packages.Package) (types.Type, error) {
	rp := FindPkgByPath(pkgs, runtimePkgImportPath)
	if rp == nil {
		return nil, fmt.Errorf(
			"could not find %s in loaded packages; "+
				"ensure the source package imports go.resystems.io/eddt/runtime",
			runtimePkgImportPath)
	}

	obj := rp.Types.Scope().Lookup("Header")
	if obj == nil {
		return nil, fmt.Errorf("runtime.Header not found in package scope of %s", runtimePkgImportPath)
	}

	return obj.Type(), nil
}

// findNamedStruct searches the top-level packages for a type named name that
// is a struct. It returns the *types.Named, the containing package, and an
// error if the name is absent or does not refer to a struct type.
//
// Only top-level packages are searched; dependency packages loaded for
// type-identity resolution are excluded.
func findNamedStruct(pkgs []*packages.Package, name string) (*types.Named, *packages.Package, error) {
	for _, pkg := range pkgs {
		obj := pkg.Types.Scope().Lookup(name)
		if obj == nil {
			continue
		}

		typeName, ok := obj.(*types.TypeName)
		if !ok {
			return nil, nil, fmt.Errorf("%q in package %q is not a type", name, pkg.PkgPath)
		}

		named, ok := typeName.Type().(*types.Named)
		if !ok {
			return nil, nil, fmt.Errorf("%q in package %q is not a named type", name, pkg.PkgPath)
		}

		if _, ok := named.Underlying().(*types.Struct); !ok {
			return nil, nil, fmt.Errorf("%q in package %q is not a struct type", name, pkg.PkgPath)
		}

		return named, pkg, nil
	}

	return nil, nil, fmt.Errorf("struct %q not found in any loaded package", name)
}
